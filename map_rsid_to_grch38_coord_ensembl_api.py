#!/usr/bin/env python3
"""
Minimal Ensembl REST rsID→GRCh38 mapper (for UKB DRAGEN WGS, PLINK2)

Input
-----
rsids.txt : one rsID per line ('rs123' or '123')

Outputs (to --outdir)
---------------------
rsid_to_coord.tsv   : rsID  CHR  POS  REF  ALT
ids_to_extract.txt  : CHR:POS:REF:ALT   (PLINK2 --extract)
rsid.range          : CHR  START  END  NAME  (PLINK2 --extract range)
missing_rsids.txt   : rsIDs with no GRCh38 mapping
ambiguous_rsids.tsv : rsIDs with >1 distinct GRCh38 mapping

Notes
-----
* Ensembl returns 1-based coords. Chrom names normalized: 1..22,X,Y,M.
* Multi-allelic rsIDs emit one row per REF→ALT pair.

Example Use
-----------
python ensembl_map_grch38.py --rsids '/sibreg_project/processed/snp_list_of_snps_with_ldscores.snplist' --outdir '../processed/rsid_to_grch38_mapping'
"""

import argparse
import os
import time
from typing import List, Dict, Tuple, Iterable
import requests

ENSEMBL_URL = (
    "https://rest.ensembl.org/variation/homo_sapiens"
    "?content-type=application/json")


def read_rsids(path: str) -> List[str]:
    """Read rsIDs; accept 'rs123' or '123'; de-dup preserving order."""
    seen = set()
    out = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if not s.startswith("rs"):
                s = "rs" + s
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    """Yield fixed-size chunks from seq."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def normalize_chr(chrom: str) -> str:
    """Normalize chromosome to PLINK/UKB: strip 'chr'; MT→M."""
    c = chrom
    if c.lower().startswith("chr"):
        c = c[3:]
    return "M" if c in ("M", "MT", "MtDNA", "mt", "MTDNA") else c


def backoff_sleep(attempt: int, retry_after: float = None) -> None:
    """Polite exponential backoff; honor Retry-After if present."""
    if retry_after is not None:
        delay = max(0.25, float(retry_after))
    else:
        delay = min(10.0, 0.25 * (2 ** attempt))
    time.sleep(delay)


def post_variation_batch(
    session: requests.Session, ids: List[str], max_retries: int = 5) -> Dict:
    """
    POST rsIDs to Ensembl; retry on 429/5xx.
    """
    payload = {"ids": ids}
    attempt = 0
    while True:
        try:
            resp = session.post(ENSEMBL_URL, json=payload, timeout=60)
        except requests.RequestException:
            if attempt >= max_retries:
                raise
            attempt += 1
            backoff_sleep(attempt)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt >= max_retries:
                resp.raise_for_status()
            ra = resp.headers.get("Retry-After")
            attempt += 1
            backoff_sleep(attempt, float(ra) if ra else None)
            continue

        resp.raise_for_status()


def parse_grch38_mappings(entry: dict) -> List[Tuple[str, int, str, str]]:
    """
    From one Ensembl 'variation' entry, return list of (CHR, POS, REF, ALT)
    for GRCh38 mappings. Uses allele_string (e.g., 'A/G' or 'A/G/T').
    """
    out: List[Tuple[str, int, str, str]] = []
    for m in entry.get("mappings", []):
        if m.get("assembly_name") != "GRCh38":
            continue
        chrom = m.get("seq_region_name")
        pos = m.get("start")
        allele_string = m.get("allele_string")
        if not chrom or not pos or not allele_string:
            continue
        chrom = normalize_chr(str(chrom))
        pos = int(pos)
        alleles = allele_string.split("/")
        ref = alleles[0]
        alts = alleles[1:] if len(alleles) > 1 else []
        if not alts:
            # Single allele_string: emit no-op (rare); keep ref→ref once
            out.append((chrom, pos, ref, ref))
        else:
            for alt in alts:
                out.append((chrom, pos, ref, alt))
    # De-duplicate identical tuples
    return sorted(set(out))


def main() -> None:
    
    # Argument parser to take in the rsIDs file, output directory, and API batch size
    ap = argparse.ArgumentParser()
    ap.add_argument("--rsids", required=True, help="Path to rsids.txt")
    ap.add_argument("--outdir", required=True, help="Output directory.")
    ap.add_argument("--batch", type=int, default=200, help="POST batch size.")
    args = ap.parse_args()

    # Read input rsIDs
    rsids = read_rsids(args.rsids)
    if not rsids:
        raise SystemExit("No rsIDs found in input.")

    # Prepare API session
    sess = requests.Session()
    sess.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "ukb-ensembl-mapper/1.2"
    })

    # Iterate through batches for the API calls
    found, missing, id_rows = set(), set(), set()
    coord_rows, range_rows, ambig_rows = set(), set(), set()
    for batch_rsids in chunks(rsids, args.batch):

        # POST API call
        data = post_variation_batch(sess, batch_rsids)
        
        # Check each rsID's mapped coordinates 
        for rs_id in batch_rsids:
            entry = data.get(rs_id)
            
            # If missing
            if not entry or ("error" in entry):
                missing.add(rs_id)
                continue
            
            # Get CHR, POS, REF, ALT mappings on GRCh38
            mappings = parse_grch38_mappings(entry)
            if not mappings:
                missing.add(rs_id)
                continue

            # Save CHR, POS, REF, ALT mappings on GRCh38
            for chrom, pos, ref, alt in mappings:
                coord_rows.add(f"{rs_id}\t{chrom}\t{pos}\t{ref}\t{alt}")
                id_rows.add(f"{chrom}:{pos}:{ref}:{alt}")
                range_rows.add(f"{chrom}\t{pos}\t{pos}\t{rs_id}")
                found.add(rs_id)

            # If multiple mappings, note ambiguity
            if len(mappings) > 1:
                joined = ";".join(f"{c}:{p}:{r}:{a}" 
                                  for (c, p, r, a) in mappings)
                ambig_rows.add(f"{rs_id}\t{len(mappings)}\t{joined}")

        time.sleep(0.1)  

    # Any requested rsID not mapped is missing
    missing.update(set(rsids) - found)

    ### Save Data ###
    # Prepare output paths
    os.makedirs(args.outdir, exist_ok=True)
    tsv_path = os.path.join(args.outdir, "rsid_to_coord.tsv")
    ids_path = os.path.join(args.outdir, "ids_to_extract.txt")
    rng_path = os.path.join(args.outdir, "rsid.range")
    miss_path = os.path.join(args.outdir, "missing_rsids.txt")
    ambg_path = os.path.join(args.outdir, "ambiguous_rsids.tsv")
    readme_path = os.path.join(args.outdir, "README.txt")

    # Save data
    readme_txt = (
        "Ensembl REST rsID→GRCh38 mapping (UKB DRAGEN WGS safe):\n"
        "- rsid_to_coord.tsv : rsID  CHR  POS  REF  ALT\n"
        "- ids_to_extract.txt: CHR:POS:REF:ALT (PLINK2 --extract)\n"
        "- rsid.range        : CHR  START  END  NAME (PLINK2 --extract range)\n"
        "- missing_rsids.txt : rsIDs with no GRCh38 mapping\n"
        "- ambiguous_rsids.tsv: rsIDs with multiple GRCh38 mappings\n"
        "Notes: chromosomes have no 'chr' prefix; MT→M to match PLINK2/UKB.\n"
    )
    with open(readme_path, "w") as f:
        f.write(readme_txt)

    with open(tsv_path, "w") as f:
        for row in sorted(coord_rows):
            f.write(row + "\n")

    with open(ids_path, "w") as f:
        for row in sorted(id_rows):
            f.write(row + "\n")

    with open(rng_path, "w") as f:
        for row in sorted(range_rows):
            f.write(row + "\n")

    with open(miss_path, "w") as f:
        for rs in sorted(missing):
            f.write(rs + "\n")

    with open(ambg_path, "w") as f:
        for row in sorted(ambig_rows):
            f.write(row + "\n")

    print("[done] rsIDs total: {} | mapped: {} | missing: {}".format(
        len(rsids), len(found), len(missing)))
    print("Outputs → {}".format(args.outdir))


if __name__ == "__main__":
    main()
