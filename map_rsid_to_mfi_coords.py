"""
This takes a list of rsIDs and converts them to coordinates as defined in the .mfi.txt files in the UKB RAP
"""

from collections import defaultdict
import json
import os

import pandas as pd

outfile_desc = 'snps_with_ldsc_scores'
snplist_path = '~/sibreg/sibreg_project/processed/snp_list_of_snps_with_ldscores.snplist'
coord_table_outfile = f'{outfile_desc}_snp_coord_table.tsv'

# Get SNP list to filter with
snplist = pd.read_csv(snplist_path, sep='\t', header=None)[0]
snplist = snplist.to_numpy()
print(f'Loaded snplist, {len(snplist):,} SNPs') 


# Load the rsID-to-coordinate mapping file (.mfi.txt for the imputed genotypes)
rsid_to_coord_master = defaultdict(set)
headers = ['ALT_ID', 'rsID', 'POS', 'A1', 'A2', 'MAF', 'Minor_Allele', 'INFO']
for chr_idx in range(1, 23):
    map_path = f'/mnt/project/Bulk/Imputation/UKB imputation from genotype/ukb22828_c{chr_idx}_b0_v3.mfi.txt'
    df = pd.read_csv(map_path, sep=r'\s+', header=None, names=headers, usecols=['rsID', 'POS'])
    
    # Filter the rsID-to-coordinate mapping for your SNPs
    df_sub = df[df['rsID'].isin(snplist)]
    for rsid, pos in zip(df_sub['rsID'], df_sub['POS']):
        rsid_to_coord_master[rsid].add(f"{chr_idx}:{pos}")

print(f'Found {len(rsid_to_coord_master):,} / {len(snplist):,} SNPs') 
rsid_to_coord_master = {k: list(v) for k, v in rsid_to_coord_master.items()}

# Save mapping to a file
with open(f"{len(rsid_to_coord_master)}_{outfile_desc}_rsid_to_coord.json", 'w') as fout:
    json.dump(rsid_to_coord_master, fout)
outfile = f"{len(rsid_to_coord_master)}_{outfile_desc}_rsid_to_coord.json"
os.system(f'dx upload {outfile}')

# Save coords to a file
all_coords = [coord for coords in rsid_to_coord_master.values() for coord in coords]
all_coords = list(set(all_coords))
with open(f"{len(rsid_to_coord_master)}_{outfile_desc}_coords.json", 'w') as fout:
    json.dump(all_coords, fout)
os.system(f'dx upload f"{len(rsid_to_coord_master)}_{outfile_desc}_coords.json"')


# Save coordinates to a table
with open(coord_table_outfile, 'w') as fout:
    for idx, coord in enumerate(all_coords):
        chr_num = coord.split(':')[0]
        pos = coord.split(':')[1]
        fout.write(f'{chr_num}\t{pos}\t{pos}\tpos_{idx}\n')
df = pd.read_csv(outfile, sep='\t', header=None)
df = df.sort_values(by=df.columns[0])
df = df.reset_index(drop=True)
df.to_csv(coord_table_outfile, sep='\t', index=False, header=False)
df = pd.read_csv(coord_table_outfile, sep='\t')

os.system(f'dx upload {coord_table_outfile}')
