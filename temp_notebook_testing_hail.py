from functools import reduce
import time

import numpy as np
import pandas as pd
import hail as hl


def load_and_filter_bfiles_into_hail(
    bfile_prefixes: list[str], genome_build: str, snp_list: str | None, person_list: str | None
    ) -> hl.MatrixTable:
    """
    Load and combine bfiles into a Hail matrix table. Filter by SNPs and people.
    """
    mts = [
        hl.import_plink(
            bed=f'{bfile}.bed', 
            bim=f'{bfile}.bim', 
            fam=f'{bfile}.fam',
            reference_genome=genome_build) 
        for bfile in bfile_prefixes
    ]
    mt = reduce(lambda mt_one, mt_two: mt_one.union_rows(mt_two), mts)

    # Make the keys be FID and IID
    col_fields = set(mt.col)
    fid_field = 'fam_id' if 'fam_id' in col_fields else None
    iid_field = 's' if 's' in col_fields else next(iter(mt.col_key.keys()))
    mt = mt.key_cols_by(fid=mt[fid_field], iid=mt[iid_field])

    # Load SNP list of rsIDs as the 'rsid' column, and then filter by rsID
    if snp_list is not None:
        snps_ht = hl.import_table(snp_list, no_header=True).rename({'f0': 'rsid'})
        snps_ht = snps_ht.key_by('rsid')
        mt = mt.filter_rows(hl.is_defined(snps_ht[mt.rsid]))

    # Load IID person list and then filter by IIDs (Note do FID-IID later)
    if person_list is not None:
        fid_iid_ht = (
            hl.import_table(person_list, no_header=True)
              .rename({'f0': 'fid', 'f1': 'iid'})
              .key_by('fid', 'iid'))
        mt = mt.semi_join_cols(fid_iid_ht)
    
    # Return Hail MatrixTable of the bfiles
    return mt


def create_paired_hail_matrix(
    mt: hl.MatrixTable, person_list_one: str, person_list_two: str
    )-> hl.MatrixTable  :
    """
    """
    # Create a "genotype" field
    mt = mt.annotate_entries(genotype = mt.GT.n_alt_alleles())

    # Load the FID and IID for group 1
    samps_one_ht = hl.import_table(
        person_list_one, no_header=True, 
        types={'f0': hl.tstr, 'f1': hl.tstr})\
        .rename({'f0': 'fid1', 'f1': 'iid1'})

    # Load the FID and IID for group 2
    samps_two_ht = hl.import_table(
        person_list_two, no_header=True, 
        types={'f0': hl.tstr, 'f1': hl.tstr})\
        .rename({'f0': 'fid2', 'f1': 'iid2'})

    # Index each sibling pair
    samps_one_ht = samps_one_ht.add_index()
    samps_two_ht = samps_two_ht.add_index()

    # Join the tables on the sibling index
    pairs_ht = samps_one_ht.key_by('idx').join(samps_two_ht.key_by('idx'))

    ### Create lookups for (fid, iid) -> (pair_index, sibling one or two)
    ht_one_keyed = pairs_ht.key_by(ppl_key=hl.struct(fid=pairs_ht.fid1, iid=pairs_ht.iid1))
    lookup_table_one = ht_one_keyed.select(pair_idx=ht_one_keyed.idx, role="sibling_one")

    ht_two_keyed = pairs_ht.key_by(ppl_key=hl.struct(fid=pairs_ht.fid2, iid=pairs_ht.iid2))
    lookup_table_two = ht_two_keyed.select(pair_idx=ht_two_keyed.idx, role="sibling_two")

    lookup_ht = lookup_table_one.union(lookup_table_two)

    # Annotate columns with pair information
    mt = mt.annotate_cols(sample_key=hl.struct(fid=mt.fam_id, iid=mt.s))
    mt = mt.annotate_cols(pair_info=lookup_ht[mt.sample_key])
    mt = mt.annotate_cols(
        pair_idx_col = mt.pair_info.pair_idx,
        role_col = mt.pair_info.role)

    # Filter to sibling pairs
    paired_mt = mt.filter_cols(hl.is_defined(mt.pair_info))
    paired_mt = paired_mt.annotate_entries(role_entry=paired_mt.role_col)

    # Group columns by sibling index
    grouped_mt = paired_mt.group_cols_by(paired_mt.pair_idx_col)

    # Aggregate to collect both siblings' genotypes
    # Use the _parent attribute to access entry fields
    paired_mt = grouped_mt.aggregate(
        data=hl.agg.collect(
            hl.struct(
                role=grouped_mt._parent.role_entry,  # Access entry field from parent
                geno=grouped_mt._parent.genotype)))

    # Now paired_mt is a regular MatrixTable again - extract and compute differences
    paired_mt = paired_mt.annotate_entries(
        genotype_sib_one = paired_mt.data.find(lambda x: x.role == 'sibling_one').geno,
        genotype_sib_two = paired_mt.data.find(lambda x: x.role == 'sibling_two').geno)



geno_root = '/opt/notebooks/'
bfile_prefixes = [
    f'{geno_root}/eur_sibs_snps_with_ldscores_orig_imputed_chr21',
    f'{geno_root}/eur_sibs_snps_with_ldscores_orig_imputed_chr22'
]  
snp_list = '/home/dnanexus/sibreg/sibreg_project/processed/snp_list_of_snps_with_ldscores.snplist'
person_list = '/home/dnanexus/sibreg/sibreg_project/processed/two_eur_sibs_per_fam_sibs_except_five_bad_chr_person_list.txt'

hl.init(
    master='local[*]',  # Use all available cores
    spark_conf={
        'spark.driver.memory': '32g',    # Allocate 32GB to Spark driver
        'spark.executor.memory': '32g',  # Allocate 32GB to executors
        'spark.driver.maxResultSize': '32g',  # Allow large results
    },
    min_block_size=0)


# Load sib list
sib_list_one = 'sib_ones.tsv'
sib_list_two = 'sib_twos.tsv'
sib_fid_iid = pd.read_csv('/opt/notebooks/kin_sib_df.kin0', sep='\t')[['#FID1', 'IID1', 'FID2', 'IID2']]
sib_fid_iid[['#FID1', 'IID1']].to_csv(sib_list_one, sep='\t', index=False)
sib_fid_iid[['FID2', 'IID2']].to_csv(sib_list_two, sep='\t', index=False)


# Load bfiles into a HailMatrix
mt = load_and_filter_bfiles_into_hail(
    bfile_prefixes=bfile_prefixes,
    person_list=person_list,
    genome_build='GRCh37',
    snp_list=snp_list)

# Pair up siblings
create_paired_hail_matrix(
    person_list_one=person_list_one, 
    person_list_two=person_list_two,
    mt=mt, 
)


start_time = time.time()

# Average the squared differences across all variants and sibling pairs
mean_squared_diff = paired_mt.aggregate_entries(
    hl.agg.mean(
        (paired_mt.genotype_sib_one - paired_mt.genotype_sib_two)**2
    )
)

print(f"Mean squared genotype difference: {mean_squared_diff}")

print(time.time() - start_time)












import sys
sys.path.append('/home/dnanexus/sibreg/')

from utils import load_genotypes

genotypes_per_partitions = []

for i, bfile in enumerate(bfile_prefixes):

    # Load genotypes for WFSR or BFSR relatedness
    genotypes_per_partition = load_genotypes(
        person_list=person_list, 
        snp_lists=[snp_list],
        bfiles=[bfile])
    genotypes_per_partitions.append(genotypes_per_partition)    



start_time = time.time()

mid = 18315

for i, bfiles in enumerate(range(len(bfile_prefixes))):
    
    genotypes_per_partition = load_genotypes(
        person_list=person_list, 
        snp_lists=[snp_list],
        bfiles=[bfile])
    
    answer = np.nanmean(
        (genotypes_per_partition[0][:mid] - genotypes_per_partition[0][mid:-1])**2,
        axis=1)
    
print(f"Mean squared genotype difference: {mean_squared_diff}")

print(time.time() - start_time)




from utils import make_rel_lowest_memory

sib_one_geno_idxs=,
sib_two_geno_idxs=,
relatedness_path='temp_rel',
sib_id_df_path=,
person_list=person_list,
snp_lists=[snp_list],
estimator='wfsr',
bfiles=bfile_prefixes

make_rel_lowest_memory(
    sib_one_geno_idxs=sib_one_geno_idxs,
    sib_two_geno_idxs=sib_two_geno_idxs,
    relatedness_path=relatedness_path,
    sib_id_df_path=sib_id_df_path,
    person_list=person_list,
    snp_lists=snp_lists,
    estimator=estimator,
    bfiles=bfiles)
