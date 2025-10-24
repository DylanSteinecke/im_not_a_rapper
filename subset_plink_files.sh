#!/bin/bash
set +e +u +o pipefail

# RAP to extract siblings
UKB_BFILE_DIR="/mnt/project/Bulk/DRAGEN WGS/DRAGEN population level WGS variants, PLINK format [500k release]"
for CHR in {1..22}; do
       plink2 --pfile "${UKB_BFILE_DIR}/ukb24308_c${CHR}_b0_v1" \
              --make-bed \
              --extract range ~/sibreg/sibreg_project/processed/snps_with_ldsc_scores_snp_coord_table.tsv \
              --keep ~/sibreg/sibreg_project/processed/two_eur_sibs_per_fam_sibs_except_five_bad_chr_person_list.txt \
              --threads 15 \
              --no-pheno \
              --out ~/eur_sibs_snps_with_ldscores_chr${CHR}
done
