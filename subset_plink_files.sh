#!/bin/bash
set +e +u +o pipefail

# RAP to extract most EUR siblings from imputed, convert to bfiles
# "The alleles in the imputation are aligned with REF/ALT, first_allele is the ref allele on the fwd strand."
# https://biobank.ndph.ox.ac.uk/ukb/label.cgi?id=100319 
UKB_BGEN_DIR="/mnt/project/Bulk/Imputation/UKB imputation from genotype"
for CHR in {5..20}; do
       plink2 --bgen "${UKB_BGEN_DIR}/ukb22828_c${CHR}_b0_v3.bgen" ref-first \
              --sample "${UKB_BGEN_DIR}/ukb22828_c${CHR}_b0_v3.sample" \
              --make-bed \
              --extract ~/sibreg/sibreg_project/processed/snp_list_of_snps_with_ldscores.snplist \
              --keep ~/sibreg/sibreg_project/processed/two_eur_sibs_per_fam_sibs_except_five_bad_chr_person_list.txt \
              --threads 40 \
              --out ~/eur_sibs_snps_with_ldscores_orig_imputed_chr${CHR} \
	      --memory 140000

       dx  upload ~/eur_sibs_snps_with_ldscores_orig_imputed_chr${CHR}*
done
exit

# RAP to extract most EUR siblings from WGS, convert to bfiles
UKB_PFILE_DIR="/mnt/project/Bulk/DRAGEN WGS/DRAGEN population level WGS variants, PLINK format [500k release]"
for CHR in {1..22}; do
       plink2 --pfile "${UKB_PFILE_DIR}/ukb24308_c${CHR}_b0_v1" \
              --make-bed \
              --extract range ~/sibreg/sibreg_project/processed/snps_with_ldsc_scores_snp_coord_table.tsv \
              --keep ~/sibreg/sibreg_project/processed/two_eur_sibs_per_fam_sibs_except_five_bad_chr_person_list.txt \
              --threads 15 \
              --no-pheno \
			  --memory 60000 \ 
              --out ~/eur_sibs_snps_with_ldscores_wgs_chr${CHR}
done



# RAP to extract most EUR siblings from WGS, convert to bfiles
UKB_PFILE_DIR="/mnt/project/Bulk/DRAGEN WGS/DRAGEN population level WGS variants, PLINK format [500k release]"
for CHR in {1..22}; do
       plink2 --pfile "${UKB_PFILE_DIR}/ukb24308_c${CHR}_b0_v1" \
              --keep ~/sibreg/sibreg_project/processed/two_eur_sibs_per_fam_sibs_except_five_bad_chr_person_list.txt \
              --threads 15 \
              --no-pheno \
			  --memory 60000 \ 
              --out ~/eur_sibs_snps_wgs_chr${CHR}
done



# RAP to extract all ancestry siblings from WGS, QC filter, filters bfiles
UKB_PFILE_DIR="/mnt/project/Bulk/DRAGEN WGS/DRAGEN population level WGS variants, PLINK format [500k release]"
for CHR in {1..22}; do
	plink2 \
		--pfile "${UKB_PFILE_DIR}/ukb24308_c${CHR}_b0_v1" \
		--keep "$HOME/sibreg/sibreg_project/processed/sibs_all_ancestries.txt" \
		--threads 32 \
		--memory 60000 \
		--geno 0.10 \
		--maf 0.01 \
		--mac 30 \
		--snps-only just-acgt
		--set-missing-var-ids @:#:$r:$a
		--rm-dup force-first
		--max-alleles 2
		--mind 0.05
		--make-bed \
		--out "$HOME/sibreg_project/sibreg/all_sibs_snps_wgs_qc_chr${CHR}"
done


# RAP to extract all ancestry siblings from WGS, QC filter, filters bfiles
for CHR in {1..22}; do
	plink2 \
		--bfile /mnt/project/genotypes/all_sibs_wgs_maf1prct/all_sibs_snps_wgs_maf1prct_chr${CHR} \
		--keep "$HOME/sibreg/sibreg_project/processed/sibs_all_ancestries.txt" \
		--threads 15 \
		--memory 60000 \
		--snps-only just-acgt \
		--max-alleles 2 \
		--geno 0.10 \
		--maf 0.01 \
		--mac 30 \
		--hwe 1e-20 \
		--snps-only just-acgt
		--max-alleles 2
		--mind 0.05
		--make-bed \
		--out "$HOME/sibreg_project/sibreg/all_sibs_snps_wgs_qc_chr${CHR}"
done


