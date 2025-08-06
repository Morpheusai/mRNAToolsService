
from config import CONFIG_YAML

import os
import shutil
import uuid
import subprocess
import json

from src.utils.log import logger
from src.utils.minio_utils import download_from_minio_uri, upload_file_to_minio
from src.protocols import (
    VcfSwitchResponse
)

INPUT_TMP_DIR = CONFIG_YAML["TOOL"]["VCFSWITCH"]["input_tmp_dir"]
OUTPUT_TMP_DIR = CONFIG_YAML["TOOL"]["VCFSWITCH"]["output_tmp_dir"]
BCFTOOLS_IMAGE = CONFIG_YAML["TOOL"]["VCFSWITCH"]["bcftools_image"]
VCF2PROT_IMAGE = CONFIG_YAML["TOOL"]["VCFSWITCH"]["vcf2prot_image"]
HEADER_FILE = CONFIG_YAML["TOOL"]["VCFSWITCH"]["header_file"]
REF_FASTA = CONFIG_YAML["TOOL"]["VCFSWITCH"]["ref_fasta"]
REF_GFF3 = CONFIG_YAML["TOOL"]["VCFSWITCH"]["ref_gff3"]
PROT_FASTA = CONFIG_YAML["TOOL"]["VCFSWITCH"]["protein_fasta"]
PROCESS_BCSQ_SCRIPT = CONFIG_YAML["TOOL"]["VCFSWITCH"]["process_bcsq_script"]

def run_cmd(cmd):
    logger.info(f"运行命令: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

async def run_vcfswitch(
    normal_file: str,  # MinIO 文件路径
    tumor_file: str,   # MinIO 文件路径
) -> str:
    random_folder = str(uuid.uuid4().hex)
    input_tmp_dir_vcf = os.path.join(INPUT_TMP_DIR, random_folder)
    output_tmp_dir = os.path.join(OUTPUT_TMP_DIR, random_folder)
    os.makedirs(input_tmp_dir_vcf, exist_ok=True)
    os.makedirs(output_tmp_dir, exist_ok=True)

    logger.info(f"创建临时输入目录: {input_tmp_dir_vcf}")
    logger.info(f"创建临时输出目录: {output_tmp_dir}")

    try:
        # 1. 下载VCF文件
        logger.info(f"下载normal_file: {normal_file} 到 {input_tmp_dir_vcf}")
        normal_vcf = download_from_minio_uri(normal_file, input_tmp_dir_vcf)
        logger.info(f"下载tumor_file: {tumor_file} 到 {input_tmp_dir_vcf}")
        tumor_vcf = download_from_minio_uri(tumor_file, input_tmp_dir_vcf)

        header_file = HEADER_FILE
        ref_fasta = REF_FASTA
        ref_gff3 = REF_GFF3
        prot_fasta = PROT_FASTA
        process_bcsq_script = PROCESS_BCSQ_SCRIPT

        # 2. 运行主流程
        logger.info("开始执行 bcftools/vcf2prot 主流程...")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"grep -v '^##' {normal_vcf} > {output_tmp_dir}/body_normal.vcf\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"cat {header_file} {output_tmp_dir}/body_normal.vcf > {output_tmp_dir}/normal.fixed.vcf\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"grep -v '^##' {tumor_vcf} > {output_tmp_dir}/body_tumor.vcf\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"cat {header_file} {output_tmp_dir}/body_tumor.vcf > {output_tmp_dir}/tumor.fixed.vcf\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools sort {output_tmp_dir}/normal.fixed.vcf -Oz -o {output_tmp_dir}/normal.sorted.vcf.gz\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"tabix -p vcf {output_tmp_dir}/normal.sorted.vcf.gz\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools sort {output_tmp_dir}/tumor.fixed.vcf -Oz -o {output_tmp_dir}/tumor.sorted.vcf.gz\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"tabix -p vcf {output_tmp_dir}/tumor.sorted.vcf.gz\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools isec -C {output_tmp_dir}/tumor.sorted.vcf.gz {output_tmp_dir}/normal.sorted.vcf.gz -Oz -p {output_tmp_dir}/isec_output\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools view {output_tmp_dir}/isec_output/0000.vcf.gz -Ov -o {output_tmp_dir}/tumor_specific.vcf\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools query -f '%CHROM\\t%POS\\t%REF\\t%ALT\\t%INFO/ANN\\n' {output_tmp_dir}/tumor_specific.vcf > {output_tmp_dir}/tumor_specific.tsv\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools csq -f {ref_fasta} -g {ref_gff3} -p a {output_tmp_dir}/tumor_specific.vcf -Oz -o {output_tmp_dir}/tumor_specific.bcsq.vcf.gz\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"gunzip -c {output_tmp_dir}/tumor_specific.bcsq.vcf.gz > {output_tmp_dir}/tumor_specific.bcsq.vcf\"")
        run_cmd(f"sudo docker exec {VCF2PROT_IMAGE} bash -c \"/target/release/vcf2prot -f {output_tmp_dir}/tumor_specific.bcsq.vcf -r {prot_fasta} -v -g st -o {output_tmp_dir}\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"bcftools query -f '%CHROM\\t%POS\\t%REF\\t%ALT\\t%INFO/AF\\t%INFO/BCSQ\\n' {output_tmp_dir}/tumor_specific.bcsq.vcf.gz > {output_tmp_dir}/bcsq.tsv\"")
        run_cmd(f"sudo docker exec {BCFTOOLS_IMAGE} bash -c \"chmod -R 777 {output_tmp_dir}\"")

        # 查找fasta
        fasta_mut = ""
        for f in os.listdir(output_tmp_dir):
            if f.endswith(".fasta"):
                fasta_mut = os.path.join(output_tmp_dir, f)
                break
        if not fasta_mut:
            logger.error("未找到突变蛋白序列fasta文件")
            raise Exception("未找到突变蛋白序列fasta文件")
        unique_output = os.path.join(output_tmp_dir, "tumor_pep_info_unique.xlsx")
        logger.info(f"运行 process_bcsq_file.py 生成excel: {unique_output}")
        run_cmd(f"python3 {process_bcsq_script} -i {output_tmp_dir}/bcsq.tsv -f {fasta_mut} -u {unique_output} -c 11")

        # 上传excel到minio
        logger.info(f"上传excel到minio: {unique_output}")
        new_filename = f"{random_folder}_tumor_pep_info_unique.xlsx"
        minio_url = upload_file_to_minio(unique_output, CONFIG_YAML['MINIO']['molly_bucket'], new_filename)
        logger.info(f"上传完成，minio路径: {minio_url}")
        return VcfSwitchResponse(type="link",
                                 url= minio_url,
                                 content="文件生成完成"
                                 )
    
    except Exception as e:
        logger.error(f"run_vcfswitch 发生异常: {e}", exc_info=True)
        raise
    finally:
        logger.info(f"清理临时目录: {input_tmp_dir_vcf} 和 {output_tmp_dir}")
        if os.path.exists(input_tmp_dir_vcf):
            shutil.rmtree(input_tmp_dir_vcf)
        if os.path.exists(output_tmp_dir):
            shutil.rmtree(output_tmp_dir)       