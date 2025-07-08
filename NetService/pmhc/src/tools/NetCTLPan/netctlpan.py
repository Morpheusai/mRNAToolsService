import asyncio
import json
import os
import uuid

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pathlib import Path

from config import CONFIG_YAML
from src.tools.NetCTLPan.filter_netctlpan import filter_netctlpan_output
from src.tools.NetCTLPan.netctlpan_to_excel import save_excel
from src.utils.log import logger
from src.utils.parallel_utils import split_fasta, run_commands_async, merge_excels
from src.utils.minio_utils import download_from_minio_uri, upload_file_to_minio

load_dotenv()
# MinIO 配置:
MINIO_CONFIG = CONFIG_YAML["MINIO"]
MINIO_ENDPOINT = MINIO_CONFIG["endpoint"]
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = MINIO_CONFIG["netctlpan_bucket"]
MINIO_SECURE = MINIO_CONFIG.get("secure", False)

# netCTLpan 配置
NETCTLPAN_DIR = CONFIG_YAML["TOOL"]["NETCTLPAN"]["netctlpan_dir"]
INPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETCTLPAN"]["input_tmp_netctlpan_dir"]
DOWNLOADER_PREFIX = CONFIG_YAML["TOOL"]["COMMON"]["output_download_url_prefix"]
OUTPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETCTLPAN"]["output_tmp_netctlpan_dir"]



# 单FASTA并行NetCTLpan
async def run_netctlpan_single(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length: int = -1,
    weight_of_tap: float = 0.025,
    weight_of_clevage: float = 0.225,
    epi_threshold: float = 1.0,
    output_threshold: float = -99.9,
    sort_by: int = -1,
    netctlpan_dir: str = NETCTLPAN_DIR,
    output_dir: str = OUTPUT_TMP_DIR
) -> str:
    """
    单个FASTA文件运行NetCTLpan，返回Excel路径。
    该函数用于并行主流程的子任务，也可单独调用。
    :param input_fasta: 单个FASTA文件路径
    :return: 生成的Excel文件路径
    """
    random_id = uuid.uuid4().hex
    input_path = Path(INPUT_TMP_DIR) / f"{random_id}.fsa"
    # 拷贝输入内容到临时文件
    with open(str(input_path), "w") as f:
        with open(input_fasta, "r") as fin:
            f.write(fin.read())
    output_filename = f"{random_id}_NetCTLpan_results.xlsx"
    output_path = Path(output_dir) / output_filename
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")
    print(input_path)
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")
    print("1111111111111111111111111111111111111111111111111")

    # 构建命令行参数
    cmd = [
        f"{netctlpan_dir}/netCTLpan",
        "-f", str(input_path),
        "-a", mhc_allele,
        "-wt", str(weight_of_tap),
        "-wc", str(weight_of_clevage),
        "-ethr", str(epi_threshold),
        "-thr", str(output_threshold),
        "-s", str(sort_by)
    ]
    if peptide_length != -1:
        cmd.extend(["-l", str(peptide_length)])
    # 启动外部命令，异步等待完成
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=f"{netctlpan_dir}"
    )
    stdout, stderr = await proc.communicate()
    output_content = stdout.decode()
    # 保存命令输出为Excel
    save_excel(output_content, str(output_dir), output_filename)
    # input_path.unlink(missing_ok=True)
    return str(output_path)

# 并行主流程
async def run_netctlpan_parallel(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length: int = -1,
    weight_of_tap: float = 0.025,
    weight_of_clevage: float = 0.225,
    epi_threshold: float = 1.0,
    output_threshold: float = -99.9,
    sort_by: int = -1,
    num_workers: int = 1,
    netctlpan_dir: str = NETCTLPAN_DIR,
    output_dir: str = OUTPUT_TMP_DIR
) -> str:
    """
    拆分FASTA并并发运行NetCTLpan，合并Excel，返回合并后Excel的MinIO路径。
    :param input_fasta: 原始FASTA文件路径或minio://路径
    :param num_workers: 并行任务数
    :return: 合并后Excel的MinIO路径
    """
    # 1. 保证 input_fasta 是本地文件（如为minio://路径则下载到本地临时目录）
    if input_fasta.startswith("minio://"):
        # 使用minio_utils的download_from_minio_uri下载
        input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)
    # 2. 拆分FASTA为num_workers个子文件
    split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
    split_dir.mkdir(parents=True, exist_ok=True)
    sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
    # 3. 并发调度每个子文件的NetCTLpan运行
    async def run_one(sub_fasta, *_):
        return await run_netctlpan_single(
            sub_fasta, mhc_allele, peptide_length, weight_of_tap, weight_of_clevage,
            epi_threshold, output_threshold, sort_by, netctlpan_dir, output_dir
        )
    excel_files = await run_commands_async(run_one, sub_fastas, num_workers=num_workers)
    # 4. 合并所有Excel为一个总表
    merged_excel = Path(output_dir) / f"merged_{uuid.uuid4().hex}_NetCTLpan_results.xlsx"
    merge_excels(excel_files, str(merged_excel))
    # 5. 上传合并后的Excel到MinIO，返回minio路径
    minio_excel_path = upload_file_to_minio(
        str(merged_excel),
        MINIO_BUCKET
    )
    # 6. 清理子文件和分片Excel
    for f in sub_fastas:
        print(f"\n[DEBUG] 即将删除的分片FASTA文件: {f}（本次未删除，供人工检查）\n")
        # Path(f).unlink(missing_ok=True)
    for f in excel_files:
        print(f"\n[DEBUG] 即将删除的分片Excel文件: {f}（本次未删除，供人工检查）\n")
        # Path(f).unlink(missing_ok=True)
    print(f"\n[DEBUG] 即将删除的分片目录: {split_dir}（本次未删除，供人工检查）\n")
    # split_dir.rmdir()
    print(f"\n[DEBUG] 即将删除的本地FASTA文件: {input_fasta}（本次未删除，供人工检查）\n")
    # Path(input_fasta).unlink(missing_ok=True)
    print(f"\n[DEBUG] 即将删除的合并后Excel: {merged_excel}（本次未删除，供人工检查）\n")
    # Path(merged_excel).unlink(missing_ok=True)
    return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetCTLpan并行处理完成，结果已合并。"}, ensure_ascii=False)

# def NetCTLpan(
#     input_filename: str,
#     mhc_allele: str = "HLA-A02:01",
#     peptide_length: int = -1,
#     weight_of_tap: float = 0.025,
#     weight_of_clevage: float = 0.225,
#     epi_threshold: float = 1.0,
#     output_threshold: float = -99.9,
#     sort_by: int = -1
# ) -> str:
#     """
#     使用NetCTLpan工具预测肽段序列与指定MHC分子的结合亲和力，用于筛选潜在的免疫原性肽段。
#     该函数结合蛋白质裂解、TAP转运和MHC结合的预测，适用于疫苗设计和免疫研究。
#     :param input_filename: 输入的FASTA格式肽段序列文件路径
#     :param mhc_allele: 用于比对的MHC等位基因名称，默认为"HLA-A02:01"
#     :param peptide_length: 预测的肽段长度，-1表示不加-l参数，默认9
#     :param weight_of_tap: TAP转运效率预测的权重，默认为0.025
#     :param weight_of_clevage: 蛋白质裂解预测的权重，默认为0.225
#     :param epi_threshold: 表位阈值，默认1.0
#     :param output_threshold: 输出得分阈值，默认-99.9
#     :param sort_by: 排序方式，默认-1
#     :return: 返回预测结果字符串，包含高亲和力肽段信息
#     """
#     try:
#         # 调用异步函数并获取返回结果
#         result = asyncio.run(run_netctlpan(
#             input_filename, mhc_allele, peptide_length, weight_of_tap, weight_of_clevage, epi_threshold, output_threshold, sort_by))
#         return result
#     except Exception as e:
#         # 捕获并返回异常信息
#         result = {
#             "type": "text",
#             "content": f"调用NetCTLpan工具失败: {str(e)}"
#         }
#         return json.dumps(result, ensure_ascii=False)


# if __name__ == "__main__":
#     print(asyncio.run(run_netctlpan_single(
#         input_fasta="minio://molly/2ad83c64-0440-4d70-80bf-8a0054c0ecac_B0702.fsa", peptide_length=9)))
