import asyncio
import json
import os
import sys
import uuid
import traceback
import datetime
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pathlib import Path
from typing import List

from config import CONFIG_YAML
from src.tools.NetChop.filter_netchop import filter_netchop_output
from src.tools.NetChop.netchop_to_excel import save_excel
from src.utils.log import logger
from src.utils.parallel_utils import split_fasta, run_commands_async, merge_excels
from src.utils.minio_utils import download_from_minio_uri, upload_file_to_minio
from src.utils.utils import deduplicate_fasta_by_sequence

load_dotenv()
# MinIO 配置:
MINIO_CONFIG = CONFIG_YAML["MINIO"]
MINIO_BUCKET = MINIO_CONFIG["netchop_bucket"]

# netchop 配置 
NETCHOP_DIR = CONFIG_YAML["TOOL"]["NETCHOP"]["netchop_dir"]
INPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETCHOP"]["input_tmp_netchop_dir"]
DOWNLOADER_PREFIX = CONFIG_YAML["TOOL"]["COMMON"]["output_download_url_prefix"]
OUTPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETCHOP"]["output_tmp_netchop_dir"]



#获取滑窗肽段文件
def sliding_window_from_file(input_file: str, window_sizes: List[int], output_file: str) -> None:
    """
    从 FASTA 文件读取序列，进行滑窗切割，并输出到新的 FASTA 文件。
    标识头格式：>原标识_子序列_子序列长度

    参数:
        input_file: 输入 FASTA 文件路径（.fasta 或 .fsa）。
        window_sizes: 滑窗长度的列表（如 [8, 9, 10]）。
        output_file: 输出 FASTA 文件路径。
    """
    # 读取输入文件
    with open(input_file, 'r') as f:
        fasta_content = f.read()

    # 解析原始 FASTA，允许同名头，全部保留
    peptides = []
    current_id = None
    current_seq = []

    for line in fasta_content.split('\n'):
        line = line.strip()
        if line.startswith('>'):
            if current_id is not None:
                peptides.append((current_id, ''.join(current_seq)))
            current_id = line[1:]  # 去掉 '>'
            current_seq = []
        else:
            if line:  # 忽略空行
                current_seq.append(line)

    if current_id is not None:  # 添加最后一条序列
        peptides.append((current_id, ''.join(current_seq)))

    # 生成滑窗子序列并格式化为 FASTA
    output_lines = []
    for peptide_id, seq in peptides:
        for window in window_sizes:
            if window > len(seq):
                continue  # 跳过无效窗口
            for i in range(len(seq) - window + 1):
                subseq = seq[i:i+window]
                # 新标识符格式：>原标识_子序列_子序列长度
                header = f">{peptide_id}_{subseq}_{window}"
                output_lines.append(header)
                output_lines.append(subseq)

    # 写入输出文件
    with open(output_file, 'w') as f:
        f.write('\n'.join(output_lines))



# 新增：单文件处理逻辑（原run_netchop主体，便于并行调用）
async def run_netchop_single(
    input_fasta: str,
    cleavage_site_threshold: float = 0.5,
    model: int = 0,
    format: int = 0,
    strict: int = 0,
    netchop_dir: str = NETCHOP_DIR,
    output_dir: str = OUTPUT_TMP_DIR
) -> str:
    """
    单个FASTA文件运行NetChop，返回Excel路径。
    该函数用于并行主流程的子任务，也可单独调用。
    :param input_fasta: 单个FASTA文件路径
    :return: 生成的Excel文件路径
    """
    random_id = uuid.uuid4().hex
    input_dir = Path(INPUT_TMP_DIR)
    output_dir = Path(OUTPUT_TMP_DIR)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    input_path = input_dir / f"{random_id}.fsa"
    with open(str(input_path), "w") as f:
        with open(input_fasta, "r") as fin:
            f.write(fin.read())
    output_filename = f"{random_id}_NetChop_results.xlsx"
    output_path = output_dir / output_filename
    cmd = [
        f"{netchop_dir}/netchop",
        "-t", str(cleavage_site_threshold),
        "-v", str(model),
        "-s" if format == 1 else "",
        "-ostrict" if strict == 1 else "",
        str(input_path)
    ]
    
    # 过滤掉空字符串
    cmd = [arg for arg in cmd if arg]

    # 启动外部命令，异步等待完成
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=f"{netchop_dir}"
    )
    stdout, stderr = await proc.communicate()
    output_content = stdout.decode()
    
    # 保存命令输出为Excel
    save_excel(output_content, str(output_dir), output_filename)
    input_path.unlink(missing_ok=True)
    return str(output_path)

# 新增：并行处理逻辑
async def run_netchop_parallel(
    input_fasta: str,
    cleavage_site_threshold: float = 0.5,
    model: int = 0,
    format: int = 0,
    strict: int = 0,
    num_workers: int = 1,
    window_sizes: List[int] =[8,9,10,11],
    netchop_dir: str = NETCHOP_DIR,
    output_dir: str = OUTPUT_TMP_DIR
) -> str:
    # 1. 拆分FASTA
    if isinstance(input_fasta, str) and input_fasta.startswith("minio://"):
        input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)

    # 读取、去重、写回
    with open(input_fasta, 'r', encoding='utf-8') as f:
        fasta_content = f.read()
    deduped, total_before, total_after = deduplicate_fasta_by_sequence(fasta_content)
    print(f"输入文件去重前肽段总数: {total_before}")
    print(f"输入文件去重后肽段总数: {total_after}")
    with open(input_fasta, 'w', encoding='utf-8') as f:
        f.write(deduped)        

    sliding_window_from_file(input_fasta, window_sizes, input_fasta)

    # 读取、去重、写回
    with open(input_fasta, 'r', encoding='utf-8') as f:
        fasta_content = f.read()
    deduped, total_before, total_after = deduplicate_fasta_by_sequence(fasta_content)
    print(f"滑窗得到去重前肽段总数: {total_before}")
    print(f"滑窗得到去重后肽段总数: {total_after}")
    with open(input_fasta, 'w', encoding='utf-8') as f:
        f.write(deduped)

    split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
    split_dir.mkdir(parents=True, exist_ok=True)
    sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
    # 2. 并发调度
    async def run_one(sub_fasta, *_):
        return await run_netchop_single(
            sub_fasta, cleavage_site_threshold, model, format, strict, netchop_dir, output_dir
        )
    excel_files = await run_commands_async(run_one, sub_fastas, num_workers=num_workers)
    # 3. 合并Excel
    merged_excel = Path(output_dir) / f"merged_{uuid.uuid4().hex}_NetChop_results.xlsx"
    merge_excels(excel_files, str(merged_excel))
    # 4. 先上传合并后的Excel到MinIO
    beijing_time = datetime.now(ZoneInfo("Asia/Shanghai"))
    time_str = beijing_time.strftime('%Y-%m-%d_%H-%M-%S')
    tool_output_filename = f"{uuid.uuid4().hex}_NetChop_results_{time_str}.xlsx"
    minio_excel_path = upload_file_to_minio(str(merged_excel), MINIO_BUCKET, tool_output_filename)
    # 5. 清理中间excel、分片fasta和分片目录
    for f in excel_files:
        try:
            if Path(f).exists():
                Path(f).unlink()
        except Exception as e:
            print(f"[WARN] 删除中间Excel失败: {f}, {e}")
            traceback.print_exc()
    for f in sub_fastas:
        try:
            if Path(f).exists():
                Path(f).unlink()
        except Exception as e:
            print(f"[WARN] 删除分片FASTA失败: {f}, {e}")
            traceback.print_exc()
    try:
        if merged_excel.exists():
            merged_excel.unlink()
    except Exception as e:
        print(f"[WARN] 删除合并Excel失败: {merged_excel}, {e}")
        traceback.print_exc()
    try:
        if split_dir.exists():
            split_dir.rmdir()
    except Exception as e:
        print(f"[WARN] 删除分片目录失败: {split_dir}, {e}")
        traceback.print_exc()

    return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetChop并行处理完成，结果已合并。"}, ensure_ascii=False)


# def NetChop(input_filename: str, cleavage_site_threshold: float = 0.5, model: int = 0, format: int = 0, strict: int = 0) -> str:
#     """                                    
#     NetChops是一种用于预测蛋白质序列中蛋白酶体切割位点的生物信息学工具。
#     Args:                                  
#         input_filename (str): 输入的肽段序例fasta文件路径           
#         cleavage_site_threshold (float): 设定切割位点的置信度阈值（范围：0.0 ~ 1.0），默认值0.5
#         model (int): 预测模型版本，0-Cterm3.0，1-20S-3.0，默认值0
#         format (int): 输出格式，0-长格式，1-短格式，默认值0
#         strict (int): 严格模式，0-开启严格模式，1-关闭严格模式，默认值0
#     Returns:                               
#         str: 返回高结合亲和力的肽段序例信息                                                                                                                           
#     """
#     try:
#         return asyncio.run(run_netchop(input_filename, cleavage_site_threshold, model, format, strict))

#     except Exception as e:
#         result = {
#             "type": "text",
#             "content": f"调用NetChop工具失败: {e}"
#         }
#         return json.dumps(result, ensure_ascii=False)
