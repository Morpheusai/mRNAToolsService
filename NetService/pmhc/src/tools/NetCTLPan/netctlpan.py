import asyncio
import json
import os
import uuid
import traceback

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
from typing import List, Tuple

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
    # print(output_content)
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
    output_dir: str = OUTPUT_TMP_DIR,
    sub_fastas: list = None,  # 新增参数
) -> str:
    """
    拆分FASTA并并发运行NetCTLpan，合并Excel，返回合并后Excel的本地路径。
    :param input_fasta: 原始FASTA文件路径或minio://路径
    :param num_workers: 并行任务数
    :param sub_fastas: 已切割好的分片文件列表（如有则直接用）
    :return: 合并后Excel的本地路径
    """
    try:
        # 1. 保证 input_fasta 是本地文件（如为minio://路径则下载到本地临时目录）
        if input_fasta.startswith("minio://"):
            input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)
        # 2. 拆分FASTA为num_workers个子文件（如果没传sub_fastas）
        if sub_fastas is None:
            split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
            split_dir.mkdir(parents=True, exist_ok=True)
            sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
        for f in sub_fastas:
            print("  -", f, "exists:", Path(f).exists(), "type:", type(f))
            if not isinstance(f, str) or not Path(f).exists():
                raise FileNotFoundError(f"分片文件不存在或不是字符串: {f}")
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
        # 5. 直接返回本地合并Excel路径，不再上传MinIO
        return str(merged_excel)
    except Exception as e:
        print(f"[ERROR] run_netctlpan_parallel 执行异常: {e}")
        traceback.print_exc()
        raise

# 新增：多肽长并行NetCTLpan
async def run_netctlpan_multi_length(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length = -1,  # 支持 int 或 str
    weight_of_tap: float = 0.025,
    weight_of_clevage: float = 0.225,
    epi_threshold: float = 1.0,
    output_threshold: float = -99.9,
    sort_by: int = -1,
    num_workers: int = 1,
    mode: int = 0,
    hla_mode: int=0,
    peptide_duplication_mode: int=0,
    netctlpan_dir: str = NETCTLPAN_DIR,
    output_dir: str = OUTPUT_TMP_DIR,

) -> str:
    """
    支持多肽长并行预测，peptide_length为-1时预测8/9/10/11，为'9,11'时预测9和11，为单个数字时只预测该长度。
    mode==1时，按肽长分组拆分fasta，每个肽长一个文件。
    其它情况只下载/切割一次fasta，所有肽长共用分片，合并所有Excel输出，上传minio并清理中间文件。
    
    并行度分配逻辑：
    - 当只有一个肽长时，使用全部num_workers
    - 当有n个肽长时，每个肽长分配num_workers//n个并行度
    - 如果有余数，将余数分配给前几个肽长
    - 如果总肽长数大于总并发数，则每个肽长至少分配1个并行度
    """
    input_dir = Path(INPUT_TMP_DIR)
    output_dir =Path(OUTPUT_TMP_DIR)

    # 如果hla_mode==1，则mhc_allele只取第一个（逗号分割）
    if hla_mode == 1:
        mhc_allele = mhc_allele.split(",")[0]

    # 创建目录
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. 解析 peptide_length，确保lengths始终为list
    if peptide_length == "-1":
        lengths = [8, 9, 10, 11]
    elif isinstance(peptide_length, str):
        lengths = [int(x) for x in peptide_length.split(",") if x.strip()]
    elif isinstance(peptide_length, int):
        lengths = [peptide_length]
    else:
        try:
            lengths = [int(peptide_length)]
        except Exception:
            raise ValueError(f"peptide_length参数类型不支持: {peptide_length}")
        
    # 2. 动态分配并行度
    num_lengths = len(lengths)
    if num_lengths == 1:
        # 只有一个肽长时，使用全部并行度
        workers_per_length = [num_workers]
    elif num_lengths >= num_workers:
        # 肽长数大于等于总并发数时，每个肽长分配1个并行度
        # 这样可能会超过总并发数，但确保每个肽长都被处理
        workers_per_length = [1] * num_lengths
    else:
        # 肽长数小于总并发数时，平均分配
        base_workers = num_workers // num_lengths
        remainder = num_workers % num_lengths
        workers_per_length = [base_workers] * num_lengths
        # 将余数分配给前几个肽长
        for i in range(remainder):
            workers_per_length[i] += 1
    
    print(f"肽长列表: {lengths}")
    print(f"总并发数: {num_workers}")
    print(f"各肽长分配的并行度: {workers_per_length}")

    if isinstance(input_fasta, str) and input_fasta.startswith("minio://"):
        input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)    


    # 新增：如果peptide_duplication_mode==1，对FASTA文件内容去重
    if peptide_duplication_mode == 1:
        def deduplicate_fasta_by_sequence(fasta_str: str) -> Tuple[str, int, int]:
            lines = fasta_str.strip().split('\n')
            seen_seq = set()
            result = []
            total_before = 0
            total_after = 0
            i = 0
            while i < len(lines):
                if lines[i].startswith('>'):
                    total_before += 1
                    desc = lines[i]
                    seq = lines[i+1] if i+1 < len(lines) else ''
                    if seq not in seen_seq:
                        seen_seq.add(seq)
                        result.append(desc)
                        result.append(seq)
                        total_after += 1
                    i += 2
                else:
                    i += 1
            return '\n'.join(result), total_before, total_after
        # 读取、去重、写回
        with open(input_fasta, 'r', encoding='utf-8') as f:
            fasta_content = f.read()
        deduped, total_before, total_after = deduplicate_fasta_by_sequence(fasta_content)
        print(f"去重前肽段总数: {total_before}")
        print(f"去重后肽段总数: {total_after}")
        with open(input_fasta, 'w', encoding='utf-8') as f:
            f.write(deduped)

    # 2. mode==1且肽长只包含8/9/10/11时，按肽长分组
    if mode == 1 and all(l in [8,9,10,11] for l in lengths):
            
        split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
        split_dir.mkdir(parents=True, exist_ok=True)
        sub_fastas = split_fasta_by_length(input_fasta, lengths, str(split_dir))
        # 过滤掉空文件和对应的length
        non_empty_fastas = []
        non_empty_lengths = []
        non_empty_workers = []
        for i, f in enumerate(sub_fastas):
            try:
                if Path(f).stat().st_size > 0:
                    non_empty_fastas.append(f)
                    non_empty_lengths.append(lengths[i])
                    non_empty_workers.append(workers_per_length[i])
            except Exception as e:
                print(f"[WARN] 检查分组FASTA文件大小失败: {f}, {e}")
        
        tasks = [
            run_netctlpan_parallel(
                non_empty_fastas[i], mhc_allele, non_empty_lengths[i], weight_of_tap, weight_of_clevage,
                epi_threshold, output_threshold, sort_by, non_empty_workers[i], netctlpan_dir, output_dir
                # 分组模式下不传sub_fastas参数，使用动态分配的并行度
            )
            for i in range(len(non_empty_fastas))
        ]

        excel_files = await asyncio.gather(*tasks)
        # 5. 合并所有excel
        merged_excel = Path(output_dir) / f"merged_multi_{uuid.uuid4().hex}_NetCTLpan_results.xlsx"
        merge_excels(excel_files, str(merged_excel))
        # 6. 上传合并后的Excel到MinIO
        minio_excel_path = upload_file_to_minio(str(merged_excel), MINIO_BUCKET)
        # 7. 删除所有中间excel和分组fasta和合并excel
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
                print(f"[WARN] 删除分组FASTA失败: {f}, {e}")
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
            print(f"[WARN] 删除分组目录失败: {split_dir}, {e}")
            traceback.print_exc()
             
        return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetCTLpan多肽长并行处理完成，结果已合并。"}, ensure_ascii=False)
    else:
        
        # 2. 切割一次fasta
        split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
        split_dir.mkdir(parents=True, exist_ok=True)
        sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
        # 4. 针对每个肽长并发run_netctlpan_parallel，传入同一批分片
        try:
            tasks = [
                run_netctlpan_parallel(
                    input_fasta, mhc_allele, l, weight_of_tap, weight_of_clevage,
                    epi_threshold, output_threshold, sort_by, workers_per_length[i], netctlpan_dir, output_dir,
                    sub_fastas=sub_fastas
                )
                for i, l in enumerate(lengths)
            ]
            excel_files = await asyncio.gather(*tasks)
            # 5. 合并所有excel
            merged_excel = Path(output_dir) / f"merged_multi_{uuid.uuid4().hex}_NetCTLpan_results.xlsx"
            merge_excels(excel_files, str(merged_excel))
            # 6. 上传合并后的Excel到MinIO
            minio_excel_path = upload_file_to_minio(str(merged_excel), MINIO_BUCKET)
        except Exception as e:
            print(f"[ERROR] run_netctlpan_multi_length 分片并发/合并/上传异常: {e}")
            traceback.print_exc()
            raise
        # 7. 删除所有中间excel和分片fasta和合并excel
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
 
        return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetCTLpan多肽长并行处理完成，结果已合并。"}, ensure_ascii=False)



# # 新增：按肽长分组拆分fasta
# def split_fasta_by_length(input_fasta: str, lengths: List[int], output_dir: str) -> List[str]:
#     """
#     按肽长分组拆分fasta，每个肽长一个文件，返回文件路径列表，顺序与lengths一致。
#     """
#     from pathlib import Path
#     out_map = {l: [] for l in lengths}
#     with open(input_fasta, 'r') as f:
#         lines = f.readlines()
#     current = []
#     for line in lines:
#         if line.startswith('>'):
#             if current:
#                 for l in lengths:
#                     if f'len{l}' in current[0]:
#                         out_map[l].append(''.join(current))
#                         break
#             current = [line]
#         else:
#             current.append(line)
#     if current:
#         for l in lengths:
#             if f'len{l}' in current[0]:
#                 out_map[l].append(''.join(current))
#                 break
#     out_files = []
#     for l in lengths:
#         out_path = Path(output_dir) / f"split_len{l}.fasta"
#         with open(out_path, 'w') as f:
#             f.writelines(out_map[l])
#         out_files.append(str(out_path))
#     return out_files

# 新增：按肽长分组拆分fasta
def split_fasta_by_length(input_fasta: str, lengths: list, output_dir: str) -> list:
    """
    按实际序列长度分组fasta，每个肽长一个文件，返回文件路径列表，顺序与lengths一致。
    """
    from pathlib import Path
    out_map = {l: [] for l in lengths}
    with open(input_fasta, 'r') as f:
        lines = f.readlines()
    current = []
    for line in lines:
        if line.startswith('>'):
            if current:
                seq = ''.join(current[1:]).replace('\n', '')
                seq_len = len(seq)
                if seq_len in out_map:
                    out_map[seq_len].append(''.join(current))
            current = [line]
        else:
            current.append(line)
    if current:
        seq = ''.join(current[1:]).replace('\n', '')
        seq_len = len(seq)
        if seq_len in out_map:
            out_map[seq_len].append(''.join(current))
    out_files = []
    for l in lengths:
        out_path = Path(output_dir) / f"split_len{l}.fasta"
        with open(out_path, 'w') as f:
            f.writelines(out_map[l])
        out_files.append(str(out_path))
    return out_files

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