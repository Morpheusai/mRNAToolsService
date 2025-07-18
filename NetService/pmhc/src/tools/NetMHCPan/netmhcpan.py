import asyncio
import json
import os
import sys
import uuid
import datetime

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error 
from pathlib import Path

from src.tools.NetMHCPan.filter_netmhcpan import filter_netmhcpan_excel
from src.tools.NetMHCPan.netmhcpan_to_excel import save_excel
from src.utils.parallel_utils import split_fasta, run_commands_async, merge_excels
from src.utils.minio_utils import download_from_minio_uri, upload_file_to_minio
import traceback
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo

load_dotenv()
current_file = Path(__file__).resolve()
project_root = current_file.parents[3] 
                                        
# 将项目根目录添加到 sys.path
sys.path.append(str(project_root))
from config import CONFIG_YAML

# MinIO 配置:
MINIO_CONFIG = CONFIG_YAML["MINIO"]
MINIO_ENDPOINT = MINIO_CONFIG["endpoint"]
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = MINIO_CONFIG["netmhcpan_bucket"]
MINIO_SECURE = MINIO_CONFIG.get("secure", False)

# netMHCpan 配置 
NETMHCPAN_DIR = CONFIG_YAML["TOOL"]["NETMHCPAN"]["netmhcpan_dir"]
INPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETMHCPAN"]["input_tmp_netmhcpan_dir"]
DOWNLOADER_PREFIX = CONFIG_YAML["TOOL"]["COMMON"]["output_download_url_prefix"]
OUTPUT_TMP_DIR = CONFIG_YAML["TOOL"]["NETMHCPAN"]["output_tmp_netmhcpan_dir"]

# # 初始化 MinIO 客户端
# minio_client = Minio(
#     MINIO_ENDPOINT,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     secure=MINIO_SECURE
# )
# #检查minio是否可用
# def check_minio_connection(bucket_name=MINIO_BUCKET):
#     try:
#         minio_client.list_buckets()
#         if not minio_client.bucket_exists(bucket_name):
#             minio_client.make_bucket(bucket_name)
#         return True
#     except S3Error as e:
#         print(f"MinIO连接或bucket操作失败: {e}")
#         return False


# async def run_netmhcpan(
#     input_filename: str,  # MinIO 文件路径，格式为 "bucket-name/file-path"
#     mhc_allele: str = "HLA-A02:01",  # HLA 等位基因（MHC 分子类型）
#     peptide_length: str = "-1",  # 肽段长度，范围8-11，-1表示使用默认值
#     high_threshold_of_bp: float = 0.5,  # 高结合力肽段的阈值
#     low_threshold_of_bp: float = 2.0,  # 低结合力肽段的阈值
#     rank_cutoff: float = -99.9,  # 输出结果的%Rank截断值
#     netmhcpan_dir: str = NETMHCPAN_DIR
#     ) -> str:

#     """
#     异步运行 netMHCpan 并将处理后的结果上传到 MinIO
#     :param input_filename: MinIO 文件路径，格式为 "bucket-name/file-path"
#     :param mhc_allele: HLA 等位基因（MHC 分子类型），默认值HLA-A02:01
#     :param peptide_length: 肽段长度，范围8-11，-1表示使用默认值，默认值-1
#     :param high_threshold_of_bp: 高结合力肽段的阈值，默认值0.5
#     :param low_threshold_of_bp: 低结合力肽段的阈值，默认值2.0
#     :param rank_cutoff: 输出结果的%Rank截断值，默认值-99.9
#     :param netmhcpan_dir: netMHCpan 安装目录
#     :return: JSON 字符串，包含 MinIO 文件路径（或下载链接）
#     """

#     minio_available = check_minio_connection()
#     #提取桶名和文件
#     try:
#         # 去掉 minio:// 前缀
#         path_without_prefix = input_filename[len("minio://"):]
        
#         # 找到第一个斜杠的位置，用于分割 bucket_name 和 object_name
#         first_slash_index = path_without_prefix.find("/")
        
#         if first_slash_index == -1:
#             raise ValueError("Invalid file path format: missing bucket name or object name")
        
#         # 提取 bucket_name 和 object_name
#         bucket_name = path_without_prefix[:first_slash_index]
#         object_name = path_without_prefix[first_slash_index + 1:]
        
#         # 打印提取结果（可选）
#         # logger.info(f"Extracted bucket_name: {bucket_name}, object_name: {object_name}")
        
#     except Exception as e:
#         # logger.error(f"Failed to parse file_path: {file_path}, error: {str(e)}")
#         raise str(status_code=400, detail=f"Failed to parse file path: {str(e)}")     

#     try:
#         response = minio_client.get_object(bucket_name, object_name)
#         file_content = response.read().decode("utf-8")
#     except S3Error as e:
#         return json.dumps({
#             "type": "text",
#             "content": f"无法从 MinIO 读取文件: {str(e)}"
#         }, ensure_ascii=False)    

#     # 生成随机ID和文件路径
#     random_id = uuid.uuid4().hex
#     #base_path = Path(__file__).resolve().parents[3]  # 根据文件位置调整层级
#     input_dir = Path(INPUT_TMP_DIR)
#     output_dir =Path(OUTPUT_TMP_DIR)
    

#     # 创建目录
#     input_dir.mkdir(parents=True, exist_ok=True)
#     output_dir.mkdir(parents=True, exist_ok=True)

#     # 写入输入文件
#     input_path = input_dir / f"{random_id}.fsa"
#     with open(input_path, "w") as f:
#         f.write(file_content)

#     # 构建输出文件名和临时路径
#     output_filename = f"{random_id}_NetMHCpan_results.xlsx"
#     output_path = output_dir / output_filename

#     # 构建命令
#     cmd = [
#         f"{netmhcpan_dir}/netMHCpan",
#         "-BA",
#         "-a", mhc_allele,  # HLA 等位基因
#         "-rth", str(high_threshold_of_bp),  # 高结合力肽段阈值
#         "-rlt", str(low_threshold_of_bp),  # 低结合力肽段阈值
#         "-t", str(rank_cutoff),  # 输出结果%Rank截断值
#         str(input_path)  # 输入文件路径
#     ]
    
#     # 只有当peptide_length不为-1时才添加-l参数
#     if peptide_length != -1:
#         cmd.insert(-1, "-l")  # 在输入文件路径前插入-l
#         cmd.insert(-1, str(peptide_length))  # 在-l后插入peptide_length值

#     # 过滤掉空字符串
#     cmd = [arg for arg in cmd if arg]
#     # 启动异步进程
#     proc = await asyncio.create_subprocess_exec(
#         *cmd,
#         stdout=asyncio.subprocess.PIPE,
#         stderr=asyncio.subprocess.PIPE,
#         cwd=f"{netmhcpan_dir}"
#     )

#     # 处理输出
#     stdout, stderr = await proc.communicate()
#     output = stdout.decode()
#     # stdout_text = stdout.decode()
#     # stderr_text = stderr.decode()
#     # print(f"stdout:{stdout_text}")
#     # print(f"stderr:{stderr_text}")
#     # exit()
#     print("11111111111111111111111111111111111111")
#     print("11111111111111111111111111111111111111")
#     print("11111111111111111111111111111111111111")        
#     print(output)
#     save_excel(output,output_dir,output_filename)

#     # # 直接将所有内容写入文件
#     # with open(output_path, "w") as f:
#     #     f.write("\n".join(output.splitlines()))
       
#     # 调用过滤函数
#     # filtered_content = filter_netmhcpan_excel(output_path)
    
#     # 错误处理
#     if proc.returncode != 0:
#         error_msg = stderr.decode()
#         input_path.unlink(missing_ok=True)
#         output_path.unlink(missing_ok=True)
#         result = {
#             "type": "text",
#             "content": "您的输入信息可能有误，请核对正确再试。"
#         }
#     else:
#         try:
#             if minio_available:
#                 minio_client.fput_object(
#                     MINIO_BUCKET,
#                     output_filename,
#                     str(output_path)
#                 )
#                 file_path = f"minio://{MINIO_BUCKET}/{output_filename}"
#             else:
#                 # 如果 MinIO 不可用，返回下载链接
#                 file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
#         except S3Error as e:
#             file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
#         finally:
#             # 如果 MinIO 成功上传，清理临时文件；否则保留
#             if minio_available:
#                 input_path.unlink(missing_ok=True)
#                 output_path.unlink(missing_ok=True)
#             else:
#                 input_path.unlink(missing_ok=True)  # 只删除输入文件，保留输出文件

#         # 返回结果
#         # result = {
#         #     "type": "link",
#         #     "url": file_path,
#         #     "content": filtered_content  # 替换为生成的 Markdown 内容
#         # }
#         result = {
#             "type": "link",
#             "url": file_path,
#             "content": "NetMHCPan处理完成"  
#         }
        

#     return json.dumps(result, ensure_ascii=False)


# 单FASTA并行NetMHCPan
async def run_netmhcpan_single(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length: int = -1,
    high_threshold_of_bp: float = 0.5,
    low_threshold_of_bp: float = 2.0,
    rank_cutoff: float = -99.9,
    netmhcpan_dir: str = NETMHCPAN_DIR,
    output_dir: str = OUTPUT_TMP_DIR
) -> str:
    try:
        random_id = uuid.uuid4().hex
        input_path = Path(INPUT_TMP_DIR) / f"{random_id}.fsa"
        # 拷贝输入内容到临时文件
        with open(str(input_path), "w") as f:
            with open(input_fasta, "r") as fin:
                f.write(fin.read())
        output_filename = f"{random_id}_NetMHCpan_results.xlsx"
        output_path = Path(output_dir) / output_filename
        # 构建命令行参数
        cmd = [
            f"{netmhcpan_dir}/netMHCpan",
            "-BA",
            "-a", mhc_allele,
            "-rth", str(high_threshold_of_bp),
            "-rlt", str(low_threshold_of_bp),
            "-t", str(rank_cutoff),
            str(input_path)
        ]
        if peptide_length != -1:
            cmd.insert(-1, "-l")
            cmd.insert(-1, str(peptide_length))
        cmd = [arg for arg in cmd if arg]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=f"{netmhcpan_dir}"
        )
        stdout, stderr = await proc.communicate()
        output_content = stdout.decode()
        save_excel(output_content, str(output_dir), output_filename)
        # input_path.unlink(missing_ok=True)
        return str(output_path)
    except Exception as e:
        print(f"[ERROR] run_netmhcpan_single 执行异常: {e}")
        traceback.print_exc()
        raise

# 并行主流程
async def run_netmhcpan_parallel(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length: int = -1,
    high_threshold_of_bp: float = 0.5,
    low_threshold_of_bp: float = 2.0,
    rank_cutoff: float = -99.9,
    num_workers: int = 1,
    netmhcpan_dir: str = NETMHCPAN_DIR,
    output_dir: str = OUTPUT_TMP_DIR,
    sub_fastas: list = None,
) -> str:
    try:
        print(f"run_netmhcpan_parallel: 进入函数, input_fasta={input_fasta}, peptide_length={peptide_length}, sub_fastas={sub_fastas}")
        if input_fasta.startswith("minio://"):
            input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)
        if sub_fastas is None:
            split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
            split_dir.mkdir(parents=True, exist_ok=True)
            sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
        for f in sub_fastas:
            if not isinstance(f, str) or not Path(f).exists():
                raise FileNotFoundError(f"分片文件不存在或不是字符串: {f}")
        async def run_one(sub_fasta, *_):
            print(f"run_one: 处理分片 {sub_fasta}")
            return await run_netmhcpan_single(
                sub_fasta, mhc_allele, peptide_length, high_threshold_of_bp, low_threshold_of_bp,
                rank_cutoff, netmhcpan_dir, output_dir
            )
        excel_files = await run_commands_async(run_one, sub_fastas, num_workers=num_workers)
        merged_excel = Path(output_dir) / f"merged_{uuid.uuid4().hex}_NetMHCpan_results.xlsx"
        merge_excels(excel_files, str(merged_excel))
        return str(merged_excel)
    except Exception as e:
        print(f"[ERROR] run_netmhcpan_parallel 执行异常: {e}")
        traceback.print_exc()
        raise

# 完全仿照netctlpan.py的多肽长并发逻辑
async def run_netmhcpan_multi_length(
    input_fasta: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length = -1,
    high_threshold_of_bp: float = 0.5,
    low_threshold_of_bp: float = 2.0,
    rank_cutoff: float = -99.9,
    num_workers: int = 1,
    mode: int = 0,
    netmhcpan_dir: str = NETMHCPAN_DIR,
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
    try:
        input_dir = Path(INPUT_TMP_DIR)
        output_dir =Path(OUTPUT_TMP_DIR)

        # 创建目录
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        import json
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
        
        # 2. mode==1且肽长只包含8/9/10/11时，按肽长分组
        if mode == 1 and all(l in [8,9,10,11] for l in lengths):
            if isinstance(input_fasta, str) and input_fasta.startswith("minio://"):
                input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)
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
                    traceback.print_exc()
            tasks = [
                run_netmhcpan_parallel(
                    non_empty_fastas[i], mhc_allele, non_empty_lengths[i], high_threshold_of_bp, low_threshold_of_bp,
                    rank_cutoff,  non_empty_workers[i], netmhcpan_dir, output_dir
                    # 分组模式下不传sub_fastas参数，使用动态分配的并行度
                )
                for i in range(len(non_empty_fastas))
            ]
            print("tasks内容：", tasks)
            print("tasks类型：", [type(t) for t in tasks])
            try:
                excel_files = await asyncio.gather(*tasks)
                for i, res in enumerate(excel_files):
                    if isinstance(res, Exception):
                        print(f"[ERROR] 子任务{i} 执行异常: {res}")
                        traceback.print_exception(type(res), res, res.__traceback__)
                # 过滤掉异常和无效文件
                valid_excels = [f for f in excel_files if isinstance(f, str) and Path(f).exists()]
                if not valid_excels:
                    print("[ERROR] 没有生成任何有效的Excel文件，无法合并！")
                    raise RuntimeError("没有生成任何有效的Excel文件，无法合并！")
            except Exception as e:
                print(f"[ERROR] gather tasks 执行异常: {e}")
                traceback.print_exc()
                raise
            # 5. 合并所有excel
            merged_excel = Path(output_dir) / f"merged_multi_{uuid.uuid4().hex}_NetMHCPan_results.xlsx"
            merge_excels(valid_excels, str(merged_excel))
            # 6. 上传合并后的Excel到MinIO
            beijing_time = datetime.now(ZoneInfo("Asia/Shanghai"))
            time_str = beijing_time.strftime('%Y-%m-%d_%H-%M-%S')
            tool_output_filename = f"{uuid.uuid4().hex}_NetMHCPan_results_{time_str}.xlsx"
            minio_excel_path = upload_file_to_minio(str(merged_excel), MINIO_BUCKET, tool_output_filename)
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

            return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetMHCPan多肽长并行处理完成，结果已合并。"}, ensure_ascii=False)
        else:
            # 3. 其它情况，原有分片并发逻辑
            # 1. 下载minio文件到本地（如有需要）
            if isinstance(input_fasta, str) and input_fasta.startswith("minio://"):
                input_fasta = download_from_minio_uri(input_fasta, INPUT_TMP_DIR)
            # 2. 切割一次fasta
            split_dir = Path(output_dir) / f"split_{uuid.uuid4().hex}"
            split_dir.mkdir(parents=True, exist_ok=True)
            sub_fastas = split_fasta(input_fasta, num_workers, str(split_dir))
            # 4. 针对每个肽长并发run_netmhcpan_parallel，传入同一批分片
            try:
                tasks = [
                run_netmhcpan_parallel(
                    non_empty_fastas[i], mhc_allele, non_empty_lengths[i], high_threshold_of_bp, low_threshold_of_bp,
                    rank_cutoff,  non_empty_workers[i], netmhcpan_dir, output_dir,sub_fastas=sub_fastas
                    )
                    for i, l in enumerate(lengths)
                ]
                excel_files = await asyncio.gather(*tasks, return_exceptions=True)
                for i, res in enumerate(excel_files):
                    if isinstance(res, Exception):
                        print(f"[ERROR] 子任务{i} 执行异常: {res}")
                        traceback.print_exception(type(res), res, res.__traceback__)
                # 过滤掉异常和无效文件
                valid_excels = [f for f in excel_files if isinstance(f, str) and Path(f).exists()]
                if not valid_excels:
                    print("[ERROR] 没有生成任何有效的Excel文件，无法合并！")
                    raise RuntimeError("没有生成任何有效的Excel文件，无法合并！")
                # 5. 合并所有excel
                merged_excel = Path(output_dir) / f"merged_multi_{uuid.uuid4().hex}_NetMHCPan_results.xlsx"
                merge_excels(valid_excels, str(merged_excel))
                # 6. 上传合并后的Excel到MinIO
                beijing_time = datetime.now(ZoneInfo("Asia/Shanghai"))
                time_str = beijing_time.strftime('%Y-%m-%d_%H-%M-%S')
                tool_output_filename = f"{uuid.uuid4().hex}_NetMHCPan_results_{time_str}.xlsx"
                minio_excel_path = upload_file_to_minio(str(merged_excel), MINIO_BUCKET, tool_output_filename)
            except Exception as e:
                print(f"[ERROR] run_netmhcpan_multi_length 分片并发/合并/上传异常: {e}")
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
 
            return json.dumps({"type": "link", "url": minio_excel_path, "content": "NetMHCPan多肽长并行处理完成，结果已合并。"}, ensure_ascii=False)
    except Exception as e:
        print(f"[ERROR] run_netmhcpan_multi_length 执行异常: {e}")
        traceback.print_exc()
        raise

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

# # 新主入口，支持并发
# async def NetMHCPan(
#     input_filename: str,
#     mhc_allele: str = "HLA-A02:01",
#     peptide_length: str = "-1",
#     high_threshold_of_bp: float = 0.5,
#     low_threshold_of_bp: float = 2.0,
#     rank_cutoff: float = -99.9,
#     num_workers: int = 1
# ) -> str:
#     try:
#         return await run_netmhcpan_multi_length(
#             input_filename, mhc_allele, peptide_length, high_threshold_of_bp, low_threshold_of_bp, rank_cutoff, num_workers
#         )
#     except Exception as e:
#         result = {
#             "type": "text",
#             "content": f"调用NetMHCPan工具失败: {e}"
#         }
#         return json.dumps(result, ensure_ascii=False)
    
    
