import asyncio
import json
import os
import sys
import uuid

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error 
from pathlib import Path

from src.tools.NetMHCPan.filter_netmhcpan import filter_netmhcpan_excel
from src.tools.NetMHCPan.netmhcpan_to_excel import save_excel

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

# 初始化 MinIO 客户端
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)
#检查minio是否可用
def check_minio_connection(bucket_name=MINIO_BUCKET):
    try:
        minio_client.list_buckets()
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        return True
    except S3Error as e:
        print(f"MinIO连接或bucket操作失败: {e}")
        return False


async def run_netmhcpan(
    input_filename: str,  # MinIO 文件路径，格式为 "bucket-name/file-path"
    mhc_allele: str = "HLA-A02:01",  # HLA 等位基因（MHC 分子类型）
    peptide_length: int = -1,  # 肽段长度，范围8-11，-1表示使用默认值
    high_threshold_of_bp: float = 0.5,  # 高结合力肽段的阈值
    low_threshold_of_bp: float = 2.0,  # 低结合力肽段的阈值
    rank_cutoff: float = -99.9,  # 输出结果的%Rank截断值
    netmhcpan_dir: str = NETMHCPAN_DIR
    ) -> str:

    """
    异步运行 netMHCpan 并将处理后的结果上传到 MinIO
    :param input_filename: MinIO 文件路径，格式为 "bucket-name/file-path"
    :param mhc_allele: HLA 等位基因（MHC 分子类型），默认值HLA-A02:01
    :param peptide_length: 肽段长度，范围8-11，-1表示使用默认值，默认值-1
    :param high_threshold_of_bp: 高结合力肽段的阈值，默认值0.5
    :param low_threshold_of_bp: 低结合力肽段的阈值，默认值2.0
    :param rank_cutoff: 输出结果的%Rank截断值，默认值-99.9
    :param netmhcpan_dir: netMHCpan 安装目录
    :return: JSON 字符串，包含 MinIO 文件路径（或下载链接）
    """

    minio_available = check_minio_connection()
    #提取桶名和文件
    try:
        # 去掉 minio:// 前缀
        path_without_prefix = input_filename[len("minio://"):]
        
        # 找到第一个斜杠的位置，用于分割 bucket_name 和 object_name
        first_slash_index = path_without_prefix.find("/")
        
        if first_slash_index == -1:
            raise ValueError("Invalid file path format: missing bucket name or object name")
        
        # 提取 bucket_name 和 object_name
        bucket_name = path_without_prefix[:first_slash_index]
        object_name = path_without_prefix[first_slash_index + 1:]
        
        # 打印提取结果（可选）
        # logger.info(f"Extracted bucket_name: {bucket_name}, object_name: {object_name}")
        
    except Exception as e:
        # logger.error(f"Failed to parse file_path: {file_path}, error: {str(e)}")
        raise str(status_code=400, detail=f"Failed to parse file path: {str(e)}")     

    try:
        response = minio_client.get_object(bucket_name, object_name)
        file_content = response.read().decode("utf-8")
    except S3Error as e:
        return json.dumps({
            "type": "text",
            "content": f"无法从 MinIO 读取文件: {str(e)}"
        }, ensure_ascii=False)    

    # 生成随机ID和文件路径
    random_id = uuid.uuid4().hex
    #base_path = Path(__file__).resolve().parents[3]  # 根据文件位置调整层级
    input_dir = Path(INPUT_TMP_DIR)
    output_dir =Path(OUTPUT_TMP_DIR)
    

    # 创建目录
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 写入输入文件
    input_path = input_dir / f"{random_id}.fsa"
    with open(input_path, "w") as f:
        f.write(file_content)

    # 构建输出文件名和临时路径
    output_filename = f"{random_id}_NetMHCpan_results.xlsx"
    output_path = output_dir / output_filename

    # 构建命令
    cmd = [
        f"{netmhcpan_dir}/netMHCpan",
        "-BA",
        "-a", mhc_allele,  # HLA 等位基因
        "-rth", str(high_threshold_of_bp),  # 高结合力肽段阈值
        "-rlt", str(low_threshold_of_bp),  # 低结合力肽段阈值
        "-t", str(rank_cutoff),  # 输出结果%Rank截断值
        str(input_path)  # 输入文件路径
    ]
    
    # 只有当peptide_length不为-1时才添加-l参数
    if peptide_length != -1:
        cmd.insert(-1, "-l")  # 在输入文件路径前插入-l
        cmd.insert(-1, str(peptide_length))  # 在-l后插入peptide_length值

    # 过滤掉空字符串
    cmd = [arg for arg in cmd if arg]

    # 启动异步进程
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=f"{netmhcpan_dir}"
    )

    # 处理输出
    stdout, stderr = await proc.communicate()
    output = stdout.decode()
    # stdout_text = stdout.decode()
    # stderr_text = stderr.decode()
    # print(f"stdout:{stdout_text}")
    # print(f"stderr:{stderr_text}")
    # exit()
    # print(output)
    save_excel(output,output_dir,output_filename)

    # # 直接将所有内容写入文件
    # with open(output_path, "w") as f:
    #     f.write("\n".join(output.splitlines()))
       
    # 调用过滤函数
    filtered_content = filter_netmhcpan_excel(output_path)
    
    # 错误处理
    if proc.returncode != 0:
        error_msg = stderr.decode()
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        result = {
            "type": "text",
            "content": "您的输入信息可能有误，请核对正确再试。"
        }
    else:
        try:
            if minio_available:
                minio_client.fput_object(
                    MINIO_BUCKET,
                    output_filename,
                    str(output_path)
                )
                file_path = f"minio://{MINIO_BUCKET}/{output_filename}"
            else:
                # 如果 MinIO 不可用，返回下载链接
                file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
        except S3Error as e:
            file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
        finally:
            # 如果 MinIO 成功上传，清理临时文件；否则保留
            if minio_available:
                input_path.unlink(missing_ok=True)
                output_path.unlink(missing_ok=True)
            else:
                input_path.unlink(missing_ok=True)  # 只删除输入文件，保留输出文件

        # 返回结果
        result = {
            "type": "link",
            "url": file_path,
            "content": filtered_content  # 替换为生成的 Markdown 内容
        }

    return json.dumps(result, ensure_ascii=False)


async def NetMHCpan(
    input_filename: str,
    mhc_allele: str = "HLA-A02:01",
    peptide_length: int = -1,
    high_threshold_of_bp: float = 0.5,
    low_threshold_of_bp: float = 2.0,
    rank_cutoff: float = -99.9
) -> str:
    """
    NetMHCpan用于预测肽段序列和给定MHC分子的结合能力，可高效筛选高亲和力、稳定呈递的候选肽段，用于mRNA 疫苗及个性化免疫治疗。
    Args:
        input_filename (str): 输入的肽段序例fasta文件路径
        mhc_allele (str): HLA 等位基因（MHC 分子类型），默认值HLA-A02:01
        peptide_length (int): 肽段长度，范围8-11，-1表示使用默认值，默认值-1
        high_threshold_of_bp (float): 高结合力肽段的阈值，默认值0.5
        low_threshold_of_bp (float): 低结合力肽段的阈值，默认值2.0
        rank_cutoff (float): 输出结果的%Rank截断值，默认值-99.9
    Returns:
        str: 返回高结合亲和力的肽段序例信息
    """
    try:
        return await run_netmhcpan(input_filename, mhc_allele, peptide_length, high_threshold_of_bp, low_threshold_of_bp, rank_cutoff)
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用NetMHCpan工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
    
    
