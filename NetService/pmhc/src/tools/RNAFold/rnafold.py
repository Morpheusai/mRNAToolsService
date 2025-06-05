import asyncio
import json
import os
import sys
import uuid

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from pathlib import Path

from config import CONFIG_YAML
from src.tools.RNAFold.filter_rnafold import filter_rnafold_excel
from src.tools.RNAFold.rnafold_to_excel import save_excel
from src.tools.RNAPlot.rnaplot import RNAPlot
from src.utils.log import logger

load_dotenv()



# MinIO 配置:
MINIO_CONFIG = CONFIG_YAML["MINIO"]
MINIO_ENDPOINT = MINIO_CONFIG["endpoint"]
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = MINIO_CONFIG["rnafold_bucket"]
MINIO_SECURE = MINIO_CONFIG.get("secure", False)

# netMHCpan 配置 
INPUT_TMP_DIR = CONFIG_YAML["TOOL"]["RNAFOLD"]["input_tmp_dir"]
DOWNLOADER_PREFIX = CONFIG_YAML["TOOL"]["COMMON"]["output_download_url_prefix"]
OUTPUT_TMP_DIR = CONFIG_YAML["TOOL"]["RNAFOLD"]["output_tmp_dir"]

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


async def run_rnafold(
    input_file: str,  # MinIO 文件路径，格式为 "bucket-name/file-path"
    ) -> str:

    """
    异步运行 RNAFlod 并将处理后的结果上传到 MinIO
    :param input_file: MinIO 文件路径，格式为 "bucket-name/file-path"

    """

    minio_available = check_minio_connection()
    logger.info(f"开始处理RNAFold任务，输入文件: {input_file}")
    #提取桶名和文件
    try:
        # 去掉 minio:// 前缀
        path_without_prefix = input_file[len("minio://"):]
        
        # 找到第一个斜杠的位置，用于分割 bucket_name 和 object_name
        first_slash_index = path_without_prefix.find("/")
        
        if first_slash_index == -1:
            error_msg = f"无效的文件路径格式: {input_file}"
            logger.error(error_msg)
            raise ValueError("Invalid file path format: missing bucket name or object name")
        
        # 提取 bucket_name 和 object_name
        bucket_name = path_without_prefix[:first_slash_index]
        object_name = path_without_prefix[first_slash_index + 1:]
        
        # 打印提取结果（可选）
        # logger.info(f"Extracted bucket_name: {bucket_name}, object_name: {object_name}")
        
    except Exception as e:
        logger.error(f"Failed to parse file_path: {file_path}, error: {str(e)}")
        raise str(status_code=400, detail=f"Failed to parse file path: {str(e)}")     

    try:
        response = minio_client.get_object(bucket_name, object_name)
        file_content = response.read().decode("utf-8")
    except S3Error as e:
        error_msg = f"无法从MinIO读取文件: {str(e)}"
        logger.error(error_msg)        
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
    input_path = input_dir / f"{random_id}.fasta"
    with open(input_path, "w") as f:
        f.write(file_content)

    # 构建输出文件名和临时路径
    output_filename = f"{random_id}_RNAFold_results.xlsx"
    output_path = output_dir / output_filename
    #存放输出结果的fasta的临时文件，用于给rnaplot的输入
    output_file = f"{random_id}_out.fasta"
    output_out_fsata = output_dir / output_file

    # 构建命令
    cmd = [
        "RNAfold",
        "-i", str(input_path),
        "--noPS"
    ]

    # cmd2 = [
    #     "RNAfold",
    #     "-i", str(input_path),  # 输入文件
    #     "--auto-id",
    #     "--noPS"
    # ]

    # 启动异步进程
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/opt/softwares/ViennaRNA"
    )

    # 处理输出
    stdout, stderr = await proc.communicate()
    output = stdout.decode()
    
    # 错误处理
    if proc.returncode != 0:
        error_msg = f"RNAfold执行失败 | 退出码: {proc.returncode} | 错误: {stderr.decode()}"
        logger.error(error_msg)
        input_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        output_out_fsata.unlink(missing_ok=True)
        result = {
            "type": "text",
            "content": "您的输入信息可能有误，请核对正确再试。"
        }
        return json.dumps(result, ensure_ascii=False)
    logger.info("RNAfold执行成功，正在保存结果...")
    save_excel(output, output_dir, output_filename)
    filtered_content = filter_rnafold_excel(output_path)    

    # 解析RNAfold输出
    lines = output.split('\n')
    records = []
    current_record = []
    
    # 第一步：按记录分割
    for line in lines:
        if line.startswith('>'):
            if current_record:  # 保存前一个记录
                records.append('\n'.join(current_record))
                current_record = []
            current_record.append(line)
        else:
            if current_record:  # 只有已经开始收集记录时才添加行
                current_record.append(line)
    
    # 添加最后一个记录
    if current_record:
        records.append('\n'.join(current_record))
    
    results = []
    
    for record in records:
        try:
            record_lines = record.strip().split('\n')
            
            # 解析肽段信息行
            header = record_lines[0][1:]  # 去掉开头的>
            
            # 解析肽段序列（第二行）
            sequence = record_lines[1] if len(record_lines) > 1 else ""
            
            # 解析结构行（第三行），并去除能量值部分
            if len(record_lines) > 2:
                structure_line = record_lines[2]
                # 提取结构部分（去除最后的能量值）
                structure = structure_line.split(' ')[0].strip()
            else:
                structure = ""
            
            results.append({
                "structure": structure,
                "sequence": sequence
            })
            
        except Exception as e:
            print(f"解析记录时出错，跳过该记录。错误: {str(e)}")
            print(f"问题记录内容: {record[:100]}...")

    logger.info(f"results的结果：...................{results}")
    # 上传JSON数据到MinIO
    uploaded_urls = {}
    
    for i, result in enumerate(results, 1):
        # 创建JSON内容
        json_content = {
            "structure": result['structure'],
            "sequence": result['sequence'] 
        }
        
        # 转换为JSON字符串
        json_str = json.dumps(json_content, ensure_ascii=False)
        
        # 创建文件名
        json_filename = f"{random_id}_part{i}.txt"
        
        try:
            if minio_available:
                # 直接上传字符串数据到MinIO
                minio_client.put_object(
                    MINIO_BUCKET,
                    json_filename,
                    BytesIO(json_str.encode('utf-8')),
                    len(json_str.encode('utf-8')),
                    content_type='application/json'
                )
                file_path = f"minio://{MINIO_BUCKET}/{json_filename}"
                uploaded_urls[f"fasta_path{i}"] = file_path
            else:
                logger.warning("MinIO不可用，返回本地下载链接")
                file_path = f"{DOWNLOADER_PREFIX}{json_filename}"
                uploaded_urls[f"fasta_path{i}"] = file_path
        except S3Error as e:
            logger.error(f"MinIO上传失败: {e}")
            file_path = f"{DOWNLOADER_PREFIX}{json_filename}"
            uploaded_urls[f"fasta_path{i}"] = file_path
    
    # 根据上传结果构建最终返回结构
    if len(uploaded_urls) == 1:
        # 只有一个结果文件，返回字符串形式的URL
        final_url = list(uploaded_urls.values())[0]
    else:
        # 多个结果文件，返回字典形式的URL
        final_url = uploaded_urls
    
    # 构建最终返回结果
    final_result = {"url": final_url}



    # proc2 = await asyncio.create_subprocess_exec(
    #     *cmd2,
    #     stdout=asyncio.subprocess.PIPE,  # 捕获标准输出
    #     stderr=asyncio.subprocess.PIPE,
    #     cwd="/opt/softwares/ViennaRNA"
    # )
    # stdout2, stderr2 = await proc2.communicate()
    
    # if proc2.returncode != 0:
    #     error_msg = f"RNAfold执行失败 | 退出码: {proc.returncode} | 错误: {stderr.decode()}"
    #     logger.error(error_msg)
    #     input_path.unlink(missing_ok=True)
    #     output_path.unlink(missing_ok=True)
    #     output_out_fsata.unlink(missing_ok=True)
    #     result = {
    #         "type": "text",
    #         "content": "您的输入信息可能有误，请核对正确再试。"
    #     }    
    #     return json.dumps(result, ensure_ascii=False)
    
    # with open(output_out_fsata, "w") as f:
    #     f.write(stdout2.decode())
    # rnaplot_result = await RNAPlot(str(output_out_fsata))
    # rnaplot_data = json.loads(rnaplot_result)

    

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
            logger.warning("MinIO不可用，返回本地下载链接")
            file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
    except S3Error as e:
        file_path = f"{DOWNLOADER_PREFIX}{output_filename}"
    finally:
        # 如果 MinIO 成功上传，清理临时文件；否则保留
        if minio_available:
            input_path.unlink(missing_ok=True)
            output_path.unlink(missing_ok=True)
            output_out_fsata.unlink(missing_ok=True)
            logger.info("临时文件已清理")
        else:
            input_path.unlink(missing_ok=True)  # 只删除输入文件，保留输出文件
            output_out_fsata.unlink(missing_ok=True)

    # 处理 RNAPlot 返回的 URL
    if isinstance(final_result.get("url"), dict):
        # 情况1：RNAPlot返回的是字典（多个URL），合并成一个字典
        merged_urls = {"rnaflod_result_file_url": file_path}  # 原始 file_path
        merged_urls.update(final_result["url"])  # 合并 RNAPlot 的所有 URL
        file_path = merged_urls
    elif isinstance(final_result.get("url"), str):
        # 情况2：RNAPlot返回的是字符串（单个URL），构造字典
        file_path = {
            "original_result_file_url": file_path,
            "rnaplot_result_file_url": final_result["url"]
        }
    else:
        # 其他情况保持原样
        pass

    # 返回结果
    result = {
        "type": "link",
        "url": file_path,
        "content": filtered_content  # 替换为生成的 Markdown 内容
    }

    return json.dumps(result, ensure_ascii=False)

async def RNAFold(input_file: str) -> str:
    """                                    
    RNAFold是预测其最小自由能（MFE）二级结构，输出括号表示法和自由能值。
    Args:                                  
        input_file (str): 输入的肽段序例fasta文件路径           
    Returns:                               
        str: 返回输出括号表示法和自由能值字符串。                                                                                                                          
    """
    try:
        return await run_rnafold(input_file)

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用RNAFold工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
    
# if __name__ == "__main__":
#     input_file = "minio://molly/ab58067f-162f-49af-9d42-a61c30d227df_test_netchop.fsa"
    
#     # 最佳调用方式
#     tool_result = RNAFold.ainvoke({
#         "input_file": input_file,
#     })
#     print("工具结果:", tool_result)