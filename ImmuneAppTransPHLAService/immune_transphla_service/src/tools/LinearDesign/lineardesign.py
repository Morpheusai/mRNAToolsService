import asyncio
import json
import os
import subprocess
import sys
import uuid

from pathlib import Path

# 当前脚本路径
current_file = Path(__file__).resolve()
project_root = current_file.parents[5]
sys.path.append(str(project_root))
from src.utils.log import logger
from config import CONFIG_YAML
from src.utils.minio_utils import upload_file_to_minio,download_from_minio_uri

# 读取 config 中的配置
MINIO_CONFIG = CONFIG_YAML["MINIO"]
MINIO_BUCKET = MINIO_CONFIG["lineardesign_bucket"]

# 配置路径
linear_design_script = CONFIG_YAML["TOOL"]["LINEARDESIGN"]["script"]
input_dir = CONFIG_YAML["TOOL"]["LINEARDESIGN"]["input_tmp_dir"]
output_dir = CONFIG_YAML["TOOL"]["LINEARDESIGN"]["output_tmp_dir"]
linear_design_dir = Path(linear_design_script).parents[0]



async def run_lineardesign(minio_input_fasta: str, lambda_val: float = 1.0) -> str:
    try:
        
        output_uuid = str(uuid.uuid4())[:8]
        output_filename = f"{output_uuid}_lineardesign_result.fasta"
        local_output = Path(output_dir) / output_filename
        local_output.parent.mkdir(parents=True, exist_ok=True)
        local_input = download_from_minio_uri(minio_input_fasta, input_dir)
        # 构建命令
        command = [
            "python", str(linear_design_script),
            "-o", str(local_output),
            "-l", str(lambda_val),
            "-f", str(local_input)
        ]
        #查看命令
        # print(f"Running command: {' '.join(command)}")
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=linear_design_dir  
        )

        # 等待执行结束，收集输出
        stdout, stderr = await process.communicate()
        # print(f"STDOUT: {stdout.decode()}")
        # print(f"STDERR: {stderr.decode()}")
        # exit()
        # 错误处理
        if process.returncode != 0:
            error_message = (
                f"LinearDesign exited with code {process.returncode}\n"
                f"--- stdout ---\n{stdout.decode()}\n"
                f"--- stderr ---\n{stderr.decode()}"
            )
            raise RuntimeError(error_message)
        minio_object_name = f"{uuid.uuid4().hex}_lineardesign_result.fasta"
        # 上传结果到 MinIO
        minio_output_path = upload_file_to_minio(str(local_output),MINIO_BUCKET,minio_object_name)
        
        if minio_input_fasta:
            os.remove(local_input)
        os.remove(local_output)
        
        return json.dumps({
            "type": "link",
            "url": minio_output_path,
            "content": f"LinearDesign 运行完成，请下载结果查看"
        }, ensure_ascii=False)

    except Exception as e:
        return json.dumps({
            "type": "text",
            "content": f"LinearDesign 调用失败：{e}"
        }, ensure_ascii=False)


async def LinearDesign(minio_input_fasta: str , lambda_val: float = 0.5) -> str:
    """
    使用 LinearDesign 工具对给定的肽段或 FASTA 文件进行 mRNA 序列优化。

    参数：
        minio_input_fasta: MinIO 中的输入文件路径（例如 minio://bucket/input.fasta）
        lambda_val: lambda 参数控制表达/结构平衡，默认 0.5

    返回：
        包含 MinIO 链接的 JSON 字符串
    """
    try:
        return await run_lineardesign(minio_input_fasta, lambda_val)

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用LinearDesign工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)


if __name__ == "__main__":
    print(asyncio.run(
        run_lineardesign(
            minio_input_fasta="minio://extract-peptide-results/488eaf064dc74baea195075551388008_peptide_sequence.fasta",
            lambda_val= 0.5)))
