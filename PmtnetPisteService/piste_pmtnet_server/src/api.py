import json
import traceback

from src.protocols import (
    PisteRequest,
    PMTNetRequest,
)

from src.tools.PMTNet.pMTnet import run_pMTnet
from src.tools.Piste.piste import run_PISTE

async def piste(request: PisteRequest) -> str:
    """
    Run the PISTE tool on a given input file and return results.

    Args:
        input_file_dir (str): MinIO路径，例如 minio://bucket/file.csv
        model_name (str, optional): 使用的模型名，如 random、unipep、reftcr。
        threshold (float, optional): binder判定阈值（0-1）。
        antigen_type (str, optional): 抗原类型，MT 或 WT。

    Returns:
        str: JSON格式的预测结果
    """
    input_file_dir_minio = request.input_file_dir_minio
    model_name = request.model_name
    threshold = request.threshold
    antigen_type = request.antigen_type
    try:
        return await run_PISTE(
            input_file_dir_minio,model_name,threshold,antigen_type
        )

    except Exception as e:
        # result = {
        #     "type": "text",
        #     "content": f"调用Piste工具失败: {e}"
        # }
        # return json.dumps(result, ensure_ascii=False)
        result = {
        "type": "text",
        "content": f"调用Piste工具失败: {str(e)}\n详细错误:\n{traceback.format_exc()}"
    }
    return json.dumps(result, ensure_ascii=False)

async def pmtnet(request: PMTNetRequest) -> str:
    """
     Run the pMTnet tool on a given input file directory and return the results.
     Args:
         input_file_dir (str): The path to the input file directory.
     Returns:
         str: The JSON-formatted results of the pMTnet analysis.
    """
    input_file_dir_minio = request.input_file_dir_minio
    try:
        return await run_pMTnet(
            input_file_dir_minio
        )

    except Exception as e:
        # result = {
        #     "type": "text",
        #     "content": f"调用PMTNet工具失败: {e}"
        # }
        # return json.dumps(result, ensure_ascii=False)        
        result = {
        "type": "text",
        "content": f"调用PMTnet工具失败: {str(e)}\n详细错误:\n{traceback.format_exc()}"
    }
    return json.dumps(result, ensure_ascii=False)