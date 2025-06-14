import json

from src.protocols import UniPMT

from src.tools.UniPMT.unipmt import run_unipmt



async def unipmt(request: UniPMT) -> str:
    """
    使用 unipmt 工具来完成三元复合体结构建模

    参数：
        minio_input_fasta: MinIO 中的输入文件路径（例如 minio://bucket/input.fasta）
        lambda_val: lambda 参数控制表达/结构平衡，默认 0.5

    返回：
        包含 MinIO 链接的 JSON 字符串
    """
    minio_input_fasta = request.minio_input_fasta
    lambda_val = request.lambda_val
    try:
        return await run_unipmt(
            minio_input_fasta,
            lambda_val,
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用LinearDesign工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)