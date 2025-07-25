import pandas as pd
from .minio_utils import download_from_minio_uri

def read_excel_from_minio_to_dictlist(minio_uri: str):
    """
    从MinIO下载Excel文件并转为字典列表
    :param minio_uri: MinIO文件地址（如minio://bucket/path/to/file.xlsx）
    :return: List[dict]，每行为一个字典
    """
    # 下载文件到本地
    local_path = download_from_minio_uri(minio_uri)
    # 读取Excel
    df = pd.read_excel(local_path)
    # 转为字典列表
    result = df.to_dict(orient="records")
    return result