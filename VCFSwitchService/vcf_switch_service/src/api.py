import json

from src.protocols import (
    VcfSwitchRequest,
    VcfSwitchResponse
)
from src.tools.VCFSwitch.vcfswitch import run_vcfswitch


async def vcfswitch(request: VcfSwitchRequest) -> VcfSwitchResponse:
    """                                    
    VcfSwitch是一种用于从正常和异常VCF文件中的提取突变肽段的工具。
    Args:                                  
        normal_file (str): 输入的正常样本VCF文件路径           
        tumor_file (str): 输入的肿瘤样本VCF文件路径
    Returns:                               
        str: 返回minio路径                                                                                                                          
    """

    try:
        return await run_vcfswitch(
            request.normal_file,
            request.tumor_file
        )
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()

        return VcfSwitchResponse(type="text",
                                 content=f"调用VcfSwitch工具失败: {str(e)}\n\n完整异常堆栈:\n{error_trace}"
                                 )
