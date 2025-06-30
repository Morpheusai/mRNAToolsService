import json

from src.protocols import (
    NetChopRequest, 
    NetMHCPanRequest, 
    NetCTLPanRequest, 
    NetMHCStabPanRequest, 
    NetTCRRequest,
    BigMHCRequest,
    PrimeRequest,
    RNAPlotRequest,
    RNAFoldRequest
)

from src.tools.NetChop.netchop import run_netchop
from src.tools.NetMHCPan.netmhcpan import run_netmhcpan
from src.tools.NetCTLPan.netctlpan import run_netctlpan
from src.tools.NetMHCStabPan.netmhcstabpan import run_netmhcstabpan
from src.tools.NetTCR.nettcr import run_nettcr
from src.tools.BigMHC.bigmhc import run_bigmhc
from src.tools.Prime.prime import run_prime
from src.tools.RNAPlot.rnaplot import run_rnaplot
from pmhc.src.tools.RNAFold.rnafold import run_rnafold

async def netchop(request: NetChopRequest) -> str:
    """                                    
    NetChops是一种用于预测蛋白质序列中蛋白酶体切割位点的生物信息学工具。
    Args:                                  
        input_filename (str): 输入的肽段序例fasta文件路径           
        cleavage_site_threshold (float): 设定切割位点的置信度阈值（范围：0.0 ~ 1.0）。
        model (int): 预测模型版本，0-Cterm3.0，1-20S-3.0，默认值0
        format (int): 输出格式，0-长格式，1-短格式，默认值0
        strict (int): 严格模式，0-开启严格模式，1-关闭严格模式，默认值0
    Returns:                               
        str: 返回高结合亲和力的肽段序例信息                                                                                                                           
    """
    input_filename = request.input_filename
    cleavage_site_threshold = request.cleavage_site_threshold
    model = request.model
    format = request.format
    strict = request.strict
    try:
        return await run_netchop(
            input_filename,
            cleavage_site_threshold,
            model,
            format,
            strict
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用NetChop工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)

async def netMHCpan(request: NetMHCPanRequest) -> str:
    """
    NetMHCPan用于预测肽段序列和给定MHC分子的结合能力，可高效筛选高亲和力、稳定呈递的候选肽段，用于mRNA 疫苗及个性化免疫治疗。
    Args:
        input_filename: 输入的肽段序例fasta文件路径
        mhc_allele: HLA 等位基因（MHC 分子类型）
        peptide_length: 肽段长度，范围8-11，-1表示使用默认值
        high_threshold_of_bp: 高结合力肽段的阈值
        low_threshold_of_bp: 低结合力肽段的阈值
        rank_cutoff: 输出结果的%Rank截断值
    Returns:
        str: 返回高结合亲和力的肽段序例信息
    """
    input_filename = request.input_filename
    mhc_allele = request.mhc_allele
    high_threshold_of_bp = request.high_threshold_of_bp
    low_threshold_of_bp = request.low_threshold_of_bp
    peptide_length = request.peptide_length
    rank_cutoff = request.rank_cutoff
    try:
        return await run_netmhcpan(
            input_filename,
            mhc_allele,
            peptide_length,
            high_threshold_of_bp,
            low_threshold_of_bp,
            rank_cutoff
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用NetMHCPan工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)

async def netCTLpan(request: NetCTLPanRequest) -> str:
    """
    使用NetCTLPan工具预测肽段序列与指定MHC分子的结合亲和力，用于筛选潜在的免疫原性肽段。
    该函数结合蛋白质裂解、TAP转运和MHC结合的预测，适用于疫苗设计和免疫研究。

    :param input_filename: 输入的FASTA格式肽段序列文件路径
    :param mhc_allele: 用于比对的MHC等位基因名称，默认为"HLA-A02:01"
    :param peptide_length: 肽段长度，范围8-11，-1表示不加-l参数
    :param weight_of_tap: TAP转运效率预测的权重，默认为0.025
    :param weight_of_clevage: 蛋白质裂解预测的权重，默认为0.225
    :param epi_threshold: 表位阈值，默认1.0
    :param output_threshold: 输出得分阈值，默认-99.9
    :param sort_by: 排序方式，默认-1
    :return: 返回预测结果字符串，包含高亲和力肽段信息
    """
    input_filename = request.input_filename
    mhc_allele = request.mhc_allele
    weight_of_clevage = request.weight_of_clevage
    weight_of_tap = request.weight_of_tap 
    peptide_length = request.peptide_length
    epi_threshold = request.epi_threshold
    output_threshold = request.output_threshold
    sort_by = request.sort_by
    try:
        # 调用异步函数并获取返回结果
        result = await run_netctlpan(
            input_filename, 
            mhc_allele, 
            peptide_length,
            weight_of_tap, 
            weight_of_clevage,
            epi_threshold,
            output_threshold,
            sort_by
        )
        return result
    except Exception as e:
        # 捕获并返回异常信息
        result = {
            "type": "text",
            "content": f"调用NetCTLPan工具失败: {str(e)}"
        }
        return json.dumps(result, ensure_ascii=False)

async def netMHCstabpan(request: NetMHCStabPanRequest) -> str:
    """                                    
    NetMHCStabPan用于预测肽段与MHC结合后复合物的稳定性，可用于优化疫苗设计和免疫治疗。
    Args:
        input_file (str): 输入的肽段序列fasta文件路径 
        mhc_allele (str): MHC比对的等位基因
        peptide_length (str): 预测时所使用的肽段长度            
        high_threshold_of_bp (float): 肽段和MHC分子高结合能力的阈值
        low_threshold_of_bp (float): 肽段和MHC分子弱结合能力的阈值
    Returns:
        str: 返回高稳定性的肽段序列信息                                                                                                                           
    """
    input_file = request.input_file
    mhc_allele = request.mhc_allele
    high_threshold_of_bp = request.high_threshold_of_bp
    low_threshold_of_bp = request.low_threshold_of_bp
    peptide_length = request.peptide_length
    try:
        return await run_netmhcstabpan(
            input_file,
            mhc_allele,
            high_threshold_of_bp,
            low_threshold_of_bp,
            peptide_length
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用NetMHCStabPan工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)

async def netTCR(request: NetTCRRequest) -> str:
    """                                    
    NetTCR用于预测肽段（peptide）与 T 细胞受体（TCR）的相互作用。
    Args:                                  
        input_file (str): 输入文件的路径，文件需包含待预测的肽段和 TCR 序列。
    Returns:                               
        str: 返回高结合亲和力的肽段序例信息                                                                                                                           
    """
    input_file = request.input_file
    try:
        return await run_nettcr(
            input_file
        )

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用NetTCR工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)


async def bigMHC(request: BigMHCRequest) -> str:
    """                                    
    BigMHC是基于深度学习的 MHC-I 抗原呈递（BigMHC EL）和免疫原性（BigMHC IM）预测工具。
    Args:                                  
        input_filename (str): 输入文件的路径，文件需包含待预测的肽段序列。
        mhc_allele (str): MHC-I 等位基因列表，用逗号分隔，如"HLA-A02:01,HLA-A01:01"。
        model_type (str): 模型类型："el"（抗原呈递）或 "im"（免疫原性），默认为el。
    Returns:                               
        str: 返回高结合亲和力的肽段序例信息                                                                                                                           
    """
    input_filename = request.input_filename
    mhc_allele = request.mhc_allele
    model_type = request.model_type
    try:
        return await run_bigmhc(
            input_filename,
            mhc_allele,
            model_type
        )

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用BigMHC工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)


async def prime(request: PrimeRequest) -> str:
    """                                    
    Prime 是一款用于预测 I 类免疫原性表位 的计算工具，通过结合 MHC-I 分子结合亲和力（基于 MixMHCpred）和 TCR 识别倾向，帮助研究人员筛选潜在的 CD8+ T 细胞表位，适用于疫苗开发和免疫治疗研究。
    Args:                                  
        input_file (str): 输入的肽段序例fasta文件路径           
        mhc_allele (str): MHC-I 等位基因列表，用逗号分隔,如"A0101,A2501,B0801,B1801"。
    Returns:                               
        str: 返回高结合亲和力的肽段序例信息                                                                                                                           
    """
    input_file = request.input_file
    mhc_allele = request.mhc_allele
    try:
        return await run_prime(
            input_file,mhc_allele
        )

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用Prime工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
    
async def rnaPlot(request: RNAPlotRequest) -> str:
    """                                    
    RNAPlot是用来绘制 RNA 的二级结构图。
    Args:                                  
        input_file (str): 输入的肽段序例fasta文件路径           
    Returns:                               
        str: 返回一个RNA的二级结构的矢量图的存放路径                                                                                                            
    """
    input_file = request.input_file
    try:
        return await run_rnaplot(
            input_file
        )

    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用RNAPlot工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)    
    
async def rnaFold(request: RNAFoldRequest) -> str:
    """                                    
    RNAFold是预测其最小自由能（MFE）二级结构，输出括号表示法和自由能值。
    Args:                                  
        input_file (str): 输入的肽段序例fasta文件路径           
    Returns:                               
        str: 返回输出括号表示法和自由能值字符串。                                                                                                                          
    """
    input_file = request.input_file
    try:
        return await run_rnafold(
            input_file
        )

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        result = {
            "type": "text",
            "content": f"调用RNAFold工具失败: {error_trace}"
        }
        return json.dumps(result, ensure_ascii=False)