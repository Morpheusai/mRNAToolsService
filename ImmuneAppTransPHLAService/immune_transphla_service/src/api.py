import json

from src.protocols import ImmuneAppRequest, ImmuneNeoRequest, TransphlaRequest,LinearDesign

from src.tools.ImmuneApp.immuneapp import run_ImmuneApp
from src.tools.ImmuneAppNeo.immuneapp_neo import run_ImmuneApp_Neo
from src.tools.TransPHLA.transphla import run_TransPHLA
from src.tools.LinearDesign.lineardesign import run_lineardesign

async def immuneapp(request: ImmuneAppRequest) -> str:
    """
    使用 ImmuneApp 工具预测抗原肽段与 MHC 的结合能力。

    自动识别输入类型：
      - .txt → peplist，不需要 -l
      - .fa/.fas/.fasta → fasta，需要 -l（默认 [9,10]）
    参数：
        input_file_dir (str): MinIO 文件路径，如 minio://bucket/file.fas
        alleles (str): 逗号分隔的等位基因列表
        use_binding_score (bool): 是否启用 -b
        peptide_lengths (list[int]): 仅对 fasta 输入有效
    """
    input_file = request.input_file_dir
    alleles = request.alleles
    use_binding_score = request.use_binding_score
    peptide_lengths = request.peptide_lengths
    try:
        return await run_ImmuneApp(
            input_file,
            alleles,
            use_binding_score,
            peptide_lengths
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用ImmuneApp工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)

async def immuneappneo(request: ImmuneNeoRequest) -> str:
    """
    使用 ImmuneApp-Neo 工具预测 neoepitope 的免疫原性，针对 HLA-I 抗原表位。

    该工具从 MinIO 下载输入文件，运行 ImmuneApp-Neo 脚本，并返回预测结果。
    仅支持 peplist 文件格式（.txt 或 .tsv），包含肽序列列表。

    参数：
        input_file (str): MinIO 文件路径，例如 minio://bucket/file.txt。
        alleles (str): 逗号分隔的 HLA-I 等位基因列表，例如 "HLA-A*01:01,HLA-A*02:01"。

    返回：
        JSON 格式的字符串，包含：
        - 如果脚本输出 MinIO 结果路径：包含结果文件 URL 和解析内容的 "link" 类型响应。
        - 否则：包含执行状态或错误信息的 "text" 类型响应。
    """
    input_file = request.input_file
    alleles = request.alleles
    try:
        return await run_ImmuneApp_Neo(
            input_file,
            alleles
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用ImmuneApp-Neo工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
async def transphla(request: TransphlaRequest) -> str:
    """
    使用 TransPHLA_AOMP 工具预测肽段与 HLA 的结合能力，并自动返回结果文件链接。

    参数说明：
    - peptide_file: MinIO 中的肽段 FASTA 文件路径（如 minio://bucket/peptides.fasta）
    - hla_file: MinIO 中的 HLA FASTA 文件路径（如 minio://bucket/hlas.fasta）
    - threshold: 绑定预测阈值，默认使用 0.5
    - cut_length: 肽段最大切割长度
    - cut_peptide: 是否启用肽段切割处理（True/False）

    返回值：
    - JSON 字符串，包含url和 markdown 格式的输出说明
    """
    peptide_file = request.peptide_file
    hla_file = request.hla_file
    threshold = request.threshold
    cut_length = request.cut_length
    cut_peptide = request.cut_peptide
    try:
        return await run_TransPHLA(
            peptide_file,
            hla_file,
            threshold,
            cut_length,
            cut_peptide
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用TransPHLA工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
    

async def lineardesign(request: LinearDesign) -> str:
    """
    使用 LinearDesign 工具对给定的肽段或 FASTA 文件进行 mRNA 序列优化。

    参数：
        minio_input_fasta: MinIO 中的输入文件路径（例如 minio://bucket/input.fasta）
        lambda_val: lambda 参数控制表达/结构平衡，默认 0.5

    返回：
        包含 MinIO 链接的 JSON 字符串
    """
    minio_input_fasta = request.minio_input_fasta
    lambda_val = request.lambda_val
    try:
        return await run_lineardesign(
            minio_input_fasta,
            lambda_val,
        )
    except Exception as e:
        result = {
            "type": "text",
            "content": f"调用LinearDesign工具失败: {e}"
        }
        return json.dumps(result, ensure_ascii=False)
