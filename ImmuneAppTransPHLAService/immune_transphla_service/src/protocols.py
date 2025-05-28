from typing import Optional,List
from pydantic  import BaseModel

class ImmuneAppRequest(BaseModel):
    input_file_dir: str
    alleles: Optional[str] = "HLA-A*01:01,HLA-A*02:01,HLA-A*03:01,HLA-B*07:02"
    use_binding_score: Optional[bool] = True
    peptide_lengths: Optional[List[int]] = [8,9]

class ImmuneNeoRequest(BaseModel):
    input_file: str
    alleles: Optional[str] = "HLA-A*01:01,HLA-A*02:01,HLA-A*03:01,HLA-B*07:02"
    
class TransphlaRequest(BaseModel):
    peptide_file: str
    hla_file: str
    threshold: Optional[float] = 0.5
    cut_length: Optional[int] = 10
    cut_peptide: Optional[bool] = True
