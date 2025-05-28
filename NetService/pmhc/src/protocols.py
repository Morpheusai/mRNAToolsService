from typing import Optional
from pydantic  import BaseModel

class NetChopRequest(BaseModel):
    input_file: str
    cleavage_site_threshold: Optional[float] = 0.5

class NetCTLPanRequest(BaseModel):
    input_file: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    weight_of_clevage: Optional[float] = 0.225
    weight_of_tap: Optional[float] = 0.025
    peptide_length: Optional[str] = "8,9,10,11"

class NetMHCPanRequest(BaseModel):
    input_file: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    high_threshold_of_bp: Optional[float] = 0.5
    low_threshold_of_bp: Optional[float] = 2.0
    peptide_length: Optional[str] = "8,9,10,11"

class NetMHCStabPanRequest(BaseModel):
    input_file: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    high_threshold_of_bp: Optional[float] = 0.5
    low_threshold_of_bp: Optional[float] = 2.0
    peptide_length: Optional[str] = "8,9,10,11"

class NetTCRRequest(BaseModel):
    input_file: str 

class BigMHCRequest(BaseModel):
    input_file: str    
    model_type: Optional[str] = "el" 

class PrimeRequest(BaseModel):
    input_file: str    
    mhc_allele: Optional[str] = "B1801"   