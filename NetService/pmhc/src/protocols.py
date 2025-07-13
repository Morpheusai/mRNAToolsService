from typing import Optional,List
from pydantic  import BaseModel

class NetChopRequest(BaseModel):
    input_filename: str
    cleavage_site_threshold: Optional[float] = 0.5
    model: Optional[int] = 0
    format: Optional[int] = 0
    strict: Optional[int] = 0
    num_workers: Optional[int] = 1
    window_sizes: Optional[List[int]] =[8,9,10,11]

class NetCTLPanRequest(BaseModel):
    input_filename: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    peptide_length: Optional[str] = "-1"
    weight_of_tap: Optional[float] = 0.025
    weight_of_clevage: Optional[float] = 0.225
    epi_threshold: Optional[float] = 1.0
    output_threshold: Optional[float] = -99.9
    sort_by: Optional[int] = -1
    num_workers: Optional[int] = 1
    mode: Optional[int] =0
    hla_mode: Optional[int] =0
    peptide_duplication_mode: Optional[int] =0

class NetMHCPanRequest(BaseModel):
    input_filename: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    peptide_length: Optional[str] = "-1"
    high_threshold_of_bp: Optional[float] = 0.5
    low_threshold_of_bp: Optional[float] = 2.0
    rank_cutoff: Optional[float] = -99.9
    num_workers: Optional[int] = 1
    mode: Optional[int] =0

class NetMHCStabPanRequest(BaseModel):
    input_file: str
    mhc_allele: Optional[str] = "HLA-A02:01"
    high_threshold_of_bp: Optional[float] = 0.5
    low_threshold_of_bp: Optional[float] = 2.0
    peptide_length: Optional[str] = "8,9,10,11"

class NetTCRRequest(BaseModel):
    input_file: str 

class BigMHCRequest(BaseModel):
    input_filename: str    
    mhc_allele: Optional[str] = "HLA-A02:01"
    model_type: Optional[str] = "el" 

class PrimeRequest(BaseModel):
    input_file: str    
    mhc_allele: Optional[str] = "B1801"   
    
class RNAPlotRequest(BaseModel):
    input_file: str    

class RNAFoldRequest(BaseModel):
    input_file: str      