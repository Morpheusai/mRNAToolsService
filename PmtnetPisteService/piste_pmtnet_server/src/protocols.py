from typing import Optional
from pydantic  import BaseModel
 
class PisteRequest(BaseModel):
    input_file_dir_minio: str    
    model_name: Optional[str] = "random" 
    threshold:Optional[float] = 0.5
    antigen_type: Optional[str] = "MT" 

class PMTNetRequest(BaseModel):
    input_file_dir_minio: str    