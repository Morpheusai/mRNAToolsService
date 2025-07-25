from typing import Optional,List
from pydantic  import BaseModel



class VcfSwitchRequest(BaseModel):
    normal_file: str
    tumor_file: str

class VcfSwitchResponse(BaseModel):
    type: str
    url: Optional[str] = None
    content:str
