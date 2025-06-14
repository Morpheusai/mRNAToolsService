from typing import Optional,List
from pydantic  import BaseModel




class UniPMT(BaseModel):
    input_file: str
