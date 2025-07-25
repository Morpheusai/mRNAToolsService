from fastapi import FastAPI 
from fastapi.middleware.cors import CORSMiddleware

from src.api import (
    vcfswitch
)

app = FastAPI()

origins = [
    "*",  # 允许的来源，可以添加多个
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # 允许访问的源列表
    allow_credentials=True,  # 支持cookie跨域
    allow_methods=["*"],  # 允许的请求方法
    allow_headers=["*"],  # 允许的请求头
)

@app.get("/")
def read_root():
    return {"Hello": "我提供NetTools工具服务"}


app.post("/vcfswitch",tags=["VcfSwitchTool"],summary="VcfSwitchTool")(vcfswitch)
