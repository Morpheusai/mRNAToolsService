from fastapi import FastAPI 
from fastapi.middleware.cors import CORSMiddleware

from src.api import immuneapp, immuneappneo, transphla, lineardesign

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
    return {"Hello": "我提供ImmuneApp And TransPHLA 工具服务"}

app.post("/ImmuneApp",tags=["ImmuneApp_presentation"],summary="ImmuneAppTool")(immuneapp)
app.post("/ImmuneApp_Neo",tags=["ImmuneApp_immunogenicity"],summary="ImmuneAppNeoTool")(immuneappneo)
app.post("/TransPHLA_AOMP",tags=["TransPHLA_AOMP"],summary="TransPHLATool")(transphla)
app.post("/LinearDesign",tags=["LinearDesign"],summary="LinearDesignTool")(lineardesign)
