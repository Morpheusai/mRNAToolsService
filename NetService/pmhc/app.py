from fastapi import FastAPI 
from fastapi.middleware.cors import CORSMiddleware

from src.api import (
    netchop, 
    netCTLpan, 
    netMHCpan, 
    netMHCstabpan, 
    netTCR,
    bigMHC,
    prime,
    rnaPlot,
    rnaFold
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

app.post("/netchop",tags=["NetChopTool"],summary="NetChopTool")(netchop)
app.post("/netctlpan",tags=["NetCTLTool"],summary="NetCTLTool")(netCTLpan)
app.post("/netmhcpan",tags=["NetMHCPanTool"],summary="NetMHCPanTool")(netMHCpan)
app.post("/netmhcstabpan",tags=["NetMHCStabPanTool"],summary="NetMHCStabPanTool")(netMHCstabpan)
app.post("/nettcr",tags=["NetTCRTool"],summary="NetTCRTool")(netTCR)
app.post("/bigmhc",tags=["BigMHCTool"],summary="BigMHCTool")(bigMHC)
app.post("/prime",tags=["PrimeTool"],summary="PrimeTool")(prime)
app.post("/rnaplot",tags=["RNAPlotTool"],summary="RNAPlotTool")(rnaPlot)
app.post("/rnafold",tags=["RNAFoldTool"],summary="RNAFoldTool")(rnaFold)
