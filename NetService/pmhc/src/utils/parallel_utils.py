import os
import math
import asyncio
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
from typing import List, Callable, Any

# 1. 拆分FASTA文件

def split_fasta(input_fasta: str, num_workers: int, output_dir: str) -> List[str]:
    """
    将一个FASTA文件均匀拆分为num_workers个子文件，返回子文件路径列表。
    拆分原则：每个子文件包含尽量均匀数量的肽段（以'>'开头为一条记录）。
    :param input_fasta: 原始FASTA文件路径
    :param num_workers: 并行任务数
    :param output_dir: 拆分后子文件存放目录
    :return: 子FASTA文件路径列表
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(input_fasta, 'r') as f:
        lines = f.readlines()
    # 按>分组，每个肽段为一个record
    records = []
    current = []
    for line in lines:
        if line.startswith('>'):
            if current:
                records.append(current)
            current = [line]
        else:
            current.append(line)
    if current:
        records.append(current)
    # 均匀分配到num_workers个文件
    chunk_size = math.ceil(len(records) / num_workers)
    sub_files = []
    for i in range(num_workers):
        chunk = records[i*chunk_size:(i+1)*chunk_size]
        if not chunk:
            continue
        sub_path = output_dir / f"split_{i+1}.fasta"
        with open(sub_path, 'w') as f:
            for rec in chunk:
                f.writelines(rec)
        sub_files.append(str(sub_path))
    return sub_files

# 2. 并发调度外部命令
async def run_commands_async(
    cmd_func: Callable[[str, Any], Any],
    fasta_files: List[str],
    *args,
    num_workers: int = 4,
    **kwargs
) -> List[Any]:
    """
    并发调度cmd_func（如run_netctlpan），每个fasta文件一个任务。
    :param cmd_func: 需要并发执行的异步函数，参数第一个为fasta文件路径
    :param fasta_files: 拆分后的FASTA文件路径列表
    :param num_workers: 最大并发数
    :return: 每个任务的返回结果列表
    """
    sem = asyncio.Semaphore(num_workers)  # 控制最大并发数
    async def run_one(fasta_file):
        async with sem:
            return await cmd_func(fasta_file, *args, **kwargs)
    tasks = [run_one(f) for f in fasta_files]
    return await asyncio.gather(*tasks)

# 3. 合并Excel

def merge_excels(excel_files: List[str], output_excel: str):
    """
    合并多个Excel文件为一个，只保留第一个表的表头，其余所有内容原样追加。
    :param excel_files: 需要合并的Excel文件路径列表
    :param output_excel: 合并后输出的Excel文件路径
    """
    # 读取第一个表，保留表头
    merged = pd.read_excel(excel_files[0], sheet_name=0, header=0)
    # 依次读取后续表，去掉表头（header=None），直接追加
    for file in excel_files[1:]:
        df = pd.read_excel(file, sheet_name=0, header=None, skiprows=1)
        merged = pd.concat([merged, df], ignore_index=True)
    merged.to_excel(output_excel, index=False, header=True) 