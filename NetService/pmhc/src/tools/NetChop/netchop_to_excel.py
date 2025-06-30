import re
import pandas as pd
from pathlib import Path
from openpyxl.styles import Alignment
from openpyxl import load_workbook

from src.utils.log import logger

def save_excel(output: str, output_dir: str, output_filename: str) -> bool:
    """
    将数据保存到Excel文件（适用于netChop输出格式，支持多块结果和多个summary插入）
    
    Args:
        output: 要解析的原始文本数据
        output_dir: 输出目录路径 
        output_filename: 输出文件名
        
    Returns:
        bool: 成功返回True，失败返回False
    """
    try:
        # 数据解析 - 针对netChop输出格式
        table_pattern = re.compile(
            r"\s*(\d+)\s+([A-Z])\s+([^\s])\s+([\d.]+)\s+([^\s]+)"
        )
        summary_pattern = re.compile(r"Number of cleavage\s+[^\s]+.*")

        # 记录summary出现的行号和内容
        summary_rows_idx = []
        summary_rows_val = []
        for match in summary_pattern.finditer(output):
            summary_rows_val.append(match.group())
            line_idx = output[:match.start()].count('\n')
            summary_rows_idx.append(line_idx)

        # 解析所有数据行，记录每一行在原始文本中的行号
        data_lines = []
        data_line_indices = []
        for m in table_pattern.finditer(output):
            data_lines.append(m.groups())
            data_line_indices.append(output[:m.start()].count('\n'))

        columns = [
            "Pos", "AA", "C", "score", "Ident"
        ]
        df = pd.DataFrame(data_lines, columns=columns)

        # 计算summary应该插入到df的哪一行
        insert_positions = []
        for summary_idx in summary_rows_idx:
            # 找到最后一个小于summary_idx的数据行
            insert_pos = 0
            for i, data_idx in enumerate(data_line_indices):
                if data_idx < summary_idx:
                    insert_pos = i + 1
                else:
                    break
            insert_positions.append(insert_pos)

        # 插入summary到对应位置
        offset = 0
        for pos, summary in zip(insert_positions, summary_rows_val):
            summary_row = [summary] + [""] * (len(columns) - 1)
            df.loc[pos + offset] = summary_row
            offset += 1
        df = df.sort_index().reset_index(drop=True)

        # 准备输出路径
        output_path = Path(output_dir) / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 写入Excel文件
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name="Results", index=False)
            workbook = writer.book
            worksheet = writer.sheets["Results"]

            # 合并summary行的单元格并居中
            for i, row in enumerate(df.itertuples(index=False), 2):
                if str(row[0]).startswith("Number of cleavage"):
                    worksheet.merge_cells(
                        start_row=i,
                        start_column=1,
                        end_row=i,
                        end_column=len(columns)
                    )
                    cell = worksheet.cell(row=i, column=1)
                    cell.alignment = Alignment(horizontal='center', vertical='center')

            # 调整列宽
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[column_letter].width = adjusted_width

            workbook.save(output_path)

        logger.info(f"Excel文件已成功保存至: {output_path}")
        return True

    except PermissionError as e:
        logger.error(f"文件权限错误: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"保存Excel时发生错误: {str(e)}", exc_info=True)
        return False