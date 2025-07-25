from typing import List, Tuple

#肽段文件降重，输入字符串
def deduplicate_fasta_by_sequence(fasta_str: str) -> Tuple[str, int, int]:
    lines = fasta_str.strip().split('\n')
    seen_seq = set()
    result = []
    total_before = 0
    total_after = 0
    i = 0
    while i < len(lines):
        if lines[i].startswith('>'):
            total_before += 1
            desc = lines[i]
            i += 1
            # 合并多行序列，直到下一个描述行或文件结束
            seq_parts = []
            while i < len(lines) and not lines[i].startswith('>'):
                seq_parts.append(lines[i].strip())  # 移除行首尾空格
                i += 1
            seq = ''.join(seq_parts)  # 合并为连续序列
            if seq not in seen_seq:
                seen_seq.add(seq)
                result.append(desc)
                result.append(seq)  # 注意：此处改为单行序列输出
                total_after += 1
        else:
            i += 1
    # 输出时每个序列单独一行（即使输入是多行）
    return '\n'.join(result), total_before, total_after