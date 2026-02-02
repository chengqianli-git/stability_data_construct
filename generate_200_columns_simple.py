#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
根据表结构生成测试数据
支持多进程生成，支持指定总行数和每个文件的行数
"""

import re
import random
import json
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple, Callable
import os


class ColumnType:
    """列类型定义"""
    DATE = 'DATE'
    DATETIME = 'DATETIME'
    VARCHAR = 'VARCHAR'
    CHAR = 'CHAR'
    BOOLEAN = 'BOOLEAN'
    TINYINT = 'TINYINT'
    SMALLINT = 'SMALLINT'
    INT = 'INT'
    BIGINT = 'BIGINT'
    LARGEINT = 'LARGEINT'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    DECIMAL = 'DECIMAL'
    ARRAY = 'ARRAY'
    JSON = 'JSON'


def parse_column_def(line: str) -> Tuple[str, str, dict]:
    """
    解析列定义
    返回: (字段名, 类型, 额外信息)
    """
    line = line.strip().rstrip(',')
    if not line:
        return None
    
    # 提取字段名
    match = re.match(r'`([^`]+)`\s+(.+)', line)
    if not match:
        return None
    
    field_name = match.group(1)
    type_def = match.group(2).strip()
    
    # 解析类型
    type_info = {}
    
    if ColumnType.ARRAY in type_def.upper():
        return (field_name, ColumnType.ARRAY, {})
    elif ColumnType.JSON in type_def.upper():
        return (field_name, ColumnType.JSON, {})
    elif ColumnType.DATE in type_def.upper():
        return (field_name, ColumnType.DATE, {})
    elif ColumnType.DATETIME in type_def.upper():
        return (field_name, ColumnType.DATETIME, {})
    elif ColumnType.BOOLEAN in type_def.upper():
        return (field_name, ColumnType.BOOLEAN, {})
    elif ColumnType.DECIMAL in type_def.upper():
        # DECIMAL(10,5)
        match = re.search(r'DECIMAL\((\d+),(\d+)\)', type_def, re.IGNORECASE)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            type_info = {'precision': precision, 'scale': scale}
        return (field_name, ColumnType.DECIMAL, type_info)
    elif ColumnType.VARCHAR in type_def.upper():
        # VARCHAR(200)
        match = re.search(r'VARCHAR\((\d+)\)', type_def, re.IGNORECASE)
        if match:
            length = int(match.group(1))
            type_info = {'length': length}
        return (field_name, ColumnType.VARCHAR, type_info)
    elif ColumnType.CHAR in type_def.upper():
        # CHAR(20)
        match = re.search(r'CHAR\((\d+)\)', type_def, re.IGNORECASE)
        if match:
            length = int(match.group(1))
            type_info = {'length': length}
        return (field_name, ColumnType.CHAR, type_info)
    elif ColumnType.TINYINT in type_def.upper():
        return (field_name, ColumnType.TINYINT, {})
    elif ColumnType.SMALLINT in type_def.upper():
        return (field_name, ColumnType.SMALLINT, {})
    elif ColumnType.INT in type_def.upper():
        return (field_name, ColumnType.INT, {})
    elif ColumnType.BIGINT in type_def.upper():
        return (field_name, ColumnType.BIGINT, {})
    elif ColumnType.LARGEINT in type_def.upper():
        return (field_name, ColumnType.LARGEINT, {})
    elif ColumnType.FLOAT in type_def.upper():
        return (field_name, ColumnType.FLOAT, {})
    elif ColumnType.DOUBLE in type_def.upper():
        return (field_name, ColumnType.DOUBLE, {})
    
    return None


def generate_date() -> str:
    """生成日期 YYYY-MM-DD（随机）"""
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')


def generate_date_by_index(index: int) -> str:
    """根据索引生成日期，确保均匀分布"""
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    total_days = delta.days + 1
    
    # 使用模运算确保日期在范围内均匀分布
    day_offset = index % total_days
    return (start_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')


def generate_datetime() -> str:
    """生成日期时间 YYYY-MM-DD HH:MM:SS"""
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2024, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400 - 1)
    dt = start_date + timedelta(days=random_days, seconds=random_seconds)
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def generate_string(length: int) -> str:
    """生成随机字符串"""
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(random.choice(chars) for _ in range(length))


def generate_array() -> str:
    """生成数组格式 '["3350", "8063", "17485"]'"""
    array_size = random.randint(2, 5)
    values = [random.randint(1000, 20000) for _ in range(array_size)]
    return f"{json.dumps(values)}"


def generate_json() -> str:
    """生成JSON格式"""
    jobs = [
        "Advertising account executive", "Software engineer", "Data analyst",
        "Marketing manager", "Sales representative", "Product manager",
        "Business analyst", "Project manager", "Designer", "Developer"
    ]
    companies = [
        "Graham Inc", "Tech Corp", "Data Systems", "Business Solutions",
        "Innovation Labs", "Global Services", "Digital Media", "Cloud Tech"
    ]
    domains = ["hotmail.com", "gmail.com", "yahoo.com", "company.com"]
    
    job = random.choice(jobs)
    company = random.choice(companies)
    name = generate_string(8).lower()
    domain = random.choice(domains)
    email = f"{name}@{domain}"
    birthdate = generate_date()
    
    json_obj = {
        "job": job,
        "company": company,
        "mail": email,
        "birthdate": birthdate
    }
    return json.dumps(json_obj)


def generate_value(field_name: str, field_type: str, type_info: dict, 
                  is_key_field: bool = False, date_index: int = None) -> str:
    """
    根据字段类型生成值
    
    Args:
        field_name: 字段名
        field_type: 字段类型
        type_info: 类型额外信息
        is_key_field: 是否为键字段（k1-k5），键字段不应为NULL
        date_index: 日期索引（用于k1均匀分布）
    """
    # 键字段（k1-k5）不应为NULL
    if not is_key_field and random.random() < 0.05:  # 5%的概率为NULL
        return '\\N'
    
    if field_type == ColumnType.DATE:
        if date_index is not None:
            # 使用预生成的日期列表确保均匀分布
            return generate_date_by_index(date_index)
        return generate_date()
    elif field_type == ColumnType.DATETIME:
        return generate_datetime()
    elif field_type == ColumnType.BOOLEAN:
        return 'true' if random.choice([True, False]) else 'false'
    elif field_type == ColumnType.TINYINT:
        return str(random.randint(-128, 127))
    elif field_type == ColumnType.SMALLINT:
        return str(random.randint(-32768, 32767))
    elif field_type == ColumnType.INT:
        return str(random.randint(-2147483648, 2147483647))
    elif field_type == ColumnType.BIGINT:
        return str(random.randint(-9223372036854775808, 9223372036854775807))
    elif field_type == ColumnType.LARGEINT:
        return str(random.randint(-9223372036854775808, 9223372036854775807))
    elif field_type == ColumnType.FLOAT:
        return f"{random.uniform(-1000.0, 1000.0):.6f}"
    elif field_type == ColumnType.DOUBLE:
        return f"{random.uniform(-10000.0, 10000.0):.10f}"
    elif field_type == ColumnType.DECIMAL:
        precision = type_info.get('precision', 10)
        scale = type_info.get('scale', 5)
        max_int = 10 ** (precision - scale) - 1
        int_part = random.randint(-max_int, max_int)
        decimal_part = random.randint(0, 10 ** scale - 1)
        return f"{int_part}.{decimal_part:0{scale}d}"
    elif field_type == ColumnType.VARCHAR:
        length = type_info.get('length', 20)
        actual_length = random.randint(1, min(length, 50))
        return generate_string(actual_length)
    elif field_type == ColumnType.CHAR:
        length = type_info.get('length', 20)
        return generate_string(length)
    elif field_type == ColumnType.ARRAY:
        return generate_array()
    elif field_type == ColumnType.JSON:
        return generate_json()
    
    return ''


def parse_columns_file(file_path: str) -> List[Tuple[str, str, dict]]:
    """解析列定义文件"""
    columns = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            col_def = parse_column_def(line)
            if col_def:
                columns.append(col_def)
    return columns


def generate_row(columns: List[Tuple[str, str, dict]], row_index: int = None) -> str:
    """
    生成一行数据
    
    Args:
        columns: 列定义列表
        row_index: 行索引（用于k1均匀分布）
    """
    values = []
    for field_name, field_type, type_info in columns:
        # 判断是否为键字段（k1-k5）
        is_key_field = field_name.startswith('k') and field_name[1:].isdigit()
        
        # 对于k1，使用行索引确保均匀分布
        date_index = row_index if field_name == 'k1' and row_index is not None else None
        
        value = generate_value(field_name, field_type, type_info, 
                              is_key_field=is_key_field, date_index=date_index)
        values.append(value)
    return '\t'.join(values)


def generate_file(args_tuple):
    """
    生成一个文件的数据
    使用元组参数以便多进程传递
    """
    file_path, columns, start_row, num_rows = args_tuple
    with open(file_path, 'w', encoding='utf-8') as f:
        for i in range(num_rows):
            # 使用全局行索引确保k1均匀分布
            row_index = start_row + i
            row = generate_row(columns, row_index=row_index)
            f.write(row + '\n')
            if (i + 1) % 10000 == 0:
                print(f"已生成文件 {file_path} 的 {i + 1}/{num_rows} 行")


def main():
    parser = argparse.ArgumentParser(description='根据表结构生成测试数据')
    parser.add_argument('--columns', '-c', default='columns.txt',
                       help='列定义文件路径 (默认: columns.txt)')
    parser.add_argument('--total-rows', '-t', type=int, default=1000,
                       help='总行数 (默认: 1000000)')
    parser.add_argument('--rows-per-file', '-r', type=int, default=1000,
                       help='每个文件的行数 (默认: 100000)')
    parser.add_argument('--output-dir', '-o', default='output',
                       help='输出目录 (默认: output)')
    parser.add_argument('--processes', '-n', type=int, default=4,
                       help='进程数 (默认: 4)')
    
    args = parser.parse_args()
    
    # 解析列定义
    print(f"正在解析列定义文件: {args.columns}")
    columns = parse_columns_file(args.columns)
    print(f"解析完成，共 {len(columns)} 个字段")
    
    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 计算需要生成的文件数
    num_files = (args.total_rows + args.rows_per_file - 1) // args.rows_per_file
    print(f"将生成 {num_files} 个文件，每个文件 {args.rows_per_file} 行")
    print(f"使用 {args.processes} 个进程")
    
    # 使用进程池生成文件
    with ProcessPoolExecutor(max_workers=args.processes) as executor:
        futures = []
        
        for file_idx in range(num_files):
            start_row = file_idx * args.rows_per_file
            remaining_rows = args.total_rows - start_row
            rows_in_file = min(args.rows_per_file, remaining_rows)
            
            if rows_in_file <= 0:
                break
            
            file_path = os.path.join(args.output_dir, f'data_{file_idx + 1:04d}.txt')
            # 使用元组传递参数，便于多进程序列化
            future = executor.submit(generate_file, (file_path, columns, start_row, rows_in_file))
            futures.append((future, file_path, rows_in_file))
        
        # 等待所有任务完成
        completed = 0
        for future, file_path, rows_in_file in futures:
            future.result()
            completed += 1
            print(f"完成文件 {completed}/{len(futures)}: {file_path} ({rows_in_file} 行)")
    
    print(f"\n所有文件生成完成！输出目录: {args.output_dir}")


if __name__ == '__main__':
    main()

