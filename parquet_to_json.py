#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parquet to JSON Lines 文件转换工具

功能：
1. 单个文件转换
2. 批量文件转换（支持目录）
3. 递归处理子目录
4. 保持原有目录结构
5. 自动跳过已存在的文件（可选）
6. 输出格式为JSON Lines（每行一个JSON对象）
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional
from decimal import Decimal
import pyarrow.parquet as pq
import json
import math
import numpy as np


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ParquetToJsonConverter:
    """Parquet到JSON Lines格式转换器"""
    
    def __init__(
        self, 
        overwrite: bool = False, 
        recursive: bool = False,
        indent: bool = False,
        encoding: str = 'utf-8',
        batch_size: int = 10000,
        force_string_fields: list = None
    ):
        """
        初始化转换器
        
        Args:
            overwrite: 是否覆盖已存在的文件
            recursive: 是否递归处理子目录
            indent: 是否对JSON进行格式化缩进（如果为True，每个JSON对象会被格式化，但仍然在单独的行上）
            encoding: 输出文件编码，默认为utf-8
            batch_size: 批处理大小（处理大文件时使用）
            force_string_fields: 需要强制转换为字符串的字段名列表（如 ['largeint_metric']）
        """
        self.overwrite = overwrite
        self.recursive = recursive
        self.indent = 2 if indent else None
        self.encoding = encoding
        self.batch_size = batch_size
        # 默认将 largeint_metric 字段转换为字符串
        self.force_string_fields = set(force_string_fields or ['largeint_metric'])
        self.success_count = 0
        self.error_count = 0
        self.skip_count = 0
    
    def _convert_value(self, value, field_name=None):
        """
        递归转换各种特殊类型为JSON可序列化的类型
        
        Args:
            value: 要转换的值（可以是任何类型）
            field_name: 当前字段名（用于特殊字段处理）
            
        Returns:
            JSON可序列化的值
        """
        # 如果字段名在强制转换列表中，直接转换为字符串
        if field_name and field_name in self.force_string_fields:
            if value is None or (hasattr(value, 'isnull') and callable(value.isnull) and value.isnull()):
                return None
            # 如果是浮点数（可能由于某些原因被转换了），先转为整数再转字符串
            if isinstance(value, (float, np.floating)):
                # 检查是否是整数值的浮点数
                if not math.isnan(value) and value == int(value):
                    return str(int(value))
            return str(value)
        
        # 处理字符串类型 - 检查是否为JSON字符串
        if isinstance(value, str):
            # 尝试解析JSON字符串
            stripped = value.strip()
            if (stripped.startswith('{') and stripped.endswith('}')) or \
               (stripped.startswith('[') and stripped.endswith(']')):
                try:
                    # 尝试解析为JSON
                    parsed = json.loads(value)
                    # 递归处理解析后的对象
                    return self._convert_value(parsed)
                except (json.JSONDecodeError, ValueError):
                    # 如果不是有效的JSON，保持原字符串
                    return value
            return value
        
        # 处理字典类型（递归处理）
        elif isinstance(value, dict):
            return {k: self._convert_value(v, field_name=k) for k, v in value.items()}
        
        # 处理列表和numpy数组
        elif isinstance(value, (list, tuple, np.ndarray)):
            # 检查是否为PyArrow的Map类型（转换后的格式）
            # Map类型可能有以下几种表示形式：
            # 1. [{'key': k1, 'value': v1}, {'key': k2, 'value': v2}, ...]
            # 2. [['key1', 'value1'], ['key2', 'value2'], ...]
            # 3. [('key1', 'value1'), ('key2', 'value2'), ...]
            if isinstance(value, (list, tuple)) and len(value) > 0:
                first_item = value[0]
                
                # 格式1: 字典格式 [{'key': k, 'value': v}, ...]
                if isinstance(first_item, dict) and 'key' in first_item and 'value' in first_item:
                    try:
                        result_dict = {}
                        for item in value:
                            if isinstance(item, dict) and 'key' in item and 'value' in item:
                                key = self._convert_value(item['key'])
                                val = self._convert_value(item['value'])
                                result_dict[str(key)] = val
                        return result_dict
                    except Exception:
                        pass
                
                # 格式2和3: 列表或元组格式 [['k', 'v'], ...] 或 [('k', 'v'), ...]
                # 检查是否所有元素都是长度为2的列表/元组
                elif isinstance(first_item, (list, tuple)) and len(first_item) == 2:
                    # 检查所有元素是否都符合这个模式
                    all_pairs = all(
                        isinstance(item, (list, tuple)) and len(item) == 2 
                        for item in value
                    )
                    
                    if all_pairs:
                        # 这很可能是Map类型，转换为字典
                        try:
                            result_dict = {}
                            for item in value:
                                key = self._convert_value(item[0])
                                val = self._convert_value(item[1])
                                result_dict[str(key)] = val
                            return result_dict
                        except Exception:
                            # 如果转换失败，按普通列表处理
                            pass
            
            # 普通列表/数组：将ndarray转换为list，并递归处理每个元素
            return [self._convert_value(item) for item in value]
        
        # 处理Decimal类型
        elif isinstance(value, Decimal):
            return float(value)
        
        # 处理pandas的NaN、NaT等
        elif isinstance(value, float) and math.isnan(value):
            return None
        
        # 处理Python原生大整数（超过JavaScript安全整数范围）
        elif isinstance(value, int):
            # JavaScript安全整数范围: -(2^53 - 1) 到 (2^53 - 1)
            MAX_SAFE_INTEGER = 9007199254740991  # 2^53 - 1
            MIN_SAFE_INTEGER = -9007199254740991  # -(2^53 - 1)
            
            if value > MAX_SAFE_INTEGER or value < MIN_SAFE_INTEGER:
                # 超出安全范围，转换为字符串
                return str(value)
            else:
                return value
        
        # 处理numpy的数值类型
        elif isinstance(value, (np.floating, np.integer)):
            if np.isnan(value):
                return None
            else:
                if isinstance(value, np.integer):
                    # 检查numpy整数是否超出安全范围
                    int_value = int(value)
                    MAX_SAFE_INTEGER = 9007199254740991
                    MIN_SAFE_INTEGER = -9007199254740991
                    
                    if int_value > MAX_SAFE_INTEGER or int_value < MIN_SAFE_INTEGER:
                        return str(int_value)
                    else:
                        return int_value
                else:
                    return float(value)
        
        # 处理numpy的bool类型
        elif isinstance(value, np.bool_):
            return bool(value)
        
        # 处理pandas的NA/NaT
        elif hasattr(value, 'isnull') and callable(value.isnull):
            try:
                if value.isnull():
                    return None
            except:
                pass
        
        # 处理日期时间类型
        if hasattr(value, 'isoformat') and callable(value.isoformat):
            try:
                return value.isoformat()
            except:
                pass
        
        # 处理bytes类型
        if isinstance(value, bytes):
            try:
                return value.decode('utf-8')
            except:
                return str(value)
        
        # 其他类型直接返回
        return value
    
    def convert_file(self, input_path: str, output_path: Optional[str] = None) -> bool:
        """
        转换单个parquet文件到JSON Lines格式
        
        Args:
            input_path: 输入的parquet文件路径
            output_path: 输出的JSON文件路径，如果为None则自动生成（后缀为.json）
            
        Returns:
            bool: 转换是否成功
        """
        input_path = Path(input_path)
        
        # 检查输入文件是否存在
        if not input_path.exists():
            logger.error(f"输入文件不存在: {input_path}")
            return False
        
        # 检查是否为parquet文件
        if input_path.suffix.lower() != '.parquet':
            logger.warning(f"跳过非parquet文件: {input_path}")
            return False
        
        # 生成输出路径（默认使用.json后缀）
        if output_path is None:
            output_path = input_path.with_suffix('.json')
        else:
            output_path = Path(output_path)
        
        # 检查输出文件是否已存在
        if output_path.exists() and not self.overwrite:
            logger.info(f"文件已存在，跳过: {output_path}")
            self.skip_count += 1
            return True
        
        try:
            # 确保输出目录存在
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 读取parquet文件
            logger.info(f"读取文件: {input_path}")
            parquet_file = pq.ParquetFile(input_path)
            
            # 打开输出文件
            logger.info(f"写入文件: {output_path}")
            total_rows = 0
            
            with open(output_path, 'w', encoding=self.encoding) as f:
                # 批量读取和写入
                for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                    # 将批次转换为pandas DataFrame
                    df = batch.to_pandas()
                    
                    # 逐行写入JSON
                    for idx in range(len(df)):
                        # 直接从DataFrame构建字典，避免to_dict()导致的类型转换
                        row_dict = {}
                        for col in df.columns:
                            row_dict[col] = df[col].iloc[idx]
                        
                        # 处理特殊类型（如NaN、日期、Decimal等）
                        row_dict = self._convert_value(row_dict)
                        
                        # 写入JSON行
                        json_line = json.dumps(row_dict, ensure_ascii=False, indent=self.indent)
                        # 如果有缩进，将多行JSON压缩为单行
                        if self.indent:
                            json_line = json_line.replace('\n', ' ')
                        f.write(json_line + '\n')
                        total_rows += 1
            
            # 获取文件大小信息
            input_size = input_path.stat().st_size / (1024 * 1024)  # MB
            output_size = output_path.stat().st_size / (1024 * 1024)  # MB
            
            logger.info(
                f"✓ 转换成功: {input_path.name} ({input_size:.2f}MB, {total_rows:,}行) -> "
                f"{output_path.name} ({output_size:.2f}MB)"
            )
            self.success_count += 1
            return True
            
        except Exception as e:
            logger.error(f"✗ 转换失败: {input_path} - {str(e)}")
            self.error_count += 1
            # 如果转换失败，删除可能生成的不完整文件
            if output_path.exists():
                try:
                    output_path.unlink()
                except:
                    pass
            return False
    
    def convert_directory(self, input_dir: str, output_dir: Optional[str] = None) -> None:
        """
        转换目录中的所有parquet文件
        
        Args:
            input_dir: 输入目录
            output_dir: 输出目录，如果为None则在原目录生成
        """
        input_dir = Path(input_dir)
        
        if not input_dir.exists():
            logger.error(f"输入目录不存在: {input_dir}")
            return
        
        if not input_dir.is_dir():
            logger.error(f"输入路径不是目录: {input_dir}")
            return
        
        # 查找所有parquet文件
        if self.recursive:
            parquet_files = list(input_dir.rglob('*.parquet'))
        else:
            parquet_files = list(input_dir.glob('*.parquet'))
        
        if not parquet_files:
            logger.warning(f"未找到parquet文件: {input_dir}")
            return
        
        logger.info(f"找到 {len(parquet_files)} 个parquet文件")
        
        # 转换每个文件
        for i, parquet_file in enumerate(parquet_files, 1):
            logger.info(f"\n进度: [{i}/{len(parquet_files)}]")
            
            # 计算输出路径
            if output_dir is None:
                # 在原目录生成
                json_file = parquet_file.with_suffix('.json')
            else:
                # 保持目录结构
                output_dir_path = Path(output_dir)
                relative_path = parquet_file.relative_to(input_dir)
                json_file = output_dir_path / relative_path.with_suffix('.json')
            
            self.convert_file(parquet_file, json_file)
    
    def print_summary(self) -> None:
        """打印转换统计信息"""
        logger.info("\n" + "="*60)
        logger.info("转换统计:")
        logger.info(f"  成功: {self.success_count} 个文件")
        logger.info(f"  跳过: {self.skip_count} 个文件")
        logger.info(f"  失败: {self.error_count} 个文件")
        logger.info(f"  总计: {self.success_count + self.skip_count + self.error_count} 个文件")
        logger.info("="*60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Parquet到JSON Lines文件格式转换工具（输出txt文件，每行一个JSON对象）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 转换单个文件（输出为.json）
  python parquet_to_json.py input.parquet
  
  # 转换单个文件并指定输出路径
  python parquet_to_json.py input.parquet -o output.json
  
  # 转换目录中的所有parquet文件
  python parquet_to_json.py /path/to/parquet_dir
  
  # 递归转换目录及子目录中的所有文件
  python parquet_to_json.py /path/to/parquet_dir -r
  
  # 转换到指定输出目录（保持目录结构）
  python parquet_to_json.py /path/to/input_dir -o /path/to/output_dir
  
  # 覆盖已存在的文件
  python parquet_to_json.py input.parquet --overwrite
  
  # 使用较小的批处理大小（适合内存较小的情况）
  python parquet_to_json.py input.parquet --batch-size 5000

输出格式说明:
  输出文件为.json格式，每行包含一个JSON对象
  示例:
    {"id": 1, "name": "张三", "age": 25}
    {"id": 2, "name": "李四", "age": 30}
    {"id": 3, "name": "王五", "age": 28}
        """
    )
    
    parser.add_argument(
        'input',
        help='输入的parquet文件或目录路径'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='输出的JSON文件或目录路径（可选，默认为.json后缀）',
        default=None
    )
    
    parser.add_argument(
        '-r', '--recursive',
        help='递归处理子目录',
        action='store_true'
    )
    
    parser.add_argument(
        '--overwrite',
        help='覆盖已存在的文件',
        action='store_true'
    )
    
    parser.add_argument(
        '--encoding',
        help='输出文件编码（默认为utf-8）',
        default='utf-8'
    )
    
    parser.add_argument(
        '--batch-size',
        help='批处理大小（默认10000行）',
        type=int,
        default=10000
    )
    
    parser.add_argument(
        '-v', '--verbose',
        help='显示详细日志',
        action='store_true'
    )
    
    parser.add_argument(
        '--force-string-fields',
        help='强制转换为字符串的字段名列表（逗号分隔），默认为 largeint_metric',
        default='largeint_metric'
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 解析需要强制转换为字符串的字段名
    force_string_fields = [f.strip() for f in args.force_string_fields.split(',') if f.strip()]
    
    # 创建转换器
    converter = ParquetToJsonConverter(
        overwrite=args.overwrite,
        recursive=args.recursive,
        encoding=args.encoding,
        batch_size=args.batch_size,
        force_string_fields=force_string_fields
    )
    
    # 判断输入是文件还是目录
    input_path = Path(args.input)
    
    if not input_path.exists():
        logger.error(f"输入路径不存在: {input_path}")
        sys.exit(1)
    
    try:
        if input_path.is_file():
            # 转换单个文件
            converter.convert_file(args.input, args.output)
        elif input_path.is_dir():
            # 转换目录
            converter.convert_directory(args.input, args.output)
        else:
            logger.error(f"无效的输入路径: {input_path}")
            sys.exit(1)
        
        # 打印统计信息
        converter.print_summary()
        
        # 根据结果设置退出码
        if converter.error_count > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.warning("\n用户中断操作")
        converter.print_summary()
        sys.exit(130)
    except Exception as e:
        logger.error(f"发生错误: {str(e)}", exc_info=args.verbose)
        sys.exit(1)


if __name__ == '__main__':
    main()

