#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parquet to CSV 文件转换工具

功能：
1. 单个文件转换
2. 批量文件转换（支持目录）
3. 递归处理子目录
4. 保持原有目录结构
5. 自动跳过已存在的文件（可选）
6. 支持自定义CSV格式选项
"""

import os
import sys
import argparse
import logging
import json
import csv as python_csv
from pathlib import Path
from typing import Optional
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow as pa
import pyarrow.compute as pc


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ParquetToCsvConverter:
    """Parquet到CSV格式转换器"""
    
    def __init__(
        self, 
        overwrite: bool = False, 
        recursive: bool = False,
        delimiter: str = ',',
        include_header: bool = True,
        batch_size: int = 1000000
    ):
        """
        初始化转换器
        
        Args:
            overwrite: 是否覆盖已存在的文件
            recursive: 是否递归处理子目录
            delimiter: CSV分隔符，默认为逗号
            include_header: 是否包含表头
            batch_size: 批处理大小（处理大文件时使用）
        """
        self.overwrite = overwrite
        self.recursive = recursive
        self.delimiter = delimiter
        self.include_header = include_header
        self.batch_size = batch_size
        self.success_count = 0
        self.error_count = 0
        self.skip_count = 0
    
    def _should_convert_to_null(self, arrow_type) -> bool:
        """检查是否应该将字段置为空（struct、map类型）"""
        return (
            pa.types.is_struct(arrow_type) or
            pa.types.is_map(arrow_type)
        )
    
    def _should_convert_to_json(self, arrow_type) -> bool:
        """检查是否应该转换为JSON（list类型）"""
        return (
            pa.types.is_list(arrow_type) or
            pa.types.is_large_list(arrow_type) or
            pa.types.is_fixed_size_list(arrow_type)
        )
    
    def _is_complex_type(self, arrow_type) -> bool:
        """检查是否为复杂类型（列表、结构体、映射等）"""
        return (
            self._should_convert_to_json(arrow_type) or
            self._should_convert_to_null(arrow_type) or
            pa.types.is_nested(arrow_type)
        )
    
    def _convert_column_to_null(self, column: pa.Array) -> pa.Array:
        """将列的所有值置为 NULL"""
        return pa.array([None] * len(column), type=pa.string())
    
    def _write_csv_with_custom_escape(self, table: pa.Table, output_path: Path) -> None:
        """使用自定义转义规则写入CSV文件"""
        # 将 PyArrow 表转换为 Python 列表
        data = []
        for i in range(table.num_rows):
            row = []
            for col_name in table.column_names:
                value = table.column(col_name)[i].as_py()
                if value is None:
                    row.append('')
                else:
                    row.append(str(value))
            data.append(row)
        
        # 写入 CSV
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = python_csv.writer(
                f,
                delimiter=self.delimiter,
                quoting=python_csv.QUOTE_MINIMAL,
                doublequote=False,  # 不使用双引号转义
                escapechar='\\'      # 使用反斜杠转义
            )
            
            # 写入表头
            if self.include_header:
                writer.writerow(table.column_names)
            
            # 写入数据
            writer.writerows(data)
    
    def _convert_complex_column_to_json(self, column: pa.Array) -> pa.Array:
        """将复杂类型列转换为 JSON 字符串（紧凑格式）"""
        json_strings = []
        for i in range(len(column)):
            value = column[i]
            if value.is_valid:
                # 转换为 Python 对象再序列化为 JSON
                # separators=(',', ':') 使输出紧凑，逗号和冒号后无空格
                # 不需要预先转义，CSV writer 会使用反斜杠自动转义
                py_value = value.as_py()
                json_str = json.dumps(py_value, ensure_ascii=False, separators=(',', ':'))
                json_strings.append(json_str)
            else:
                json_strings.append(None)
        
        return pa.array(json_strings, type=pa.string())
    
    def _process_table_for_csv(self, table: pa.Table) -> pa.Table:
        """处理表格，将复杂类型转换为合适的格式"""
        new_columns = []
        new_names = []
        has_complex_types = False
        struct_map_count = 0
        list_count = 0
        
        for i, field in enumerate(table.schema):
            column = table.column(i)
            
            if self._should_convert_to_null(field.type):
                # struct 和 map 类型置为空
                has_complex_types = True
                struct_map_count += 1
                logger.debug(f"将复杂类型列置空: {field.name} ({field.type})")
                new_column = self._convert_column_to_null(column)
                new_columns.append(new_column)
            elif self._should_convert_to_json(field.type):
                # list 类型转换为 JSON 字符串
                has_complex_types = True
                list_count += 1
                logger.debug(f"转换列表类型为 JSON: {field.name} ({field.type})")
                new_column = self._convert_complex_column_to_json(column)
                new_columns.append(new_column)
            else:
                # 简单类型保持不变
                new_columns.append(column)
            
            new_names.append(field.name)
        
        if has_complex_types:
            msg_parts = []
            if struct_map_count > 0:
                msg_parts.append(f"{struct_map_count}个struct/map列已置空")
            if list_count > 0:
                msg_parts.append(f"{list_count}个list列已转换为JSON")
            logger.info(f"检测到复杂类型: {', '.join(msg_parts)}")
        
        return pa.table(new_columns, names=new_names)
    
    def convert_file(self, input_path: str, output_path: Optional[str] = None) -> bool:
        """
        转换单个parquet文件到csv格式
        
        Args:
            input_path: 输入的parquet文件路径
            output_path: 输出的csv文件路径，如果为None则自动生成
            
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
        
        # 生成输出路径
        if output_path is None:
            output_path = input_path.with_suffix('.csv')
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
            table = pq.read_table(input_path)
            
            # 处理复杂类型（如列表、结构体等）
            table = self._process_table_for_csv(table)
            
            # 写入csv文件（使用自定义方法避免双引号转义问题）
            logger.info(f"写入文件: {output_path}")
            self._write_csv_with_custom_escape(table, output_path)
            
            # 获取文件大小信息
            input_size = input_path.stat().st_size / (1024 * 1024)  # MB
            output_size = output_path.stat().st_size / (1024 * 1024)  # MB
            
            logger.info(
                f"✓ 转换成功: {input_path.name} ({input_size:.2f}MB) -> "
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
                csv_file = parquet_file.with_suffix('.csv')
            else:
                # 保持目录结构
                output_dir_path = Path(output_dir)
                relative_path = parquet_file.relative_to(input_dir)
                csv_file = output_dir_path / relative_path.with_suffix('.csv')
            
            self.convert_file(parquet_file, csv_file)
    
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
        description='Parquet到CSV文件格式转换工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 转换单个文件
  python parquet_to_csv.py input.parquet
  
  # 转换单个文件并指定输出路径
  python parquet_to_csv.py input.parquet -o output.csv
  
  # 转换目录中的所有parquet文件
  python parquet_to_csv.py /path/to/parquet_dir
  
  # 递归转换目录及子目录中的所有文件
  python parquet_to_csv.py /path/to/parquet_dir -r
  
  # 转换到指定输出目录（保持目录结构）
  python parquet_to_csv.py /path/to/input_dir -o /path/to/output_dir
  
  # 使用制表符作为分隔符（TSV格式）
  python parquet_to_csv.py input.parquet -o output.tsv --delimiter $'\\t'
  
  # 覆盖已存在的文件
  python parquet_to_csv.py input.parquet --overwrite
        """
    )
    
    parser.add_argument(
        'input',
        help='输入的parquet文件或目录路径'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='输出的csv文件或目录路径（可选）',
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
        '--delimiter',
        help='CSV分隔符（默认为逗号）',
        default=','
    )
    
    parser.add_argument(
        '--no-header',
        help='不包含表头',
        action='store_true'
    )
    
    parser.add_argument(
        '--batch-size',
        help='批处理大小（默认1000000行）',
        type=int,
        default=1000000
    )
    
    parser.add_argument(
        '-v', '--verbose',
        help='显示详细日志',
        action='store_true'
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 创建转换器
    converter = ParquetToCsvConverter(
        overwrite=args.overwrite,
        recursive=args.recursive,
        delimiter=args.delimiter,
        include_header=not args.no_header,
        batch_size=args.batch_size
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

