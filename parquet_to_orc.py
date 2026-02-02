#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parquet to ORC 文件转换工具

功能：
1. 单个文件转换
2. 批量文件转换（支持目录）
3. 递归处理子目录
4. 保持原有目录结构
5. 自动跳过已存在的文件（可选）
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional
import pyarrow.parquet as pq
import pyarrow.orc
import pyarrow as pa


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ParquetToOrcConverter:
    """Parquet到ORC格式转换器"""
    
    def __init__(self, overwrite: bool = False, recursive: bool = False):
        """
        初始化转换器
        
        Args:
            overwrite: 是否覆盖已存在的文件
            recursive: 是否递归处理子目录
        """
        self.overwrite = overwrite
        self.recursive = recursive
        self.success_count = 0
        self.error_count = 0
        self.skip_count = 0
    
    def convert_file(self, input_path: str, output_path: Optional[str] = None) -> bool:
        """
        转换单个parquet文件到orc格式
        
        Args:
            input_path: 输入的parquet文件路径
            output_path: 输出的orc文件路径，如果为None则自动生成
            
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
            output_path = input_path.with_suffix('.orc')
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
            
            # 检查并转换二进制类型列为字符串类型（解决JSON列被转换为VARBINARY的问题）
            schema = table.schema
            new_arrays = []
            new_fields = []
            need_conversion = False
            converted_columns = []
            
            for i, field in enumerate(schema):
                array = table.column(i)
                # 如果是二进制类型，转换为字符串类型
                if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
                    # 将二进制数据解码为UTF-8字符串
                    # 使用 to_pylist 转换为 Python 对象，然后创建字符串数组
                    try:
                        decoded_values = [val.decode('utf-8') if val is not None else None for val in array.to_pylist()]
                        new_array = pa.array(decoded_values, type=pa.string())
                        new_arrays.append(new_array)
                        new_fields.append(pa.field(field.name, pa.string(), nullable=field.nullable, metadata=field.metadata))
                        need_conversion = True
                        converted_columns.append(field.name)
                        logger.debug(f"将列 {field.name} 从 {field.type} 转换为 string 类型")
                    except Exception as e:
                        logger.warning(f"转换列 {field.name} 时出错: {e}，保持原类型")
                        new_arrays.append(array)
                        new_fields.append(field)
                else:
                    new_arrays.append(array)
                    new_fields.append(field)
            
            # 如果需要转换，创建新表
            if need_conversion:
                new_schema = pa.schema(new_fields, metadata=schema.metadata)
                table = pa.Table.from_arrays(new_arrays, schema=new_schema)
                logger.info(f"已转换二进制类型列为字符串类型: {', '.join(converted_columns)}")
            
            # 写入orc文件
            logger.info(f"写入文件: {output_path}")
            with pa.OSFile(str(output_path), 'wb') as sink:
                writer = pa.orc.ORCWriter(sink)
                writer.write(table)
                writer.close()
            
            # 获取文件大小信息
            input_size = input_path.stat().st_size / (1024 * 1024)  # MB
            output_size = output_path.stat().st_size / (1024 * 1024)  # MB
            
            logger.info(f"✓ 转换成功: {input_path.name} ({input_size:.2f}MB) -> {output_path.name} ({output_size:.2f}MB)")
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
                orc_file = parquet_file.with_suffix('.orc')
            else:
                # 保持目录结构
                output_dir_path = Path(output_dir)
                relative_path = parquet_file.relative_to(input_dir)
                orc_file = output_dir_path / relative_path.with_suffix('.orc')
            
            self.convert_file(parquet_file, orc_file)
    
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
        description='Parquet到ORC文件格式转换工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 转换单个文件
  python parquet_to_orc.py input.parquet
  
  # 转换单个文件并指定输出路径
  python parquet_to_orc.py input.parquet -o output.orc
  
  # 转换目录中的所有parquet文件
  python parquet_to_orc.py /path/to/parquet_dir
  
  # 递归转换目录及子目录中的所有文件
  python parquet_to_orc.py /path/to/parquet_dir -r
  
  # 转换到指定输出目录（保持目录结构）
  python parquet_to_orc.py /path/to/input_dir -o /path/to/output_dir
  
  # 覆盖已存在的文件
  python parquet_to_orc.py input.parquet --overwrite
        """
    )
    
    parser.add_argument(
        'input',
        help='输入的parquet文件或目录路径'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='输出的orc文件或目录路径（可选）',
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
        '-v', '--verbose',
        help='显示详细日志',
        action='store_true'
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 创建转换器
    converter = ParquetToOrcConverter(
        overwrite=args.overwrite,
        recursive=args.recursive
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

