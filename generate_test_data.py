#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成测试数据的脚本
支持生成parquet格式的模拟数据，数据均匀分布，支持多进程
"""

import argparse
import random
import json
from datetime import datetime, timedelta
from decimal import Decimal
from multiprocessing import Pool, cpu_count
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List


class DataGenerator:
    """数据生成器类"""
    
    def __init__(self, seed=None):
        """初始化生成器"""
        if seed is not None:
            random.seed(seed)
    
    def generate_biz_id(self) -> int:
        """生成业务ID (必填)"""
        return random.randint(1, 100)
    
    def generate_user_id(self) -> int:
        """生成用户ID (必填)"""
        return random.randint(100000, 999999999)
    
    def generate_channel_code(self) -> str:
        """生成渠道代码 (必填)"""
        channels = ['APP001', 'WEB001', 'H5001', 'API001', 'WX001', 
                   'ALI001', 'JD001', 'PDD001', 'MINI001', 'PC001']
        return random.choice(channels)
    
    def generate_event_date(self) -> str:
        """生成事件日期 (必填)"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2026, 1, 28)
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime('%Y-%m-%d')
    
    def generate_order_id(self) -> int:
        """生成订单ID"""
        return random.randint(1000000000, 9999999999) if random.random() > 0.05 else None
    
    def generate_product_id(self) -> int:
        """生成商品ID"""
        return random.randint(10000, 999999) if random.random() > 0.05 else None
    
    def generate_shop_id(self) -> int:
        """生成店铺ID"""
        return random.randint(1000, 99999) if random.random() > 0.05 else None
    
    def generate_category_id(self) -> int:
        """生成类目ID"""
        return random.randint(100, 9999) if random.random() > 0.05 else None
    
    def generate_brand_id(self) -> int:
        """生成品牌ID"""
        return random.randint(100, 9999) if random.random() > 0.05 else None
    
    def generate_device_id(self) -> str:
        """生成设备ID"""
        if random.random() > 0.05:
            return ''.join(random.choices('0123456789abcdef', k=32))
        return None
    
    def generate_session_id(self) -> str:
        """生成会话ID"""
        if random.random() > 0.05:
            return ''.join(random.choices('0123456789abcdef', k=32))
        return None
    
    def generate_region_code(self) -> str:
        """生成地区代码"""
        if random.random() > 0.05:
            return str(random.randint(100000, 999999))
        return None
    
    def generate_city_code(self) -> str:
        """生成城市代码"""
        if random.random() > 0.05:
            return str(random.randint(100000, 999999))
        return None
    
    def generate_platform(self) -> str:
        """生成平台"""
        platforms = ['iOS', 'Android', 'Web', 'H5', 'MiniProgram', 'PC']
        return random.choice(platforms) if random.random() > 0.05 else None
    
    def generate_os_type(self) -> str:
        """生成操作系统类型"""
        os_types = ['iOS', 'Android', 'Windows', 'MacOS', 'Linux']
        return random.choice(os_types) if random.random() > 0.05 else None
    
    def generate_app_version(self) -> str:
        """生成APP版本"""
        if random.random() > 0.05:
            major = random.randint(1, 5)
            minor = random.randint(0, 20)
            patch = random.randint(0, 30)
            return f"{major}.{minor}.{patch}"
        return None
    
    def generate_network_type(self) -> str:
        """生成网络类型"""
        network_types = ['4G', '5G', 'WiFi', '3G', 'Ethernet']
        return random.choice(network_types) if random.random() > 0.05 else None
    
    def generate_user_level(self) -> int:
        """生成用户等级"""
        return random.randint(1, 10) if random.random() > 0.05 else None
    
    def generate_gender(self) -> int:
        """生成性别 (0:未知, 1:男, 2:女)"""
        return random.randint(0, 2) if random.random() > 0.05 else None
    
    def generate_age(self) -> int:
        """生成年龄"""
        return random.randint(18, 80) if random.random() > 0.05 else None
    
    def generate_vip_flag(self) -> bool:
        """生成VIP标识"""
        return random.choice([True, False]) if random.random() > 0.05 else None
    
    def generate_risk_level(self) -> int:
        """生成风险等级 (0-5)"""
        return random.randint(0, 5) if random.random() > 0.05 else None
    
    def generate_datetime(self, base_date: str = None) -> str:
        """生成日期时间"""
        if random.random() > 0.05:
            if base_date:
                date_obj = datetime.strptime(base_date, '%Y-%m-%d')
            else:
                date_obj = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 750))
            
            time_delta = timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            return (date_obj + time_delta).strftime('%Y-%m-%d %H:%M:%S')
        return None
    
    def generate_decimal(self, max_value: float = 10000.0) -> float:
        """生成decimal类型数据"""
        if random.random() > 0.05:
            return round(random.uniform(0, max_value), 2)
        return None
    
    def generate_count(self, max_count: int = 100) -> int:
        """生成计数"""
        return random.randint(0, max_count) if random.random() > 0.05 else None
    
    def generate_stay_time(self) -> int:
        """生成停留时间(秒)"""
        return random.randint(1, 7200) if random.random() > 0.05 else None
    
    def generate_score(self) -> float:
        """生成评分"""
        return round(random.uniform(0, 100), 2) if random.random() > 0.05 else None
    
    def generate_boolean(self) -> bool:
        """生成布尔值"""
        return random.choice([True, False]) if random.random() > 0.05 else None
    
    def generate_user_tag(self) -> str:
        """生成用户标签"""
        tags = ['新用户', '活跃用户', '沉睡用户', '流失用户', '高价值用户', 
               '普通用户', 'VIP用户', '黑名单用户']
        return random.choice(tags) if random.random() > 0.05 else None
    
    def generate_remark(self) -> str:
        """生成备注"""
        remarks = [
            '正常订单', '优惠券订单', '秒杀订单', '拼团订单', '预售订单',
            '限时抢购', '满减活动', '新人专享', '会员特权', None
        ]
        return random.choice(remarks)
    
    def generate_trace_id(self) -> str:
        """生成追踪ID"""
        if random.random() > 0.05:
            return ''.join(random.choices('0123456789abcdef', k=32))
        return None
    
    def generate_product_id_list(self) -> List[int]:
        """生成商品ID列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 10)
            return [random.randint(10000, 999999) for _ in range(count)]
        return None
    
    def generate_ext_kv_map(self) -> dict:
        """生成扩展KV映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['source', 'medium', 'campaign', 'term', 'content', 
                   'referrer', 'landing_page', 'utm_source']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            return {k: f'value_{random.randint(1, 100)}' for k in selected_keys}
        return None
    
    def generate_user_profile(self) -> dict:
        """生成用户画像(STRUCT类型)"""
        if random.random() > 0.05:
            genders = ['男', '女', '未知']
            return {
                'age': random.randint(18, 80),
                'gender': random.choice(genders),
                'level': random.randint(1, 10)
            }
        return None
    
    def generate_ext_json(self) -> str:
        """生成扩展JSON"""
        if random.random() > 0.05:
            data = {
                'extra_field_1': random.randint(1, 100),
                'extra_field_2': f'value_{random.randint(1, 100)}',
                'extra_field_3': random.choice([True, False])
            }
            return json.dumps(data, ensure_ascii=False)
        return None
    
    def generate_row(self) -> dict:
        """生成一行数据"""
        event_date = self.generate_event_date()
        
        row = {
            'biz_id': self.generate_biz_id(),
            'user_id': self.generate_user_id(),
            'channel_code': self.generate_channel_code(),
            'event_date': event_date,
            'order_id': self.generate_order_id(),
            'product_id': self.generate_product_id(),
            'shop_id': self.generate_shop_id(),
            'category_id': self.generate_category_id(),
            'brand_id': self.generate_brand_id(),
            'device_id': self.generate_device_id(),
            'session_id': self.generate_session_id(),
            'region_code': self.generate_region_code(),
            'city_code': self.generate_city_code(),
            'platform': self.generate_platform(),
            'os_type': self.generate_os_type(),
            'app_version': self.generate_app_version(),
            'network_type': self.generate_network_type(),
            'user_level': self.generate_user_level(),
            'gender': self.generate_gender(),
            'age': self.generate_age(),
            'vip_flag': self.generate_vip_flag(),
            'risk_level': self.generate_risk_level(),
            'event_datetime': self.generate_datetime(event_date),
            'order_datetime': self.generate_datetime(event_date),
            'pay_datetime': self.generate_datetime(event_date),
            'create_time': self.generate_datetime(event_date),
            'update_time': self.generate_datetime(event_date),
            'etl_time': self.generate_datetime(event_date),
            'order_amount': self.generate_decimal(50000),
            'pay_amount': self.generate_decimal(50000),
            'discount_amount': self.generate_decimal(5000),
            'refund_amount': self.generate_decimal(10000),
            'cost_amount': self.generate_decimal(30000),
            'profit_amount': self.generate_decimal(20000),
            'item_cnt': self.generate_count(50),
            'sku_cnt': self.generate_count(100),
            'order_cnt': self.generate_count(20),
            'refund_cnt': self.generate_count(10),
            'stay_time': self.generate_stay_time(),
            'score': self.generate_score(),
            'credit_score': self.generate_score(),
            'risk_score': self.generate_score(),
            'is_new_user': self.generate_boolean(),
            'user_tag': self.generate_user_tag(),
            'remark': self.generate_remark(),
            'trace_id': self.generate_trace_id(),
            'product_id_list': self.generate_product_id_list(),
            'ext_kv_map': self.generate_ext_kv_map(),
            'user_profile': self.generate_user_profile(),
            'ext_json': self.generate_ext_json(),
        }
        
        return row


def generate_batch(args):
    """生成一批数据 (用于多进程)"""
    batch_size, worker_id = args
    generator = DataGenerator(seed=worker_id)
    
    print(f"Worker {worker_id}: 开始生成 {batch_size} 行数据...")
    
    data = []
    for i in range(batch_size):
        if (i + 1) % 10000 == 0:
            print(f"Worker {worker_id}: 已生成 {i + 1} 行")
        data.append(generator.generate_row())
    
    print(f"Worker {worker_id}: 完成生成 {batch_size} 行数据")
    return data


def save_to_parquet(data: List[dict], output_file: str, use_native_types: bool = True, silent: bool = False):
    """保存数据到parquet文件
    
    Args:
        data: 要保存的数据列表
        output_file: 输出文件路径
        use_native_types: 是否使用原生复杂类型（True=原生类型，False=JSON字符串）
        silent: 是否静默模式（不打印进度信息）
    """
    if not silent:
        print(f"正在将数据转换为DataFrame...")
    df = pd.DataFrame(data)
    
    if use_native_types:
        # 使用原生复杂类型（适用于StarRocks、Spark等）
        if not silent:
            print(f"正在处理复杂数据类型（原生模式）...")
        
        # 定义PyArrow schema以确保正确的数据类型
        schema = pa.schema([
            ('biz_id', pa.int32()),
            ('user_id', pa.int64()),
            ('channel_code', pa.string()),
            ('event_date', pa.string()),
            ('order_id', pa.int64()),
            ('product_id', pa.int64()),
            ('shop_id', pa.int64()),
            ('category_id', pa.int64()),
            ('brand_id', pa.int64()),
            ('device_id', pa.string()),
            ('session_id', pa.string()),
            ('region_code', pa.string()),
            ('city_code', pa.string()),
            ('platform', pa.string()),
            ('os_type', pa.string()),
            ('app_version', pa.string()),
            ('network_type', pa.string()),
            ('user_level', pa.int32()),
            ('gender', pa.int8()),
            ('age', pa.int16()),
            ('vip_flag', pa.bool_()),
            ('risk_level', pa.int8()),
            ('event_datetime', pa.string()),
            ('order_datetime', pa.string()),
            ('pay_datetime', pa.string()),
            ('create_time', pa.string()),
            ('update_time', pa.string()),
            ('etl_time', pa.string()),
            ('order_amount', pa.float64()),
            ('pay_amount', pa.float64()),
            ('discount_amount', pa.float64()),
            ('refund_amount', pa.float64()),
            ('cost_amount', pa.float64()),
            ('profit_amount', pa.float64()),
            ('item_cnt', pa.int32()),
            ('sku_cnt', pa.int32()),
            ('order_cnt', pa.int32()),
            ('refund_cnt', pa.int32()),
            ('stay_time', pa.int64()),
            ('score', pa.float64()),
            ('credit_score', pa.float64()),
            ('risk_score', pa.float64()),
            ('is_new_user', pa.bool_()),
            ('user_tag', pa.string()),
            ('remark', pa.string()),
            ('trace_id', pa.string()),
            ('product_id_list', pa.list_(pa.int64())),
            ('ext_kv_map', pa.map_(pa.string(), pa.string())),  # 原生MAP类型
            ('user_profile', pa.struct([                         # 原生STRUCT类型
                ('age', pa.int32()),
                ('gender', pa.string()),
                ('level', pa.int32())
            ])),
            ('ext_json', pa.string()),
        ])
        
        # 处理ext_json字段（确保是字符串）
        if 'ext_json' in df.columns:
            df['ext_json'] = df['ext_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        # 转换MAP类型: dict -> list of tuples (PyArrow MAP格式要求)
        if 'ext_kv_map' in df.columns:
            df['ext_kv_map'] = df['ext_kv_map'].apply(
                lambda x: [(k, v) for k, v in x.items()] if x is not None else None
            )
        
        # STRUCT和ARRAY类型保持不变，PyArrow会自动处理
        
        if not silent:
            print(f"正在写入parquet文件: {output_file}")
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, output_file, compression='none')
        
    else:
        # 将复杂类型转换为JSON字符串（兼容性模式）
        if not silent:
            print(f"正在处理复杂数据类型（JSON字符串模式）...")
        if 'ext_kv_map' in df.columns:
            df['ext_kv_map'] = df['ext_kv_map'].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
            )
        
        if 'user_profile' in df.columns:
            df['user_profile'] = df['user_profile'].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
            )
        
        # 将list类型也转换为JSON字符串
        if 'product_id_list' in df.columns:
            df['product_id_list'] = df['product_id_list'].apply(
                lambda x: json.dumps(x) if x is not None else None
            )
        
        if not silent:
            print(f"正在写入parquet文件: {output_file}")
        # 让PyArrow自动推断schema
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, compression='none')
    
    if not silent:
        print(f"成功写入 {len(data)} 行数据到 {output_file}")


def parse_size(size_str: str) -> int:
    """解析文件大小字符串，返回字节数
    
    支持的格式: 100MB, 1GB, 500M, 2G 等
    """
    size_str = size_str.strip().upper()
    
    # 提取数字和单位
    import re
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$', size_str)
    if not match:
        raise ValueError(f"无效的大小格式: {size_str}. 请使用如 '100MB' 或 '1GB' 的格式")
    
    number = float(match.group(1))
    unit = match.group(2)
    
    # 转换为字节
    multipliers = {
        'B': 1,
        'KB': 1024,
        'K': 1024,
        'MB': 1024 * 1024,
        'M': 1024 * 1024,
        'GB': 1024 * 1024 * 1024,
        'G': 1024 * 1024 * 1024,
        'TB': 1024 * 1024 * 1024 * 1024,
        'T': 1024 * 1024 * 1024 * 1024,
    }
    
    return int(number * multipliers.get(unit, 1))


def estimate_rows_for_size(target_size_bytes: int, use_native_types: bool = True, max_iterations: int = 3) -> int:
    """估算达到目标文件大小需要的行数（使用迭代方法提高精度）
    
    Args:
        target_size_bytes: 目标文件大小（字节）
        use_native_types: 是否使用原生类型
        max_iterations: 最大迭代次数
    
    Returns:
        估算的行数
    """
    import tempfile
    import os
    
    print(f"正在估算文件大小（迭代方法）...")
    
    # 第一步：生成样本数据（根据目标大小动态调整样本量）
    # 对于大文件使用更大的样本以提高精度
    if target_size_bytes >= 1024 * 1024 * 1024:  # >= 1GB
        sample_size = 20000  # 大文件使用2万行样本
    elif target_size_bytes >= 100 * 1024 * 1024:  # >= 100MB
        sample_size = 10000  # 中等大文件使用1万行样本
    elif target_size_bytes >= 10 * 1024 * 1024:  # >= 10MB
        sample_size = 5000   # 中等文件使用5千行样本
    else:
        sample_size = min(5000, max(1000, int(target_size_bytes / 1000)))  # 小文件动态调整
    
    print(f"使用样本量: {sample_size:,} 行（目标: {target_size_bytes / (1024*1024):.2f} MB）")
    generator = DataGenerator(seed=42)
    sample_data = [generator.generate_row() for _ in range(sample_size)]
    
    # 写入临时文件测量大小
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        tmp_file = tmp.name
    
    try:
        save_to_parquet(sample_data, tmp_file, use_native_types, silent=True)
        sample_file_size = os.path.getsize(tmp_file)
        
        # 计算平均每行大小
        bytes_per_row = sample_file_size / sample_size
        
        # 初始估算行数
        estimated_rows = int(target_size_bytes / bytes_per_row)
        
        print(f"样本文件大小: {sample_file_size / 1024:.2f} KB ({sample_size} 行)")
        print(f"平均每行: {bytes_per_row:.2f} 字节")
        print(f"初始估算: {estimated_rows:,} 行")
        
        # 对于大文件（>100MB），跳过迭代以节省时间
        # 但使用更大的样本量提高估算精度
        if target_size_bytes > 100 * 1024 * 1024:  # >100MB
            # 对于超大文件，可以做一个小的调整系数（基于经验值）
            # 由于样本量已经较大（10k-20k行），估算已经比较准确
            # 但考虑到数据分布的随机性，稍微增加一点行数以补偿可能的偏差
            if target_size_bytes >= 1024 * 1024 * 1024:  # >= 1GB
                # 对于1GB+文件，增加5%的行数以补偿可能的偏差
                adjusted_rows = int(estimated_rows * 1.05)
                print(f"目标文件较大（>{target_size_bytes / (1024*1024):.0f}MB），跳过迭代以提高速度")
                print(f"使用样本估算: {estimated_rows:,} 行 → 调整后: {adjusted_rows:,} 行（精度约±5%）")
                return adjusted_rows
            else:
                print(f"目标文件较大（>{target_size_bytes / (1024*1024):.0f}MB），跳过迭代以提高速度")
                print(f"使用样本估算: {estimated_rows:,} 行（精度约±5-10%）")
                return estimated_rows
        
        # 对于中等文件（10MB-100MB），使用小规模迭代
        if target_size_bytes > 10 * 1024 * 1024:  # >10MB
            # 只生成目标大小的10%来测试，然后推算
            test_size_ratio = 0.1
            test_rows = max(1000, int(estimated_rows * test_size_ratio))
            test_data = [generator.generate_row() for _ in range(test_rows)]
            test_file = tmp_file + "_test"
            
            try:
                save_to_parquet(test_data, test_file, use_native_types, silent=True)
                test_file_size = os.path.getsize(test_file)
                
                # 根据测试文件大小推算实际行数
                if test_file_size > 0:
                    actual_bytes_per_row = test_file_size / test_rows
                    adjusted_rows = int(target_size_bytes / actual_bytes_per_row)
                    
                    test_size_mb = test_file_size / (1024 * 1024)
                    target_size_mb = target_size_bytes / (1024 * 1024)
                    print(f"快速测试: 生成 {test_rows:,} 行 ({test_size_mb:.2f}MB), "
                          f"推算需要: {adjusted_rows:,} 行 (目标: {target_size_mb:.2f}MB)")
                    return adjusted_rows
            finally:
                if os.path.exists(test_file):
                    os.remove(test_file)
            
            # 如果快速测试失败，使用初始估算
            return estimated_rows
        
        # 对于小文件（<10MB），使用完整迭代以提高精度
        iterations = max_iterations if target_size_bytes >= 100 * 1024 else min(2, max_iterations)
        current_rows = estimated_rows
        
        for iteration in range(iterations):
            # 生成测试文件
            test_data = [generator.generate_row() for _ in range(current_rows)]
            test_file = tmp_file + f"_test_{iteration}"
            
            try:
                save_to_parquet(test_data, test_file, use_native_types, silent=True)
                actual_size = os.path.getsize(test_file)
                
                # 计算偏差
                size_diff = actual_size - target_size_bytes
                size_diff_percent = (size_diff / target_size_bytes) * 100
                
                print(f"迭代 {iteration + 1}: 生成 {current_rows:,} 行, 实际大小: {actual_size / 1024:.2f} KB, "
                      f"偏差: {size_diff_percent:+.1f}%")
                
                # 如果偏差在5%以内，认为足够精确
                if abs(size_diff_percent) <= 5:
                    print(f"✓ 精度满足要求（偏差 {size_diff_percent:+.1f}%）")
                    return current_rows
                
                # 根据实际大小调整行数
                if actual_size > 0:
                    adjustment_factor = target_size_bytes / actual_size
                    current_rows = int(current_rows * adjustment_factor)
                    # 确保至少生成1行
                    current_rows = max(1, current_rows)
                else:
                    break
                    
            finally:
                if os.path.exists(test_file):
                    os.remove(test_file)
        
        print(f"使用最终估算: {current_rows:,} 行")
        return current_rows
        
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='生成测试数据并保存为parquet格式')
    parser.add_argument('-n', '--rows', type=int, default=None,
                       help='生成的数据行数 (与--size二选一)')
    parser.add_argument('-s', '--size', type=str, default=None,
                       help='目标文件大小 (如: 100MB, 1GB, 500M) (与--rows二选一)')
    parser.add_argument('-f', '--files', type=int, default=1,
                       help='生成文件数量 (默认: 1)')
    parser.add_argument('--per-file', action='store_true',
                       help='--size参数表示每个文件的大小，而不是总大小 (仅在同时使用-s和-f时有效)')
    parser.add_argument('-p', '--processes', type=int, default=None,
                       help='使用的进程数 (默认: CPU核心数)')
    parser.add_argument('-o', '--output', type=str, default='test_data.parquet',
                       help='输出文件名，多文件时自动添加序号 (默认: test_data.parquet)')
    parser.add_argument('--format', type=str, choices=['native', 'json'], default='native',
                       help='复杂类型格式: native=原生类型(适合StarRocks/Spark), json=JSON字符串(兼容性好) (默认: native)')
    
    args = parser.parse_args()
    
    # 验证参数
    num_files = args.files
    if num_files < 1:
        parser.error("文件数量必须至少为1")
    
    # 检查 --per-file 标志的使用
    if args.per_file and args.size is None:
        parser.error("--per-file 只能与 --size 参数一起使用")
    
    if args.rows is None and args.size is None:
        # 默认生成10000行
        total_rows = 10000
    elif args.rows is not None and args.size is not None:
        parser.error("--rows 和 --size 不能同时指定，请选择其中一个")
    elif args.size is not None:
        # 根据目标大小估算行数
        try:
            target_bytes = parse_size(args.size)
            use_native_types = (args.format == 'native')
            
            if args.per_file:
                # --per-file: 每个文件的大小
                rows_per_file_estimated = estimate_rows_for_size(target_bytes, use_native_types)
                total_rows = rows_per_file_estimated * num_files
            else:
                # 默认: 所有文件的总大小
                total_rows = estimate_rows_for_size(target_bytes, use_native_types)
        except Exception as e:
            parser.error(f"处理文件大小参数时出错: {e}")
    else:
        total_rows = args.rows
    
    num_processes = args.processes or cpu_count()
    output_file = args.output
    use_native_types = (args.format == 'native')
    
    # 计算每个文件的行数
    rows_per_file = total_rows // num_files
    remainder_rows = total_rows % num_files
    
    print(f"=" * 60)
    print(f"开始生成测试数据")
    if args.size and args.per_file:
        print(f"每个文件大小: {args.size}")
        print(f"文件数量: {num_files}")
        print(f"总大小: 约 {args.size} × {num_files}")
    else:
        print(f"总行数: {total_rows:,}")
        print(f"文件数量: {num_files}")
        if num_files > 1:
            print(f"每个文件约: {rows_per_file:,} 行")
    print(f"进程数: {num_processes}")
    print(f"输出文件: {output_file if num_files == 1 else output_file.replace('.parquet', '_*.parquet')}")
    print(f"复杂类型格式: {'原生类型 (适合StarRocks/Spark)' if use_native_types else 'JSON字符串 (兼容性模式)'}")
    print(f"=" * 60)
    
    start_time = datetime.now()
    
    # 生成多个文件
    import os
    
    for file_idx in range(num_files):
        # 计算当前文件的行数
        current_rows = rows_per_file + (1 if file_idx < remainder_rows else 0)
        
        # 生成文件名
        if num_files == 1:
            current_output = output_file
        else:
            # 在文件名中插入序号
            base_name = os.path.splitext(output_file)[0]
            ext = os.path.splitext(output_file)[1] or '.parquet'
            current_output = f"{base_name}_part_{file_idx + 1:03d}{ext}"
        
        print(f"\n{'=' * 60}")
        print(f"生成文件 {file_idx + 1}/{num_files}: {current_output}")
        print(f"行数: {current_rows:,}")
        print(f"{'=' * 60}")
        
        # 计算每个进程要生成的数据量
        rows_per_process = current_rows // num_processes
        remainder = current_rows % num_processes
        
        tasks = []
        for i in range(num_processes):
            batch_size = rows_per_process + (1 if i < remainder else 0)
            # 使用不同的seed确保每个文件数据不同
            seed = file_idx * 1000 + i
            tasks.append((batch_size, seed))
        
        # 使用多进程生成数据
        if num_processes > 1:
            print(f"\n使用 {num_processes} 个进程并行生成数据...")
            with Pool(num_processes) as pool:
                results = pool.map(generate_batch, tasks)
        else:
            print(f"\n使用单进程生成数据...")
            results = [generate_batch(tasks[0])]
        
        # 合并所有数据
        print(f"\n合并所有数据...")
        all_data = []
        for result in results:
            all_data.extend(result)
        
        # 保存到parquet文件
        save_to_parquet(all_data, current_output, use_native_types)
    
    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    
    print(f"\n" + "=" * 60)
    print(f"全部完成!")
    print(f"生成文件数: {num_files}")
    print(f"总行数: {total_rows:,}")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    print(f"生成速度: {total_rows / elapsed_time:.0f} 行/秒")
    print(f"=" * 60)


if __name__ == '__main__':
    main()
