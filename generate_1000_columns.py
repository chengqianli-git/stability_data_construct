#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成1000列测试数据的脚本
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
    
    def generate_user_id(self) -> int:
        """生成用户ID"""
        return random.randint(100000, 999999999)
    
    def generate_order_id(self) -> int:
        """生成订单ID"""
        return random.randint(1000000000, 9999999999) if random.random() > 0.05 else None
    
    def generate_channel_code(self) -> str:
        """生成渠道代码"""
        channels = ['APP001', 'WEB001', 'H5001', 'API001', 'WX001', 
                   'ALI001', 'JD001', 'PDD001', 'MINI001', 'PC001']
        return random.choice(channels)
    
    def generate_event_date(self) -> str:
        """生成事件日期"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2026, 1, 28)
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime('%Y-%m-%d')
    
    def generate_int_col(self) -> int:
        """生成INT类型数据"""
        return random.randint(-2147483648, 2147483647) if random.random() > 0.05 else None
    
    def generate_bigint_col(self) -> int:
        """生成BIGINT类型数据"""
        return random.randint(-9223372036854775808, 9223372036854775807) if random.random() > 0.05 else None
    
    def generate_decimal_col(self) -> float:
        """生成DECIMAL类型数据"""
        return round(random.uniform(-99999999999999.99, 99999999999999.99), 2) if random.random() > 0.05 else None
    
    def generate_float_col(self) -> float:
        """生成FLOAT类型数据"""
        return random.uniform(-3.4e38, 3.4e38) if random.random() > 0.05 else None
    
    def generate_double_col(self) -> float:
        """生成DOUBLE类型数据"""
        # 使用更安全的范围，避免生成 inf 值
        # Python float 最大值约为 1.7976931348623157e+308
        # 但 random.uniform 在接近最大值时可能产生 inf
        # 使用 1e200 作为上限更安全，这个值已经非常大
        return random.uniform(-1e200, 1e200) if random.random() > 0.05 else None
    
    def generate_boolean_col(self) -> bool:
        """生成BOOLEAN类型数据"""
        return random.choice([True, False]) if random.random() > 0.05 else None
    
    def generate_char_col(self) -> str:
        """生成CHAR(10)类型数据"""
        if random.random() > 0.05:
            return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=10))
        return None
    
    def generate_varchar_col(self) -> str:
        """生成VARCHAR(20)类型数据"""
        if random.random() > 0.05:
            length = random.randint(1, 20)
            return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=length))
        return None
    
    def generate_string_col(self) -> str:
        """生成STRING类型数据"""
        if random.random() > 0.05:
            length = random.randint(1, 100)
            return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ', k=length))
        return None
    
    def generate_binary_col(self) -> bytes:
        """生成BINARY类型数据"""
        if random.random() > 0.05:
            length = random.randint(1, 100)
            return bytes([random.randint(0, 255) for _ in range(length)])
        return None
    
    def generate_date_col(self) -> str:
        """生成DATE类型数据"""
        if random.random() > 0.05:
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2030, 12, 31)
            time_between = end_date - start_date
            days_between = time_between.days
            random_days = random.randint(0, days_between)
            random_date = start_date + timedelta(days=random_days)
            return random_date.strftime('%Y-%m-%d')
        return None
    
    def generate_datetime_col(self) -> str:
        """生成DATETIME类型数据"""
        if random.random() > 0.05:
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2030, 12, 31)
            time_between = end_date - start_date
            days_between = time_between.days
            random_days = random.randint(0, days_between)
            random_date = start_date + timedelta(days=random_days)
            
            time_delta = timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            return (random_date + time_delta).strftime('%Y-%m-%d %H:%M:%S')
        return None
    
    def generate_product_id_list(self) -> List[int]:
        """生成商品ID列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 10)
            return [random.randint(10000, 999999) for _ in range(count)]
        return None
    
    def generate_tag_list(self) -> List[str]:
        """生成标签列表(ARRAY类型)"""
        if random.random() > 0.05:
            tags = ['VIP', 'NEW', 'ACTIVE', 'INACTIVE', 'PREMIUM', 'BASIC', 'GOLD', 'SILVER']
            count = random.randint(1, 5)
            return random.sample(tags, count)
        return None
    
    def generate_event_ids(self) -> List[int]:
        """生成事件ID列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 20)
            return [random.randint(1, 100000) for _ in range(count)]
        return None
    
    def generate_score_list(self) -> List[float]:
        """生成评分列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 10)
            return [round(random.uniform(0, 100), 2) for _ in range(count)]
        return None
    
    def generate_metrics_array(self) -> List[float]:
        """生成指标数组(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 15)
            return [round(random.uniform(-10000, 10000), 2) for _ in range(count)]
        return None
    
    def generate_click_list(self) -> List[int]:
        """生成点击列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 30)
            return [random.randint(0, 1000) for _ in range(count)]
        return None
    
    def generate_view_list(self) -> List[int]:
        """生成浏览列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 30)
            return [random.randint(0, 10000) for _ in range(count)]
        return None
    
    def generate_revenue_list(self) -> List[float]:
        """生成收入列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 20)
            return [round(random.uniform(0, 100000), 2) for _ in range(count)]
        return None
    
    def generate_rating_list(self) -> List[float]:
        """生成评分列表(ARRAY类型)"""
        if random.random() > 0.05:
            count = random.randint(1, 10)
            return [round(random.uniform(1, 5), 1) for _ in range(count)]
        return None
    
    def generate_comment_list(self) -> List[str]:
        """生成评论列表(ARRAY类型)"""
        if random.random() > 0.05:
            comments = ['Great!', 'Good', 'Bad', 'Excellent', 'Poor', 'Average', 'Nice', 'Awesome']
            count = random.randint(1, 5)
            return random.sample(comments, count)
        return None
    
    def generate_amount_map(self) -> dict:
        """生成金额映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['total', 'discount', 'tax', 'shipping', 'handling']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            return {k: round(random.uniform(0, 10000), 2) for k in selected_keys}
        return None
    
    def generate_properties_map(self) -> dict:
        """生成属性映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['color', 'size', 'weight', 'material', 'brand']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            values = ['red', 'blue', 'green', 'large', 'small', 'medium', 'cotton', 'silk', 'nike', 'adidas']
            return {k: random.choice(values) for k in selected_keys}
        return None
    
    def generate_metrics_map(self) -> dict:
        """生成指标映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['ctr', 'cvr', 'roi', 'cpa', 'cpc']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            return {k: round(random.uniform(0, 100), 4) for k in selected_keys}
        return None
    
    def generate_status_map(self) -> dict:
        """生成状态映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['order', 'payment', 'shipping', 'delivery', 'return']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            values = [0, 1, 2, 3, 4]
            return {k: random.choice(values) for k in selected_keys}
        return None
    
    def generate_extra_map(self) -> dict:
        """生成扩展映射(MAP类型)"""
        if random.random() > 0.05:
            keys = ['key1', 'key2', 'key3', 'key4', 'key5']
            count = random.randint(1, 5)
            selected_keys = random.sample(keys, count)
            return {k: f'value_{random.randint(1, 100)}' for k in selected_keys}
        return None
    
    def generate_user_profile(self) -> dict:
        """生成用户画像(STRUCT类型)"""
        if random.random() > 0.05:
            return {
                'age': random.randint(18, 80),
                'gender': random.choice(['male', 'female', 'other']),
                'level': random.randint(1, 10)
            }
        return None
    
    def generate_order_detail(self) -> dict:
        """生成订单详情(STRUCT类型)"""
        if random.random() > 0.05:
            return {
                'product_id': random.randint(10000, 999999),
                'quantity': random.randint(1, 100),
                'price': round(random.uniform(1, 10000), 2)
            }
        return None
    
    def generate_address_info(self) -> dict:
        """生成地址信息(STRUCT类型)"""
        if random.random() > 0.05:
            provinces = ['Beijing', 'Shanghai', 'Guangdong', 'Zhejiang', 'Jiangsu']
            cities = ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou', 'Nanjing']
            return {
                'province': random.choice(provinces),
                'city': random.choice(cities),
                'zipcode': str(random.randint(100000, 999999))
            }
        return None
    
    def generate_shipping_info(self) -> dict:
        """生成配送信息(STRUCT类型)"""
        if random.random() > 0.05:
            carriers = ['FedEx', 'UPS', 'DHL', 'SF', 'EMS']
            return {
                'carrier': random.choice(carriers),
                'cost': round(random.uniform(0, 500), 2)
            }
        return None
    
    def generate_payment_info(self) -> dict:
        """生成支付信息(STRUCT类型)"""
        if random.random() > 0.05:
            methods = ['credit_card', 'debit_card', 'paypal', 'alipay', 'wechat_pay']
            base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 750))
            time_delta = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
            return {
                'method': random.choice(methods),
                'amount': round(random.uniform(1, 100000), 2),
                'time': (base_date + time_delta).strftime('%Y-%m-%d %H:%M:%S')
            }
        return None
    
    def generate_event_info(self) -> dict:
        """生成事件信息(STRUCT类型)"""
        if random.random() > 0.05:
            event_types = ['click', 'view', 'purchase', 'add_to_cart', 'search']
            base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 750))
            time_delta = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
            return {
                'event_type': random.choice(event_types),
                'event_time': (base_date + time_delta).strftime('%Y-%m-%d %H:%M:%S'),
                'score': round(random.uniform(0, 100), 2)
            }
        return None
    
    def generate_behavior_info(self) -> dict:
        """生成行为信息(STRUCT类型)"""
        if random.random() > 0.05:
            return {
                'clicks': random.randint(0, 10000),
                'views': random.randint(0, 50000),
                'dwell_time': random.randint(0, 36000)
            }
        return None
    
    def generate_device_info(self) -> dict:
        """生成设备信息(STRUCT类型)"""
        if random.random() > 0.05:
            device_types = ['mobile', 'tablet', 'desktop', 'laptop']
            os_list = ['iOS', 'Android', 'Windows', 'MacOS', 'Linux']
            browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
            return {
                'device_type': random.choice(device_types),
                'os': random.choice(os_list),
                'browser': random.choice(browsers)
            }
        return None
    
    def generate_session_info(self) -> dict:
        """生成会话信息(STRUCT类型)"""
        if random.random() > 0.05:
            base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 750))
            start_time = base_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
            end_time = start_time + timedelta(minutes=random.randint(1, 120))
            return {
                'session_id': ''.join(random.choices('0123456789abcdef', k=32)),
                'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S')
            }
        return None
    
    def generate_location_info(self) -> dict:
        """生成位置信息(STRUCT类型)"""
        if random.random() > 0.05:
            cities = ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou']
            return {
                'lat': round(random.uniform(-90, 90), 6),
                'lon': round(random.uniform(-180, 180), 6),
                'city': random.choice(cities)
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
        """生成一行数据（1000列）"""
        event_date = self.generate_event_date()
        
        row = {
            'user_id': self.generate_user_id(),
            'order_id': self.generate_order_id(),
            'channel_code': self.generate_channel_code(),
            'event_date': event_date,
        }
        
        for i in range(1, 201):
            row[f'int_col_{i}'] = self.generate_int_col()
        
        for i in range(1, 101):
            row[f'big_int_col_{i}'] = self.generate_bigint_col()
        
        for i in range(1, 51):
            row[f'decimal_col_{i}'] = self.generate_decimal_col()
        
        for i in range(1, 31):
            row[f'float_col_{i}'] = self.generate_float_col()
        
        for i in range(1, 31):
            row[f'double_col_{i}'] = self.generate_double_col()
        
        for i in range(1, 41):
            row[f'boolean_col_{i}'] = self.generate_boolean_col()
        
        for i in range(1, 151):
            row[f'char_col_{i}'] = self.generate_char_col()
        
        for i in range(1, 201):
            row[f'varchar_col_{i}'] = self.generate_varchar_col()
        
        for i in range(1, 101):
            row[f'string_col_{i}'] = self.generate_string_col()
        
        for i in range(1, 51):
            row[f'binary_col_{i}'] = self.generate_binary_col()
        
        for i in range(1, 9):
            row[f'date_col_{i}'] = self.generate_date_col()
        
        for i in range(1, 9):
            row[f'datetime_col_{i}'] = self.generate_datetime_col()
        
        row['product_id_list'] = self.generate_product_id_list()
        row['tag_list'] = self.generate_tag_list()
        row['event_ids'] = self.generate_event_ids()
        row['score_list'] = self.generate_score_list()
        row['metrics_array'] = self.generate_metrics_array()
        row['click_list'] = self.generate_click_list()
        row['view_list'] = self.generate_view_list()
        row['revenue_list'] = self.generate_revenue_list()
        row['rating_list'] = self.generate_rating_list()
        row['comment_list'] = self.generate_comment_list()
        
        row['amount_map'] = self.generate_amount_map()
        row['properties_map'] = self.generate_properties_map()
        row['metrics_map'] = self.generate_metrics_map()
        row['status_map'] = self.generate_status_map()
        row['extra_map'] = self.generate_extra_map()
        
        row['user_profile'] = self.generate_user_profile()
        row['order_detail'] = self.generate_order_detail()
        row['address_info'] = self.generate_address_info()
        row['shipping_info'] = self.generate_shipping_info()
        row['payment_info'] = self.generate_payment_info()
        row['event_info'] = self.generate_event_info()
        row['behavior_info'] = self.generate_behavior_info()
        row['device_info'] = self.generate_device_info()
        row['session_info'] = self.generate_session_info()
        row['location_info'] = self.generate_location_info()
        
        row['ext_json'] = self.generate_ext_json()
        row['event_json'] = self.generate_ext_json()
        row['log_json'] = self.generate_ext_json()
        row['extra_json'] = self.generate_ext_json()
        row['temp_json'] = self.generate_ext_json()
        
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
        if not silent:
            print(f"正在处理复杂数据类型（原生模式）...")
        
        fields = [
            ('user_id', pa.int64()),
            ('order_id', pa.int64()),
            ('channel_code', pa.string()),
            ('event_date', pa.string()),
        ]
        
        for i in range(1, 201):
            fields.append((f'int_col_{i}', pa.int32()))
        
        for i in range(1, 101):
            fields.append((f'big_int_col_{i}', pa.int64()))
        
        for i in range(1, 51):
            fields.append((f'decimal_col_{i}', pa.float64()))
        
        for i in range(1, 31):
            fields.append((f'float_col_{i}', pa.float32()))
        
        for i in range(1, 31):
            fields.append((f'double_col_{i}', pa.float64()))
        
        for i in range(1, 41):
            fields.append((f'boolean_col_{i}', pa.bool_()))
        
        for i in range(1, 151):
            fields.append((f'char_col_{i}', pa.string()))
        
        for i in range(1, 201):
            fields.append((f'varchar_col_{i}', pa.string()))
        
        for i in range(1, 101):
            fields.append((f'string_col_{i}', pa.string()))
        
        for i in range(1, 51):
            fields.append((f'binary_col_{i}', pa.binary()))
        
        for i in range(1, 9):
            fields.append((f'date_col_{i}', pa.string()))
        
        for i in range(1, 9):
            fields.append((f'datetime_col_{i}', pa.string()))
        
        fields.append(('product_id_list', pa.list_(pa.int64())))
        fields.append(('tag_list', pa.list_(pa.string())))
        fields.append(('event_ids', pa.list_(pa.int32())))
        fields.append(('score_list', pa.list_(pa.float32())))
        fields.append(('metrics_array', pa.list_(pa.float64())))
        fields.append(('click_list', pa.list_(pa.int32())))
        fields.append(('view_list', pa.list_(pa.int32())))
        fields.append(('revenue_list', pa.list_(pa.float64())))
        fields.append(('rating_list', pa.list_(pa.float32())))
        fields.append(('comment_list', pa.list_(pa.string())))
        
        fields.append(('amount_map', pa.map_(pa.string(), pa.float64())))
        fields.append(('properties_map', pa.map_(pa.string(), pa.string())))
        fields.append(('metrics_map', pa.map_(pa.string(), pa.float32())))
        fields.append(('status_map', pa.map_(pa.string(), pa.int32())))
        fields.append(('extra_map', pa.map_(pa.string(), pa.string())))
        
        fields.append(('user_profile', pa.struct([
            ('age', pa.int32()),
            ('gender', pa.string()),
            ('level', pa.int32())
        ])))
        fields.append(('order_detail', pa.struct([
            ('product_id', pa.int64()),
            ('quantity', pa.int32()),
            ('price', pa.float64())
        ])))
        fields.append(('address_info', pa.struct([
            ('province', pa.string()),
            ('city', pa.string()),
            ('zipcode', pa.string())
        ])))
        fields.append(('shipping_info', pa.struct([
            ('carrier', pa.string()),
            ('cost', pa.float64())
        ])))
        fields.append(('payment_info', pa.struct([
            ('method', pa.string()),
            ('amount', pa.float64()),
            ('time', pa.string())
        ])))
        fields.append(('event_info', pa.struct([
            ('event_type', pa.string()),
            ('event_time', pa.string()),
            ('score', pa.float32())
        ])))
        fields.append(('behavior_info', pa.struct([
            ('clicks', pa.int32()),
            ('views', pa.int32()),
            ('dwell_time', pa.int32())
        ])))
        fields.append(('device_info', pa.struct([
            ('device_type', pa.string()),
            ('os', pa.string()),
            ('browser', pa.string())
        ])))
        fields.append(('session_info', pa.struct([
            ('session_id', pa.string()),
            ('start_time', pa.string()),
            ('end_time', pa.string())
        ])))
        fields.append(('location_info', pa.struct([
            ('lat', pa.float64()),
            ('lon', pa.float64()),
            ('city', pa.string())
        ])))
        
        fields.append(('ext_json', pa.string()))
        fields.append(('event_json', pa.string()))
        fields.append(('log_json', pa.string()))
        fields.append(('extra_json', pa.string()))
        fields.append(('temp_json', pa.string()))
        
        schema = pa.schema(fields)
        
        if 'ext_json' in df.columns:
            df['ext_json'] = df['ext_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        if 'event_json' in df.columns:
            df['event_json'] = df['event_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        if 'log_json' in df.columns:
            df['log_json'] = df['log_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        if 'extra_json' in df.columns:
            df['extra_json'] = df['extra_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        if 'temp_json' in df.columns:
            df['temp_json'] = df['temp_json'].apply(
                lambda x: x if isinstance(x, str) or x is None else json.dumps(x, ensure_ascii=False)
            )
        
        map_columns = ['amount_map', 'properties_map', 'metrics_map', 'status_map', 'extra_map']
        for col in map_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: [(k, v) for k, v in x.items()] if x is not None else None
                )
        
        if not silent:
            print(f"正在写入parquet文件: {output_file}")
        table = pa.Table.from_pandas(df, schema=schema)
        pq.write_table(table, output_file, compression='none')
        
    else:
        if not silent:
            print(f"正在处理复杂数据类型（JSON字符串模式）...")
        
        map_columns = ['amount_map', 'properties_map', 'metrics_map', 'status_map', 'extra_map']
        for col in map_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )
        
        struct_columns = ['user_profile', 'order_detail', 'address_info', 'shipping_info', 
                         'payment_info', 'event_info', 'behavior_info', 'device_info', 
                         'session_info', 'location_info']
        for col in struct_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )
        
        list_columns = ['product_id_list', 'tag_list', 'event_ids', 'score_list', 'metrics_array',
                       'click_list', 'view_list', 'revenue_list', 'rating_list', 'comment_list']
        for col in list_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if x is not None else None
                )
        
        if not silent:
            print(f"正在写入parquet文件: {output_file}")
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_file, compression='none')
    
    if not silent:
        print(f"成功写入 {len(data)} 行数据到 {output_file}")


def parse_size(size_str: str) -> int:
    """解析文件大小字符串，返回字节数
    
    支持的格式: 100MB, 1GB, 500M, 2G 等
    """
    size_str = size_str.strip().upper()
    
    import re
    match = re.match(r'^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$', size_str)
    if not match:
        raise ValueError(f"无效的大小格式: {size_str}. 请使用如 '100MB' 或 '1GB' 的格式")
    
    number = float(match.group(1))
    unit = match.group(2)
    
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


def estimate_rows_for_size(target_size_bytes: int, use_native_types: bool = True) -> int:
    """估算达到目标文件大小需要的行数"""
    import tempfile
    import os
    
    print(f"正在估算文件大小...")
    
    if target_size_bytes >= 1024 * 1024 * 1024:
        sample_size = 5000
    elif target_size_bytes >= 100 * 1024 * 1024:
        sample_size = 3000
    elif target_size_bytes >= 10 * 1024 * 1024:
        sample_size = 2000
    else:
        sample_size = min(1000, max(500, int(target_size_bytes / 10000)))
    
    print(f"使用样本量: {sample_size:,} 行")
    generator = DataGenerator(seed=42)
    sample_data = [generator.generate_row() for _ in range(sample_size)]
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        tmp_file = tmp.name
    
    try:
        save_to_parquet(sample_data, tmp_file, use_native_types, silent=True)
        sample_file_size = os.path.getsize(tmp_file)
        
        bytes_per_row = sample_file_size / sample_size
        estimated_rows = int(target_size_bytes / bytes_per_row)
        
        print(f"样本文件大小: {sample_file_size / 1024:.2f} KB ({sample_size} 行)")
        print(f"平均每行: {bytes_per_row:.2f} 字节")
        print(f"估算需要: {estimated_rows:,} 行")
        
        return estimated_rows
        
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='生成1000列测试数据并保存为parquet格式')
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
    parser.add_argument('-o', '--output', type=str, default='test_data_1000cols.parquet',
                       help='输出文件名，多文件时自动添加序号 (默认: test_data_1000cols.parquet)')
    parser.add_argument('--format', type=str, choices=['native', 'json'], default='native',
                       help='复杂类型格式: native=原生类型(适合StarRocks/Spark), json=JSON字符串(兼容性好) (默认: native)')
    
    args = parser.parse_args()
    
    num_files = args.files
    if num_files < 1:
        parser.error("文件数量必须至少为1")
    
    if args.per_file and args.size is None:
        parser.error("--per-file 只能与 --size 参数一起使用")
    
    if args.rows is None and args.size is None:
        total_rows = 10000
    elif args.rows is not None and args.size is not None:
        parser.error("--rows 和 --size 不能同时指定，请选择其中一个")
    elif args.size is not None:
        try:
            target_bytes = parse_size(args.size)
            use_native_types = (args.format == 'native')
            
            if args.per_file:
                rows_per_file_estimated = estimate_rows_for_size(target_bytes, use_native_types)
                total_rows = rows_per_file_estimated * num_files
            else:
                total_rows = estimate_rows_for_size(target_bytes, use_native_types)
        except Exception as e:
            parser.error(f"处理文件大小参数时出错: {e}")
    else:
        total_rows = args.rows
    
    num_processes = args.processes or cpu_count()
    output_file = args.output
    use_native_types = (args.format == 'native')
    
    rows_per_file = total_rows // num_files
    remainder_rows = total_rows % num_files
    
    print(f"=" * 60)
    print(f"开始生成1000列测试数据")
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
    
    import os
    
    for file_idx in range(num_files):
        current_rows = rows_per_file + (1 if file_idx < remainder_rows else 0)
        
        if num_files == 1:
            current_output = output_file
        else:
            base_name = os.path.splitext(output_file)[0]
            ext = os.path.splitext(output_file)[1] or '.parquet'
            current_output = f"{base_name}_part_{file_idx + 1:03d}{ext}"
        
        print(f"\n{'=' * 60}")
        print(f"生成文件 {file_idx + 1}/{num_files}: {current_output}")
        print(f"行数: {current_rows:,}")
        print(f"{'=' * 60}")
        
        rows_per_process = current_rows // num_processes
        remainder = current_rows % num_processes
        
        tasks = []
        for i in range(num_processes):
            batch_size = rows_per_process + (1 if i < remainder else 0)
            seed = file_idx * 1000 + i
            tasks.append((batch_size, seed))
        
        if num_processes > 1:
            print(f"\n使用 {num_processes} 个进程并行生成数据...")
            with Pool(num_processes) as pool:
                results = pool.map(generate_batch, tasks)
        else:
            print(f"\n使用单进程生成数据...")
            results = [generate_batch(tasks[0])]
        
        print(f"\n合并所有数据...")
        all_data = []
        for result in results:
            all_data.extend(result)
        
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
