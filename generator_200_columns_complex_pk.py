#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据生成脚本 - 用于生成符合 primary_key_optimized 表结构的测试数据

表结构说明:
- 主键列: 5列
- 数值类型: 90列  
- 字符串类型: 70列
- 日期时间类型: 20列
- 半结构化类型: 15列 (5 JSON + 4 ARRAY + 2 MAP + 4 STRUCT)
- 总计: 200列
"""

import argparse
import csv
import json
import os
import random
import string
import uuid
import threading
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, List, Dict
from faker import Faker

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

# 延迟初始化 Faker（在子进程中按需初始化）
fake_cn = None
fake_en = None

def _init_faker():
    global fake_cn, fake_en
    if fake_cn is None:
        fake_cn = Faker('zh_CN')
        fake_en = Faker('en_US')


class PrimaryDataGenerator:
    """主键表数据生成器"""
    
    # 类级别预生成数据池（所有实例共享）
    _data_pool = None
    _pool_size = 5000  # 增大到 5000
    
    @classmethod
    def _init_data_pool(cls):
        """预生成常用数据池，避免重复调用 Faker"""
        if cls._data_pool is not None:
            return
        _init_faker()
        cls._data_pool = {
            'names': [fake_en.name() for _ in range(cls._pool_size)],
            'emails': [fake_en.email() for _ in range(cls._pool_size)],
            'phones': [fake_en.phone_number()[:20] for _ in range(cls._pool_size)],
            'companies': [fake_en.company()[:200] for _ in range(cls._pool_size)],
            'addresses': [fake_en.address()[:500] for _ in range(cls._pool_size)],
            'streets': [fake_en.street_address()[:200] for _ in range(cls._pool_size)],
            'states': [fake_en.state() for _ in range(cls._pool_size)],
            'postcodes': [fake_en.postcode() for _ in range(cls._pool_size)],
            'words': [fake_en.word() for _ in range(cls._pool_size)],
            'texts': [fake_en.text(max_nb_chars=100) for _ in range(cls._pool_size)],
            'user_agents': [fake_en.user_agent()[:1000] for _ in range(2000)],
            'ipv4s': [fake_en.ipv4() for _ in range(cls._pool_size)],
            'urls': [fake_en.url()[:500] for _ in range(cls._pool_size)],
            'jobs': [fake_en.job()[:100] for _ in range(2000)],
            'first_names': [fake_en.first_name() for _ in range(2000)],
            'last_names': [fake_en.last_name() for _ in range(2000)],
            'secondary_addr': [fake_en.secondary_address()[:200] for _ in range(2000)],
            'domain_names': [fake_en.domain_name()[:100] for _ in range(2000)],
        }
    
    def __init__(self, null_ratio: float = 0.05, seed: Optional[int] = None):
        self.null_ratio = null_ratio
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        
        self.id_counter = 0
        
        # 初始化数据池
        self._init_data_pool()
        
        # 预定义数据
        self.regions = ['CN', 'US', 'JP', 'KR', 'UK', 'DE', 'FR', 'AU', 'SG', 'IN']
        self.currencies = ['CNY', 'USD', 'EUR', 'GBP', 'JPY', 'KRW', 'AUD', 'SGD']
        self.countries = ['China', 'United States', 'Japan', 'Korea', 'United Kingdom', 'Germany', 'France']
        self.cities = ['Beijing', 'Shanghai', 'New York', 'Tokyo', 'London', 'Berlin', 'Paris', 'Sydney']
        self.brands = ['Apple', 'Samsung', 'Huawei', 'Xiaomi', 'Sony', 'Dell', 'HP', 'Lenovo', 'Nike', 'Adidas']
        self.payment_methods = ['credit_card', 'debit_card', 'alipay', 'wechat_pay', 'paypal', 'bank_transfer']
        self.device_types = ['mobile', 'desktop', 'tablet', 'smart_tv', 'wearable']
        self.os_names = ['Windows', 'macOS', 'iOS', 'Android', 'Linux', 'Chrome OS']
        self.browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
    
    def _pool_choice(self, key: str, add_random_suffix: bool = False) -> str:
        """从预生成池中随机选取
        
        Args:
            key: 数据池键名
            add_random_suffix: 是否添加随机后缀以增加唯一性
        """
        value = random.choice(self._data_pool[key])
        if add_random_suffix:
            # 添加随机数字后缀，增加唯一性
            value = f"{value}_{random.randint(1000, 9999)}"
        return value
    
    def _should_be_null(self, is_key: bool = False) -> bool:
        if is_key:
            return False
        return random.random() < self.null_ratio
    
    def _wrap_null(self, value: Any, is_key: bool = False) -> Any:
        if self._should_be_null(is_key):
            return None
        return value
    
    # ========== 主键列 ==========
    def gen_id(self) -> int:
        self.id_counter += 1
        return self.id_counter
    
    def gen_tenant_id(self) -> int:
        return random.randint(1, 100)
    
    def gen_event_date(self) -> str:
        days_ago = random.randint(0, 365)
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    
    def gen_business_key(self) -> str:
        return f"BK{uuid.uuid4().hex[:12].upper()}"
    
    def gen_region_code(self) -> str:
        return random.choice(self.regions)
    
    # ========== 数值类型 ==========
    def gen_bigint(self, min_v=1, max_v=9999999999) -> Optional[int]:
        return self._wrap_null(random.randint(min_v, max_v))
    
    def gen_int(self, min_v=1, max_v=999999) -> Optional[int]:
        return self._wrap_null(random.randint(min_v, max_v))
    
    def gen_smallint(self, min_v=1, max_v=32000) -> Optional[int]:
        return self._wrap_null(random.randint(min_v, max_v))
    
    def gen_tinyint(self) -> Optional[int]:
        return self._wrap_null(random.randint(-128, 127))
    
    def gen_largeint(self) -> Optional[int]:
        return self._wrap_null(random.randint(1, 99999999999999999))
    
    def gen_decimal(self, precision: int, scale: int, min_v: float = 0, max_v: float = 10000) -> Optional[str]:
        value = round(random.uniform(min_v, max_v), scale)
        return self._wrap_null(f"{value:.{scale}f}")
    
    def gen_float(self, min_v: float = 0, max_v: float = 100) -> Optional[float]:
        return self._wrap_null(round(random.uniform(min_v, max_v), 4))
    
    def gen_double(self, min_v: float = 0, max_v: float = 1000) -> Optional[float]:
        return self._wrap_null(round(random.uniform(min_v, max_v), 6))
    
    def gen_boolean(self) -> Optional[bool]:
        return self._wrap_null(random.choice([True, False]))
    
    def gen_rate(self, max_v: float = 1.0, scale: int = 4) -> Optional[str]:
        return self._wrap_null(f"{round(random.uniform(0, max_v), scale):.{scale}f}")
    
    def gen_amount(self, max_v: float = 100000, scale: int = 2) -> Optional[str]:
        return self._wrap_null(f"{round(random.uniform(0, max_v), scale):.{scale}f}")
    
    def gen_count(self, max_v: int = 100000) -> Optional[int]:
        return self._wrap_null(random.randint(0, max_v))
    
    def gen_score(self, max_v: float = 5.0, scale: int = 2) -> Optional[str]:
        return self._wrap_null(f"{round(random.uniform(0, max_v), scale):.{scale}f}")
    
    # ========== 字符串类型 (使用预生成池加速) ==========
    def gen_char(self, length: int) -> Optional[str]:
        chars = string.ascii_uppercase + string.digits
        return self._wrap_null(''.join(random.choices(chars, k=length)))
    
    def gen_varchar(self, max_len: int) -> Optional[str]:
        return self._wrap_null(self._pool_choice('texts')[:max_len])
    
    def gen_email(self) -> Optional[str]:
        # 邮箱添加随机后缀以提高唯一性
        if self._should_be_null():
            return None
        base_email = self._pool_choice('emails')
        # 在 @ 前插入随机数
        if '@' in base_email:
            parts = base_email.split('@')
            return f"{parts[0]}{random.randint(100, 999)}@{parts[1]}"
        return base_email
    
    def gen_phone(self) -> Optional[str]:
        # 电话号码添加随机数字以提高唯一性
        if self._should_be_null():
            return None
        base_phone = self._pool_choice('phones')
        # 在末尾添加随机数字
        suffix = f"-{random.randint(1000, 9999)}"
        return (base_phone + suffix)[:20]  # 保持长度限制
    
    def gen_name(self) -> Optional[str]:
        # 组合姓名：从池中随机选择名和姓
        if self._should_be_null():
            return None
        # 20% 概率直接使用完整姓名，80% 概率重新组合
        if random.random() < 0.2:
            return self._pool_choice('names')
        else:
            first = self._pool_choice('first_names')
            last = self._pool_choice('last_names')
            return f"{first} {last}"
    
    def gen_company(self) -> Optional[str]:
        # 公司名添加分支后缀以提高唯一性
        if self._should_be_null():
            return None
        base_company = self._pool_choice('companies')
        # 30% 概率添加分支/部门后缀
        if random.random() < 0.3:
            suffix = random.choice(['Inc.', 'Corp.', 'Ltd.', 'LLC', 'Co.'])
            branch = random.choice(['', f' {random.choice(self.cities)} Branch', f' Division {random.randint(1,9)}'])
            return f"{base_company.replace(' Inc.', '').replace(' LLC', '').replace(' Corp.', '').replace(' Ltd.', '')} {suffix}{branch}"[:200]
        return base_company
    
    def gen_address(self, max_len: int = 500) -> Optional[str]:
        return self._wrap_null(self._pool_choice('addresses')[:max_len])
    
    def gen_product_name(self) -> Optional[str]:
        brand = random.choice(self.brands)
        product = self._pool_choice('words').capitalize()
        return self._wrap_null(f"{brand} {product} {random.randint(1, 999)}")
    
    def gen_sku(self) -> Optional[str]:
        return self._wrap_null(f"SKU-{uuid.uuid4().hex[:12].upper()}")
    
    def gen_order_number(self) -> Optional[str]:
        return self._wrap_null(f"ORD{datetime.now().strftime('%Y%m%d')}{random.randint(100000, 999999)}")
    
    def gen_ip_address(self) -> Optional[str]:
        # IP地址：50%使用池，50%生成随机IP以提高唯一性
        if self._should_be_null():
            return None
        if random.random() < 0.5:
            return self._pool_choice('ipv4s')
        else:
            # 生成随机IP（模拟常见的内网/公网IP段）
            first = random.choice([10, 172, 192, random.randint(1, 223)])
            if first == 10:
                return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            elif first == 172:
                return f"172.{random.randint(16, 31)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            elif first == 192:
                return f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
            else:
                return f"{first}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    
    def gen_user_agent(self) -> Optional[str]:
        return self._wrap_null(self._pool_choice('user_agents'))
    
    def gen_url(self, max_len: int = 500) -> Optional[str]:
        return self._wrap_null(self._pool_choice('urls')[:max_len])
    
    def gen_status_message(self, max_len: int = 500) -> Optional[str]:
        messages = ['Success', 'Completed', 'Pending', 'Processing', 'Failed', 'Cancelled', 'Timeout']
        return self._wrap_null(random.choice(messages))
    
    def gen_error_message(self) -> Optional[str]:
        errors = ['Connection timeout', 'Resource not found', 'Permission denied', 'Invalid parameter',
                  'Internal error', 'Service unavailable', 'Rate limit exceeded', None]
        return self._wrap_null(random.choice(errors))
    
    # ========== 日期时间类型 ==========
    def gen_date(self, days_range: int = 365) -> Optional[str]:
        days_ago = random.randint(0, days_range)
        return self._wrap_null((datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d'))
    
    def gen_datetime_str(self, days_range: int = 365) -> Optional[str]:
        days_ago = random.randint(0, days_range)
        dt = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        return self._wrap_null(dt.strftime('%Y-%m-%d %H:%M:%S'))
    
    def gen_future_date(self, days_range: int = 365) -> Optional[str]:
        days_ahead = random.randint(1, days_range)
        return self._wrap_null((datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d'))
    
    # ========== 半结构化类型 (JSON) ==========
    def gen_json_metadata(self) -> Optional[str]:
        data = {
            "version": f"{random.randint(1, 5)}.{random.randint(0, 9)}",
            "source": random.choice(["api", "web", "app", "import"]),
            "priority": random.choice(["low", "medium", "high"]),
            "tags": [self._pool_choice('words') for _ in range(random.randint(1, 3))]
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_attributes(self) -> Optional[str]:
        data = {
            "color": random.choice(["red", "blue", "green", "black", "white"]),
            "size": random.choice(["S", "M", "L", "XL", "XXL"]),
            "material": random.choice(["cotton", "polyester", "metal", "plastic"]),
            "weight": round(random.uniform(0.1, 50), 2)
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_settings(self) -> Optional[str]:
        data = {
            "notifications": random.choice([True, False]),
            "theme": random.choice(["light", "dark", "auto"]),
            "language": random.choice(["en", "zh", "ja", "ko"]),
            "timezone": random.choice(["UTC", "Asia/Shanghai", "America/New_York"])
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_audit_log(self) -> Optional[str]:
        data = {
            "action": random.choice(["create", "update", "delete", "view"]),
            "user_id": random.randint(1000, 9999),
            "timestamp": datetime.now().isoformat(),
            "ip": self._pool_choice('ipv4s')
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_performance(self) -> Optional[str]:
        data = {
            "response_time_ms": random.randint(10, 5000),
            "cpu_percent": round(random.uniform(0, 100), 2),
            "memory_mb": random.randint(100, 8000),
            "throughput": random.randint(100, 10000)
        }
        return self._wrap_null(json.dumps(data))
    
    # ========== 半结构化类型 (ARRAY/MAP/STRUCT for CSV) ==========
    def gen_array_tags(self) -> Optional[str]:
        if self._should_be_null():
            return None
        tags = [self._pool_choice('words') for _ in range(random.randint(1, 5))]
        return json.dumps(tags)
    
    def gen_array_categories(self) -> Optional[str]:
        if self._should_be_null():
            return None
        cats = random.sample(['electronics', 'fashion', 'home', 'sports', 'food', 'beauty'], random.randint(1, 3))
        return json.dumps(cats)
    
    def gen_array_features(self) -> Optional[str]:
        if self._should_be_null():
            return None
        features = [f"feature_{i}" for i in range(random.randint(1, 4))]
        return json.dumps(features)
    
    def gen_array_images(self) -> Optional[str]:
        if self._should_be_null():
            return None
        images = [f"https://example.com/img/{uuid.uuid4().hex[:8]}.jpg" for _ in range(random.randint(1, 5))]
        return json.dumps(images)
    
    def gen_map_prefs(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({"theme": "dark", "lang": "en", "notify": "true"})
    
    def gen_map_attrs(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({"color": "blue", "size": "M", "stock": "100"})
    
    def gen_struct_user_info(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({
            "first_name": self._pool_choice('first_names'),
            "last_name": self._pool_choice('last_names'),
            "age": random.randint(18, 70),
            "email": self._pool_choice('emails'),
            "phone": self._pool_choice('phones')
        })
    
    def gen_struct_address_info(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({
            "street": self._pool_choice('streets'),
            "city": random.choice(self.cities),
            "state": self._pool_choice('states'),
            "country": random.choice(self.countries),
            "postal_code": self._pool_choice('postcodes')
        })
    
    def gen_struct_product_info(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({
            "name": f"{random.choice(self.brands)} Product",
            "description": self._pool_choice('texts')[:200],
            "category": random.choice(['electronics', 'fashion', 'home']),
            "brand": random.choice(self.brands),
            "weight": round(random.uniform(0.1, 50), 3)
        })
    
    def gen_struct_device_info(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({
            "type": random.choice(self.device_types),
            "brand": random.choice(self.brands),
            "model": f"Model-{random.randint(100, 999)}",
            "os": random.choice(self.os_names),
            "browser": random.choice(self.browsers)
        })
    
    def generate_row(self) -> dict:
        """生成一行CSV数据"""
        return {
            # 主键列 (5列)
            'id': self.gen_id(),
            'tenant_id': self.gen_tenant_id(),
            'event_date': self.gen_event_date(),
            'business_key': self.gen_business_key(),
            'region_code': self.gen_region_code(),
            
            # 数值类型 - ID类 (13列)
            'user_id': self.gen_bigint(),
            'product_id': self.gen_bigint(),
            'order_id': self.gen_bigint(),
            'customer_id': self.gen_bigint(),
            'supplier_id': self.gen_bigint(),
            'employee_id': self.gen_int(1, 99999),
            'department_id': self.gen_smallint(1, 500),
            'category_id': self.gen_int(1, 1000),
            'location_id': self.gen_bigint(),
            'warehouse_id': self.gen_int(1, 1000),
            'transaction_id': self.gen_bigint(),
            'session_id': self.gen_bigint(),
            'device_id': self.gen_bigint(),
            
            # 数值类型 - 整数 (9列) - tinyint(2)+smallint(2)+int(2)+bigint(2)+largeint(1)
            'tinyint_col1': self.gen_tinyint(),
            'tinyint_col2': self.gen_tinyint(),
            'smallint_col1': self.gen_smallint(),
            'smallint_col2': self.gen_smallint(),
            'int_col1': self.gen_int(),
            'int_col2': self.gen_int(),
            'bigint_col1': self.gen_bigint(),
            'bigint_col2': self.gen_bigint(),
            'largeint_col': self.gen_largeint(),
            
            # 数值类型 - DECIMAL (10列)
            'decimal_p5_s2': self.gen_decimal(5, 2, 0, 999),
            'decimal_p10_s4': self.gen_decimal(10, 4, 0, 99999),
            'decimal_p15_s6': self.gen_decimal(15, 6, 0, 999999),
            'decimal_p20_s8': self.gen_decimal(20, 8, 0, 9999999),
            'decimal_p28_s10': self.gen_decimal(28, 10, 0, 99999999),
            'decimal_p38_s12': self.gen_decimal(38, 12, 0, 999999999),
            'decimal_p10_s0': self.gen_decimal(10, 0, 0, 9999999999),
            'decimal_p18_s0': self.gen_decimal(18, 0, 0, 999999999999999),
            'decimal_p8_s3': self.gen_decimal(8, 3, 0, 99999),
            'decimal_p12_s2': self.gen_decimal(12, 2, 0, 9999999999),
            
            # 数值类型 - 浮点 (4列) - float(2)+double(2)
            'float_col1': self.gen_float(),
            'float_col2': self.gen_float(),
            'double_col1': self.gen_double(),
            'double_col2': self.gen_double(),
            
            # 数值类型 - 布尔 (4列)
            'is_active': self.gen_boolean(),
            'is_valid': self.gen_boolean(),
            'is_approved': self.gen_boolean(),
            'is_completed': self.gen_boolean(),
            
            # 数值类型 - 业务金额/数量/单价 (15列)
            'amount_total': self.gen_amount(1000000, 4),
            'amount_subtotal': self.gen_amount(500000, 4),
            'amount_tax': self.gen_amount(50000, 2),
            'amount_shipping': self.gen_amount(1000, 2),
            'amount_fee': self.gen_amount(500, 2),
            'quantity_ordered': self.gen_int(1, 1000),
            'quantity_shipped': self.gen_int(1, 1000),
            'quantity_returned': self.gen_smallint(0, 100),
            'quantity_backordered': self.gen_smallint(0, 100),
            'unit_cost': self.gen_decimal(12, 4, 0.01, 10000),
            'unit_price': self.gen_decimal(12, 4, 0.01, 10000),
            'margin_percent': self.gen_decimal(5, 2, 0, 100),
            'tax_rate': self.gen_rate(0.5, 4),
            'discount_rate': self.gen_rate(0.5, 3),
            'commission_rate': self.gen_rate(0.3, 3),
            
            # 数值类型 - 物理度量 (11列)
            'weight_kg': self.gen_decimal(10, 3, 0.001, 1000),
            'volume_liters': self.gen_decimal(10, 3, 0.001, 10000),
            'length_cm': self.gen_decimal(8, 2, 0.1, 1000),
            'width_cm': self.gen_decimal(8, 2, 0.1, 1000),
            'height_cm': self.gen_decimal(8, 2, 0.1, 1000),
            'area_sqm': self.gen_decimal(10, 3, 0.001, 10000),
            'temperature_c': self.gen_float(-50, 100),
            'humidity_percent': self.gen_decimal(5, 2, 0, 100),
            'pressure_kpa': self.gen_double(80, 120),
            'speed_kmh': self.gen_float(0, 300),
            'acceleration_ms2': self.gen_double(-20, 20),
            
            # 数值类型 - 统计 (6列)
            'view_count': self.gen_count(10000000),
            'click_count': self.gen_count(1000000),
            'conversion_count': self.gen_count(100000),
            'impression_count': self.gen_count(10000000),
            'engagement_count': self.gen_count(500000),
            'comment_count': self.gen_count(100000),
            
            # 数值类型 - 评分和比率 (10列)
            'avg_rating': self.gen_score(5, 2),
            'quality_score': self.gen_score(100, 3),
            'performance_score': self.gen_score(1000, 3),
            'success_rate': self.gen_rate(1, 4),
            'error_rate': self.gen_rate(0.1, 4),
            'completion_rate': self.gen_rate(1, 3),
            'utilization_rate': self.gen_rate(1, 4),
            'efficiency_ratio': self.gen_rate(2, 5),
            'accuracy_score': self.gen_score(100, 3),
            'reliability_score': self.gen_score(100, 3),
            
            # 数值类型 - 金融 (8列)
            'balance_current': self.gen_amount(10000000, 2),
            'balance_previous': self.gen_amount(10000000, 2),
            'credit_limit': self.gen_amount(1000000, 2),
            'available_credit': self.gen_amount(1000000, 2),
            'interest_rate': self.gen_rate(0.3, 5),
            'monthly_payment': self.gen_amount(100000, 2),
            'annual_fee': self.gen_amount(10000, 2),
            'transaction_fee': self.gen_amount(1000, 2),
            
            # 字符串类型 - CHAR (7列)
            'status_code': self.gen_char(1),
            'country_code': self.gen_char(2),
            'currency_code': random.choice(self.currencies)[:3] if not self._should_be_null() else None,
            'language_code': self._wrap_null(random.choice(['en-US', 'zh-CN', 'ja-JP', 'ko-KR', 'de-DE'])),
            'size_code': self.gen_char(3),
            'type_code': self.gen_char(4),
            'level_code': self.gen_char(2),
            
            # 字符串类型 - 短文本 (3列)
            'short_code': self.gen_char(10),
            'short_name': self._wrap_null(self._pool_choice('words')[:20]),
            'short_desc': self.gen_varchar(50),
            
            # 字符串类型 - 客户信息 (11列)
            'customer_name': self.gen_name(),
            'customer_email': self.gen_email(),
            'customer_phone': self.gen_phone(),
            'contact_name': self.gen_name(),
            'contact_email': self.gen_email(),
            'contact_phone': self.gen_phone(),
            'billing_address': self.gen_address(),
            'shipping_address': self.gen_address(),
            'company_name': self.gen_company(),
            'department_name': self._wrap_null(self._pool_choice('jobs')),
            'team_name': self._wrap_null(f"Team {self._pool_choice('words').capitalize()}"[:80]),
            
            # 字符串类型 - 产品 (8列)
            'product_name': self.gen_product_name(),
            'product_description': self.gen_varchar(500),
            'product_sku': self.gen_sku(),
            'product_upc': self.gen_char(20),
            'product_brand': self._wrap_null(random.choice(self.brands)),
            'product_model': self._wrap_null(f"Model-{random.randint(100, 9999)}"[:80]),
            'product_category': self._wrap_null(random.choice(['electronics', 'fashion', 'home', 'sports'])),
            'product_type': self._wrap_null(random.choice(['physical', 'digital', 'service'])),
            
            # 字符串类型 - 订单 (5列)
            'order_number': self.gen_order_number(),
            'invoice_number': self._wrap_null(f"INV{random.randint(100000, 999999)}"),
            'tracking_number': self._wrap_null(f"TRK{uuid.uuid4().hex[:16].upper()}"),
            'receipt_number': self._wrap_null(f"RCP{random.randint(100000, 999999)}"),
            'po_number': self._wrap_null(f"PO{random.randint(10000, 99999)}"),
            
            # 字符串类型 - 支付 (5列)
            'payment_method': self._wrap_null(random.choice(self.payment_methods)),
            'payment_gateway': self._wrap_null(random.choice(['stripe', 'paypal', 'alipay', 'wechat'])),
            'card_last_four': self.gen_char(4),
            'bank_name': self._wrap_null(self._pool_choice('companies')[:100]),
            'account_number': self._wrap_null(f"{random.randint(1000000000, 9999999999)}"[:25]),
            
            # 字符串类型 - 地理位置 (7列)
            'country_name': self._wrap_null(random.choice(self.countries)),
            'state_name': self._wrap_null(self._pool_choice('states')),
            'city_name': self._wrap_null(random.choice(self.cities)),
            'postal_code': self._wrap_null(
                self._pool_choice('postcodes') if random.random() < 0.5 
                else f"{random.randint(10000, 99999)}" if random.random() < 0.5 
                else f"{random.randint(10000, 99999)}-{random.randint(1000, 9999)}"
            ),
            'timezone_name': self._wrap_null(random.choice(['UTC', 'Asia/Shanghai', 'America/New_York', 'Europe/London'])),
            'address_line1': self.gen_address(200),
            'address_line2': self._wrap_null(self._pool_choice('secondary_addr')),
            
            # 字符串类型 - 技术信息 (8列)
            'ip_address': self.gen_ip_address(),
            'user_agent': self.gen_user_agent(),
            'referrer_url': self.gen_url(),
            'landing_page': self.gen_url(),
            'exit_page': self.gen_url(),
            'campaign_name': self._wrap_null(f"Campaign_{self._pool_choice('words')}"[:200]),
            'ad_group_name': self._wrap_null(f"AdGroup_{self._pool_choice('words')}"[:150]),
            'keyword_text': self._wrap_null(self._pool_choice('words')[:100]),
            
            # 字符串类型 - 设备 (6列)
            'device_type': self._wrap_null(random.choice(self.device_types)),
            'device_model': self._wrap_null(f"{random.choice(self.brands)} {self._pool_choice('words')}"[:80]),
            'os_name': self._wrap_null(random.choice(self.os_names)),
            'os_version': self._wrap_null(f"{random.randint(10, 17)}.{random.randint(0, 5)}"),
            'browser_name': self._wrap_null(random.choice(self.browsers)),
            'browser_version': self._wrap_null(f"{random.randint(80, 120)}.0.{random.randint(1000, 9999)}"),
            
            # 字符串类型 - 标识符 (5列)
            'session_id_str': self._wrap_null(uuid.uuid4().hex),
            'transaction_id_str': self._wrap_null(f"TXN{uuid.uuid4().hex[:20].upper()}"),
            'batch_id': self._wrap_null(f"BAT{datetime.now().strftime('%Y%m%d%H%M%S')}"),
            'request_id': self._wrap_null(uuid.uuid4().hex[:40]),
            'correlation_id': self._wrap_null(uuid.uuid4().hex[:40]),
            
            # 字符串类型 - 消息 (5列)
            'status_message': self.gen_status_message(),
            'error_message': self.gen_error_message(),
            'warning_message': self._wrap_null(random.choice(['Low stock', 'Rate limit warning', None])),
            'info_message': self._wrap_null(random.choice(['Processing', 'Queued', 'Scheduled', None])),
            'notes_text': self.gen_varchar(500),
            
            # 日期时间类型 (20列)
            'created_date': self.gen_date(),
            'created_time': self.gen_datetime_str(),
            'modified_date': self.gen_date(),
            'modified_time': self.gen_datetime_str(),
            'processed_date': self.gen_date(),
            'processed_time': self.gen_datetime_str(),
            'scheduled_date': self.gen_future_date(),
            'scheduled_time': self._wrap_null((datetime.now() + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')),
            'effective_date': self.gen_date(),
            'expiry_date': self.gen_future_date(730),
            'start_date': self.gen_date(),
            'end_date': self.gen_future_date(),
            'last_login_date': self.gen_date(30),
            'last_activity_date': self.gen_date(7),
            'registration_date': self.gen_date(1095),
            'birth_date': self.gen_date(365 * 50),
            'payment_due_date': self.gen_future_date(90),
            'delivery_date': self.gen_future_date(30),
            'completion_date': self.gen_date(),
            'archive_date': self.gen_date(),
            
            # 半结构化类型 (15列)
            'json_metadata': self.gen_json_metadata(),
            'json_attributes': self.gen_json_attributes(),
            'json_settings': self.gen_json_settings(),
            'json_audit_log': self.gen_json_audit_log(),
            'json_performance_data': self.gen_json_performance(),
            'array_tags': self.gen_array_tags(),
            'array_categories': self.gen_array_categories(),
            'array_features': self.gen_array_features(),
            'array_images': self.gen_array_images(),
            'map_user_prefs': self.gen_map_prefs(),
            'map_product_attrs': self.gen_map_attrs(),
            'user_info': self.gen_struct_user_info(),
            'address_info': self.gen_struct_address_info(),
            'product_info': self.gen_struct_product_info(),
            'device_info': self.gen_struct_device_info(),
        }
    
    # ========== Parquet专用方法 ==========
    def _gen_decimal_parquet(self, scale: int, min_v: float = 0, max_v: float = 10000) -> Optional[Decimal]:
        if self._should_be_null():
            return None
        return Decimal(str(round(random.uniform(min_v, max_v), scale)))
    
    def _gen_array_native(self, gen_func, max_items: int = 5) -> Optional[List]:
        if self._should_be_null():
            return None
        return [gen_func() for _ in range(random.randint(1, max_items))]
    
    def _gen_map_native(self, keys: List[str], val_func) -> Optional[List[tuple]]:
        if self._should_be_null():
            return None
        selected = random.sample(keys, min(len(keys), random.randint(2, 4)))
        return [(k, val_func()) for k in selected]
    
    def generate_row_for_parquet(self) -> dict:
        """生成Parquet格式数据"""
        row = self.generate_row()
        
        # 转换DECIMAL为Decimal对象
        decimal_cols = ['decimal_p5_s2', 'decimal_p10_s4', 'decimal_p15_s6', 'decimal_p20_s8',
                        'decimal_p28_s10', 'decimal_p38_s12', 'decimal_p10_s0', 'decimal_p18_s0',
                        'decimal_p8_s3', 'decimal_p12_s2', 'amount_total', 'amount_subtotal',
                        'amount_tax', 'amount_shipping', 'amount_fee',
                        'unit_cost', 'unit_price', 'margin_percent', 'tax_rate', 'discount_rate',
                        'commission_rate', 'weight_kg', 'volume_liters', 'length_cm', 'width_cm',
                        'height_cm', 'area_sqm', 'humidity_percent', 'avg_rating', 'quality_score',
                        'performance_score', 'success_rate', 'error_rate', 'completion_rate',
                        'utilization_rate', 'efficiency_ratio', 'accuracy_score', 'reliability_score',
                        'balance_current', 'balance_previous', 'credit_limit', 'available_credit',
                        'interest_rate', 'monthly_payment', 'annual_fee', 'transaction_fee']
        
        for col in decimal_cols:
            if row.get(col) is not None:
                row[col] = Decimal(row[col])
        
        # 转换ARRAY为原生列表
        array_cols = ['array_tags', 'array_categories', 'array_features', 'array_images']
        for col in array_cols:
            if row.get(col) is not None:
                row[col] = json.loads(row[col])
        
        # 转换MAP为[(key, value), ...]格式
        if row.get('map_user_prefs') is not None:
            data = json.loads(row['map_user_prefs'])
            row['map_user_prefs'] = list(data.items())
        if row.get('map_product_attrs') is not None:
            data = json.loads(row['map_product_attrs'])
            row['map_product_attrs'] = list(data.items())
        
        # 转换STRUCT为原生dict
        struct_cols = ['user_info', 'address_info', 'product_info', 'device_info']
        for col in struct_cols:
            if row.get(col) is not None:
                row[col] = json.loads(row[col])
        
        return row


def format_value(value: Any) -> str:
    if value is None:
        return '\\N'
    elif isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    else:
        return str(value)


def generate_batch_data(null_ratio: float, batch_size: int, seed: Optional[int], for_parquet: bool = False) -> List[dict]:
    # 重置数据池（多进程时每个进程需要独立初始化）
    PrimaryDataGenerator._data_pool = None
    generator = PrimaryDataGenerator(null_ratio=null_ratio, seed=seed)
    rows = []
    for _ in range(batch_size):
        if for_parquet:
            rows.append(generator.generate_row_for_parquet())
        else:
            rows.append(generator.generate_row())
    return rows


def generate_csv_file_mt(file_path: str, null_ratio: float, rows_per_file: Optional[int],
                          max_file_size_mb: Optional[float], seed: Optional[int], delimiter: str,
                          columns: List[str], num_workers: int, file_idx: int, num_files: int,
                          use_multiprocess: bool = True) -> int:
    file_rows = 0
    file_size = 0
    batch_size = 10000  # 增大批量大小
    
    # 选择执行器（多进程或多线程）
    Executor = ProcessPoolExecutor if use_multiprocess else ThreadPoolExecutor
    
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        
        target_rows = rows_per_file if rows_per_file else float('inf')
        batch_idx = 0
        
        while file_rows < target_rows:
            if max_file_size_mb and file_size / (1024 * 1024) >= max_file_size_mb:
                break
            
            remaining = target_rows - file_rows if rows_per_file else batch_size * num_workers
            batches_needed = min(num_workers, max(1, int(remaining / batch_size)))
            
            with Executor(max_workers=batches_needed) as executor:
                futures = []
                for i in range(batches_needed):
                    actual_batch_size = min(batch_size, int(remaining / batches_needed))
                    if actual_batch_size <= 0:
                        break
                    batch_seed = (seed + batch_idx * num_workers + i) if seed else None
                    futures.append(executor.submit(generate_batch_data, null_ratio, actual_batch_size, batch_seed, False))
                
                for future in as_completed(futures):
                    for row_data in future.result():
                        if rows_per_file and file_rows >= rows_per_file:
                            break
                        if max_file_size_mb and file_size / (1024 * 1024) >= max_file_size_mb:
                            break
                        row_values = [format_value(row_data[col]) for col in columns]
                        writer.writerow(row_values)
                        file_rows += 1
                        file_size += len(delimiter.join(row_values).encode('utf-8')) + 1
            
            batch_idx += 1
            if file_rows % 100000 == 0 and file_rows > 0:
                print(f"  文件 {file_idx + 1}/{num_files}: 已生成 {file_rows:,} 行")
    
    return file_rows


def generate_parquet_file_mt(file_path: str, null_ratio: float, num_rows: int,
                              seed: Optional[int], num_workers: int, use_multiprocess: bool = True) -> int:
    if not PARQUET_AVAILABLE:
        raise ImportError("需要安装 pyarrow: pip install pyarrow")
    
    # 定义STRUCT类型
    user_info_type = pa.struct([('first_name', pa.string()), ('last_name', pa.string()),
                                 ('age', pa.int32()), ('email', pa.string()), ('phone', pa.string())])
    address_info_type = pa.struct([('street', pa.string()), ('city', pa.string()),
                                    ('state', pa.string()), ('country', pa.string()), ('postal_code', pa.string())])
    product_info_type = pa.struct([('name', pa.string()), ('description', pa.string()),
                                    ('category', pa.string()), ('brand', pa.string()), ('weight', pa.float64())])
    device_info_type = pa.struct([('type', pa.string()), ('brand', pa.string()),
                                   ('model', pa.string()), ('os', pa.string()), ('browser', pa.string())])
    
    # 定义schema - 5主键+90数值+70字符串+20日期+15半结构化=200列
    schema = pa.schema([
        # 主键 (5)
        ('id', pa.int64()), ('tenant_id', pa.int32()), ('event_date', pa.string()),
        ('business_key', pa.string()), ('region_code', pa.string()),
        # ID类 (13)
        ('user_id', pa.int64()), ('product_id', pa.int64()), ('order_id', pa.int64()),
        ('customer_id', pa.int64()), ('supplier_id', pa.int64()), ('employee_id', pa.int32()),
        ('department_id', pa.int16()), ('category_id', pa.int32()), ('location_id', pa.int64()),
        ('warehouse_id', pa.int32()), ('transaction_id', pa.int64()), ('session_id', pa.int64()),
        ('device_id', pa.int64()),
        # 整数 (9)
        ('tinyint_col1', pa.int8()), ('tinyint_col2', pa.int8()),
        ('smallint_col1', pa.int16()), ('smallint_col2', pa.int16()),
        ('int_col1', pa.int32()), ('int_col2', pa.int32()),
        ('bigint_col1', pa.int64()), ('bigint_col2', pa.int64()),
        ('largeint_col', pa.int64()),
        # DECIMAL (10)
        ('decimal_p5_s2', pa.decimal128(5, 2)), ('decimal_p10_s4', pa.decimal128(10, 4)),
        ('decimal_p15_s6', pa.decimal128(15, 6)), ('decimal_p20_s8', pa.decimal128(20, 8)),
        ('decimal_p28_s10', pa.decimal128(28, 10)), ('decimal_p38_s12', pa.decimal128(38, 12)),
        ('decimal_p10_s0', pa.decimal128(10, 0)), ('decimal_p18_s0', pa.decimal128(18, 0)),
        ('decimal_p8_s3', pa.decimal128(8, 3)), ('decimal_p12_s2', pa.decimal128(12, 2)),
        # 浮点 (4)
        ('float_col1', pa.float32()), ('float_col2', pa.float32()),
        ('double_col1', pa.float64()), ('double_col2', pa.float64()),
        # 布尔 (4)
        ('is_active', pa.bool_()), ('is_valid', pa.bool_()),
        ('is_approved', pa.bool_()), ('is_completed', pa.bool_()),
        # 金额 (5)
        ('amount_total', pa.decimal128(18, 4)), ('amount_subtotal', pa.decimal128(16, 4)),
        ('amount_tax', pa.decimal128(12, 2)),
        ('amount_shipping', pa.decimal128(10, 2)), ('amount_fee', pa.decimal128(8, 2)),
        ('quantity_ordered', pa.int32()), ('quantity_shipped', pa.int32()),
        ('quantity_returned', pa.int16()), ('quantity_backordered', pa.int16()),
        ('unit_cost', pa.decimal128(12, 4)), ('unit_price', pa.decimal128(12, 4)),
        ('margin_percent', pa.decimal128(5, 2)), ('tax_rate', pa.decimal128(6, 4)),
        ('discount_rate', pa.decimal128(5, 3)), ('commission_rate', pa.decimal128(5, 3)),
        # 物理 (11)
        ('weight_kg', pa.decimal128(10, 3)), ('volume_liters', pa.decimal128(10, 3)),
        ('length_cm', pa.decimal128(8, 2)), ('width_cm', pa.decimal128(8, 2)),
        ('height_cm', pa.decimal128(8, 2)), ('area_sqm', pa.decimal128(10, 3)),
        ('temperature_c', pa.float32()), ('humidity_percent', pa.decimal128(5, 2)),
        ('pressure_kpa', pa.float64()), ('speed_kmh', pa.float32()), ('acceleration_ms2', pa.float64()),
        # 统计 (6)
        ('view_count', pa.int64()), ('click_count', pa.int32()), ('conversion_count', pa.int32()),
        ('impression_count', pa.int32()), ('engagement_count', pa.int32()), ('comment_count', pa.int32()),
        # 评分 (10) - 增加精度以容纳生成的值
        ('avg_rating', pa.decimal128(4, 2)), ('quality_score', pa.decimal128(7, 3)),
        ('performance_score', pa.decimal128(8, 3)), ('success_rate', pa.decimal128(6, 4)),
        ('error_rate', pa.decimal128(6, 4)), ('completion_rate', pa.decimal128(7, 3)),
        ('utilization_rate', pa.decimal128(7, 4)), ('efficiency_ratio', pa.decimal128(8, 5)),
        ('accuracy_score', pa.decimal128(7, 3)), ('reliability_score', pa.decimal128(7, 3)),
        # 金融 (8)
        ('balance_current', pa.decimal128(15, 2)), ('balance_previous', pa.decimal128(15, 2)),
        ('credit_limit', pa.decimal128(12, 2)), ('available_credit', pa.decimal128(12, 2)),
        ('interest_rate', pa.decimal128(7, 5)), ('monthly_payment', pa.decimal128(10, 2)),
        ('annual_fee', pa.decimal128(8, 2)), ('transaction_fee', pa.decimal128(6, 2)),
        # CHAR (7)
        ('status_code', pa.string()), ('country_code', pa.string()), ('currency_code', pa.string()),
        ('language_code', pa.string()), ('size_code', pa.string()), ('type_code', pa.string()), ('level_code', pa.string()),
        # 短文本 (3)
        ('short_code', pa.string()), ('short_name', pa.string()), ('short_desc', pa.string()),
        # 客户 (11)
        ('customer_name', pa.string()), ('customer_email', pa.string()), ('customer_phone', pa.string()),
        ('contact_name', pa.string()), ('contact_email', pa.string()), ('contact_phone', pa.string()),
        ('billing_address', pa.string()), ('shipping_address', pa.string()), ('company_name', pa.string()),
        ('department_name', pa.string()), ('team_name', pa.string()),
        # 产品 (8)
        ('product_name', pa.string()), ('product_description', pa.string()), ('product_sku', pa.string()),
        ('product_upc', pa.string()), ('product_brand', pa.string()), ('product_model', pa.string()),
        ('product_category', pa.string()), ('product_type', pa.string()),
        # 订单 (5)
        ('order_number', pa.string()), ('invoice_number', pa.string()), ('tracking_number', pa.string()),
        ('receipt_number', pa.string()), ('po_number', pa.string()),
        # 支付 (5)
        ('payment_method', pa.string()), ('payment_gateway', pa.string()), ('card_last_four', pa.string()),
        ('bank_name', pa.string()), ('account_number', pa.string()),
        # 地理 (7)
        ('country_name', pa.string()), ('state_name', pa.string()), ('city_name', pa.string()),
        ('postal_code', pa.string()), ('timezone_name', pa.string()),
        ('address_line1', pa.string()), ('address_line2', pa.string()),
        # 技术 (8)
        ('ip_address', pa.string()), ('user_agent', pa.string()), ('referrer_url', pa.string()),
        ('landing_page', pa.string()), ('exit_page', pa.string()), ('campaign_name', pa.string()),
        ('ad_group_name', pa.string()), ('keyword_text', pa.string()),
        # 设备 (6)
        ('device_type', pa.string()), ('device_model', pa.string()), ('os_name', pa.string()),
        ('os_version', pa.string()), ('browser_name', pa.string()), ('browser_version', pa.string()),
        # 标识符 (5)
        ('session_id_str', pa.string()), ('transaction_id_str', pa.string()), ('batch_id', pa.string()),
        ('request_id', pa.string()), ('correlation_id', pa.string()),
        # 消息 (5)
        ('status_message', pa.string()), ('error_message', pa.string()), ('warning_message', pa.string()),
        ('info_message', pa.string()), ('notes_text', pa.string()),
        # 日期时间 (20)
        ('created_date', pa.string()), ('created_time', pa.string()), ('modified_date', pa.string()),
        ('modified_time', pa.string()), ('processed_date', pa.string()), ('processed_time', pa.string()),
        ('scheduled_date', pa.string()), ('scheduled_time', pa.string()), ('effective_date', pa.string()),
        ('expiry_date', pa.string()), ('start_date', pa.string()), ('end_date', pa.string()),
        ('last_login_date', pa.string()), ('last_activity_date', pa.string()), ('registration_date', pa.string()),
        ('birth_date', pa.string()), ('payment_due_date', pa.string()), ('delivery_date', pa.string()),
        ('completion_date', pa.string()), ('archive_date', pa.string()),
        # 半结构化 (15)
        ('json_metadata', pa.string()), ('json_attributes', pa.string()), ('json_settings', pa.string()),
        ('json_audit_log', pa.string()), ('json_performance_data', pa.string()),
        ('array_tags', pa.list_(pa.string())), ('array_categories', pa.list_(pa.string())),
        ('array_features', pa.list_(pa.string())), ('array_images', pa.list_(pa.string())),
        ('map_user_prefs', pa.map_(pa.string(), pa.string())),
        ('map_product_attrs', pa.map_(pa.string(), pa.string())),
        ('user_info', user_info_type), ('address_info', address_info_type),
        ('product_info', product_info_type), ('device_info', device_info_type),
    ])
    
    # 选择执行器
    Executor = ProcessPoolExecutor if use_multiprocess else ThreadPoolExecutor
    
    writer = pq.ParquetWriter(file_path, schema, compression='snappy')
    rows_written = 0
    batch_size = 20000  # 增大批量
    batch_idx = 0
    
    while rows_written < num_rows:
        remaining = num_rows - rows_written
        batches_to_gen = min(num_workers, max(1, remaining // batch_size))
        
        all_rows = []
        with Executor(max_workers=batches_to_gen) as executor:
            futures = []
            for i in range(batches_to_gen):
                actual_size = min(batch_size, remaining // batches_to_gen)
                if actual_size <= 0:
                    break
                batch_seed = (seed + batch_idx * num_workers + i) if seed else None
                futures.append(executor.submit(generate_batch_data, null_ratio, actual_size, batch_seed, True))
            for future in as_completed(futures):
                all_rows.extend(future.result())
        
        if not all_rows:
            break
        
        batch_data = {field.name: [] for field in schema}
        for row in all_rows:
            if rows_written >= num_rows:
                break
            for key, value in row.items():
                batch_data[key].append(value)
            rows_written += 1
        
        table = pa.table(batch_data, schema=schema)
        writer.write_table(table)
        batch_idx += 1
        
        if rows_written % 100000 == 0:
            print(f"  已生成 {rows_written:,} 行...")
    
    writer.close()
    return rows_written


def generate_single_file(file_idx: int, num_files: int, output_dir: str, rows_per_file: Optional[int],
                          max_file_size_mb: Optional[float], null_ratio: float, seed: Optional[int],
                          file_format: str, delimiter: str, num_workers: int, columns: List[str],
                          use_multiprocess: bool = True) -> tuple:
    file_name = f"data_{file_idx + 1:04d}.{file_format}"
    file_path = os.path.join(output_dir, file_name)
    file_seed = (seed + file_idx) if seed is not None else None
    
    print(f"[进程/线程 {os.getpid()}] 正在生成文件 {file_idx + 1}/{num_files}: {file_name}")
    
    if file_format == 'parquet':
        file_rows = generate_parquet_file_mt(file_path, null_ratio, rows_per_file or 100000, 
                                              file_seed, num_workers, use_multiprocess)
    else:
        file_rows = generate_csv_file_mt(file_path, null_ratio, rows_per_file, max_file_size_mb,
                                          file_seed, delimiter, columns, num_workers, file_idx, 
                                          num_files, use_multiprocess)
    
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"✓ 文件 {file_name} 生成完成: {file_rows:,} 行, {file_size_mb:.2f} MB")
    return file_rows, file_size_mb


def generate_data_files(output_dir: str, num_files: int = 1, rows_per_file: Optional[int] = None,
                         max_file_size_mb: Optional[float] = None, null_ratio: float = 0.05,
                         seed: Optional[int] = None, file_format: str = 'csv',
                         delimiter: str = '\t', num_workers: int = 4, use_multiprocess: bool = True):
    if file_format == 'parquet' and not PARQUET_AVAILABLE:
        raise ImportError("需要安装 pyarrow: pip install pyarrow")
    
    if file_format == 'parquet' and max_file_size_mb:
        print("⚠️ Parquet格式不支持按大小限制，将使用行数限制")
        if rows_per_file is None:
            rows_per_file = 100000
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 初始化一次数据池
    PrimaryDataGenerator._data_pool = None
    generator = PrimaryDataGenerator(null_ratio=null_ratio, seed=seed)
    columns = list(generator.generate_row().keys())
    
    total_rows = 0
    total_size = 0.0
    start_time = datetime.now()
    
    mode = "多进程" if use_multiprocess else "多线程"
    
    if num_files > 1 and num_workers > 1:
        file_workers = min(num_files, num_workers)
        workers_per_file = max(1, num_workers // file_workers)
        print(f"🚀 使用 {file_workers} 个{mode}并行生成 {num_files} 个文件...")
        
        # 文件级别使用线程（避免进程嵌套问题）
        with ThreadPoolExecutor(max_workers=file_workers) as executor:
            futures = [executor.submit(generate_single_file, i, num_files, output_dir, rows_per_file,
                                        max_file_size_mb, null_ratio, seed, file_format,
                                        delimiter, workers_per_file, columns, use_multiprocess) 
                       for i in range(num_files)]
            for future in as_completed(futures):
                rows, size = future.result()
                total_rows += rows
                total_size += size
    else:
        for i in range(num_files):
            rows, size = generate_single_file(i, num_files, output_dir, rows_per_file, max_file_size_mb,
                                               null_ratio, seed, file_format, delimiter, num_workers, 
                                               columns, use_multiprocess)
            total_rows += rows
            total_size += size
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n========== 生成完成 ==========")
    print(f"总文件数: {num_files}")
    print(f"总行数: {total_rows:,}")
    print(f"总大小: {total_size:.2f} MB")
    print(f"耗时: {elapsed:.2f} 秒")
    print(f"速度: {total_rows / elapsed:,.0f} 行/秒" if elapsed > 0 else "")
    print(f"输出目录: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description='生成 primary_key_optimized 表数据 (5主键+90数值+70字符串+20日期+15半结构化=200列)',
                                      formatter_class=argparse.RawDescriptionHelpFormatter,
                                      epilog='''
使用示例:
  python primary_data_generator.py -n 3 -r 10000
  python primary_data_generator.py -n 5 -s 100 -t 8
  python primary_data_generator.py -n 1 -r 100000 --format parquet
  python primary_data_generator.py -n 3 -r 100000 --fast  # 使用多进程加速
''')
    parser.add_argument('-n', '--num-files', type=int, default=1, help='文件数量 (默认: 1)')
    parser.add_argument('-r', '--rows', type=int, help='每文件行数')
    parser.add_argument('-s', '--size-mb', type=float, help='每文件大小限制(MB)')
    parser.add_argument('-o', '--output-dir', default='./generated_data_primary', help='输出目录')
    parser.add_argument('--null-ratio', type=float, default=0.1, help='NULL比例 (默认: 0.1)')
    parser.add_argument('--seed', type=int, help='随机种子')
    parser.add_argument('--format', choices=['csv', 'txt', 'parquet'], default='csv', help='输出格式')
    parser.add_argument('--delimiter', default='tab', help='分隔符 (tab/comma/pipe)')
    parser.add_argument('-t', '--workers', type=int, default=4, help='并行数 (默认: 4)')
    parser.add_argument('--fast', action='store_true', help='使用多进程模式加速 (绕过GIL)')
    parser.add_argument('--threads-only', action='store_true', help='强制使用多线程 (兼容性更好)')
    
    args = parser.parse_args()
    
    delimiter_map = {'tab': '\t', '\\t': '\t', 'comma': ',', 'pipe': '|'}
    if args.delimiter.lower() in delimiter_map:
        args.delimiter = delimiter_map[args.delimiter.lower()]
    
    if len(args.delimiter) != 1:
        parser.error(f"分隔符必须是单字符: '{args.delimiter}'")
    
    if args.rows is None and args.size_mb is None:
        args.rows = 1000
        print(f"未指定行数/大小，默认每文件 {args.rows} 行")
    
    # 决定使用多进程还是多线程
    use_multiprocess = args.fast and not args.threads_only
    mode = "多进程" if use_multiprocess else "多线程"
    
    print(f"\n========== 配置 ==========")
    print(f"文件数: {args.num_files}, 行数: {args.rows or '按大小'}, 大小限制: {args.size_mb or '无'} MB")
    print(f"NULL比例: {args.null_ratio*100:.1f}%, 格式: {args.format}, 并行: {args.workers} ({mode})")
    print(f"===========================\n")
    
    generate_data_files(args.output_dir, args.num_files, args.rows, args.size_mb,
                         args.null_ratio, args.seed, args.format, args.delimiter, 
                         args.workers, use_multiprocess)


if __name__ == '__main__':
    main()
