#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据生成脚本 - 用于生成符合 duplicate_key_optimized 表结构的测试数据
模拟用户行为事件/日志数据

表结构说明:
- 排序键列: 5列
- 数值类型: 95列
- 字符串类型: 65列
- 日期时间类型: 20列
- 半结构化类型: 15列 (6 JSON + 4 ARRAY + 3 MAP + 2 STRUCT)
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

# 延迟初始化 Faker
fake_cn = None
fake_en = None

def _init_faker():
    global fake_cn, fake_en
    if fake_cn is None:
        fake_cn = Faker('zh_CN')
        fake_en = Faker('en_US')


class DuplicateDataGenerator:
    """重复键表数据生成器 - 模拟用户行为事件"""
    
    # 类级别预生成数据池
    _data_pool = None
    _pool_size = 5000  # 增大到 5000
    
    @classmethod
    def _init_data_pool(cls):
        """预生成常用数据池，避免重复调用 Faker"""
        if cls._data_pool is not None:
            return
        _init_faker()
        cls._data_pool = {
            'names': [fake_en.name()[:100] for _ in range(cls._pool_size)],
            'emails': [fake_en.email() for _ in range(cls._pool_size)],
            'phones': [fake_en.phone_number()[:30] for _ in range(cls._pool_size)],
            'companies': [fake_en.company()[:150] for _ in range(cls._pool_size)],
            'addresses': [fake_en.address()[:500] for _ in range(cls._pool_size)],
            'states': [fake_en.state() for _ in range(cls._pool_size)],
            'postcodes': [fake_en.postcode() for _ in range(cls._pool_size)],
            'words': [fake_en.word() for _ in range(cls._pool_size)],
            'texts': [fake_en.text(max_nb_chars=100) for _ in range(cls._pool_size)],
            'user_agents': [fake_en.user_agent()[:1000] for _ in range(2000)],
            'ipv4s': [fake_en.ipv4() for _ in range(cls._pool_size)],
            'urls': [fake_en.url()[:500] for _ in range(cls._pool_size)],
            'domain_names': [fake_en.domain_name()[:100] for _ in range(2000)],
            'sentences': [fake_en.sentence()[:100] for _ in range(2000)],
            'first_names': [fake_en.first_name() for _ in range(2000)],
            'last_names': [fake_en.last_name() for _ in range(2000)],
        }
    
    def __init__(self, null_ratio: float = 0.05, seed: Optional[int] = None):
        self.null_ratio = null_ratio
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        
        # 初始化数据池
        self._init_data_pool()
        
        # 预定义数据
        self.event_types = ['page_view', 'click', 'purchase', 'add_cart', 'search', 'login', 'logout',
                            'signup', 'share', 'like', 'comment', 'video_play', 'download', 'error']
        self.regions = ['CHN', 'USA', 'JPN', 'KOR', 'GBR', 'DEU', 'FRA', 'AUS', 'SGP', 'IND']
        self.countries = ['China', 'United States', 'Japan', 'Korea', 'United Kingdom', 'Germany', 'France']
        self.cities = ['Beijing', 'Shanghai', 'New York', 'Tokyo', 'London', 'Berlin', 'Paris', 'Sydney']
        self.brands = ['Apple', 'Samsung', 'Huawei', 'Xiaomi', 'Sony', 'Dell', 'HP', 'Lenovo', 'Nike', 'Adidas']
        self.device_types = ['mobile', 'desktop', 'tablet', 'smart_tv', 'wearable']
        self.os_names = ['Windows', 'macOS', 'iOS', 'Android', 'Linux', 'Chrome OS']
        self.browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera']
        self.payment_types = ['credit_card', 'debit_card', 'alipay', 'wechat_pay', 'paypal']
        self.order_types = ['standard', 'express', 'subscription', 'gift', 'bulk']
        self.shipping_methods = ['standard', 'express', 'overnight', 'pickup', 'international']
        self.user_tiers = ['free', 'basic', 'premium', 'enterprise']
        self.membership_levels = ['bronze', 'silver', 'gold', 'platinum', 'diamond']
        self.connection_types = ['wifi', '4g', '5g', '3g', 'ethernet', 'unknown']
        self.carriers = ['China Mobile', 'China Unicom', 'China Telecom', 'AT&T', 'Verizon', 'T-Mobile']
    
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
    
    # ========== 排序键列 ==========
    def gen_event_date(self) -> str:
        days_ago = random.randint(0, 30)
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    
    def gen_tenant_id(self) -> int:
        return random.randint(1, 100)
    
    def gen_event_type(self) -> str:
        weights = [0.25, 0.2, 0.1, 0.1, 0.1, 0.05, 0.05, 0.03, 0.03, 0.03, 0.02, 0.02, 0.01, 0.01]
        return random.choices(self.event_types, weights=weights[:len(self.event_types)])[0]
    
    def gen_region_code(self) -> str:
        return random.choice(self.regions)
    
    def gen_status_code(self) -> int:
        return random.choice([0, 1, 2, 3, 4, 5])  # 0=success, 1=pending, 2=processing, 3=failed, etc.
    
    # ========== 数值类型 ==========
    def gen_bigint(self, min_v=1, max_v=9999999999) -> Optional[int]:
        return self._wrap_null(random.randint(min_v, max_v))
    
    def gen_int(self, min_v=0, max_v=999999) -> Optional[int]:
        return self._wrap_null(random.randint(min_v, max_v))
    
    def gen_smallint(self, min_v=0, max_v=32000) -> Optional[int]:
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
    
    def gen_duration_ms(self, max_v: int = 60000) -> Optional[int]:
        weights = [0.4, 0.3, 0.2, 0.08, 0.02]
        ranges = [(10, 500), (500, 2000), (2000, 10000), (10000, 30000), (30000, max_v)]
        # 过滤有效范围
        valid = [(r, w) for r, w in zip(ranges, weights) if r[0] <= r[1] and r[1] <= max_v]
        if not valid:
            return self._wrap_null(random.randint(1, max(1, max_v)))
        valid_ranges, valid_weights = zip(*valid)
        r = random.choices(valid_ranges, weights=valid_weights)[0]
        return self._wrap_null(random.randint(r[0], min(r[1], max_v)))
    
    def gen_duration_sec(self, max_v: int = 3600) -> Optional[int]:
        weights = [0.3, 0.3, 0.25, 0.1, 0.05]
        ranges = [(1, 30), (30, 120), (120, 600), (600, 1800), (1800, max_v)]
        # 过滤有效范围
        valid = [(r, w) for r, w in zip(ranges, weights) if r[0] <= r[1] and r[0] <= max_v]
        if not valid:
            return self._wrap_null(random.randint(1, max(1, max_v)))
        valid_ranges, valid_weights = zip(*valid)
        r = random.choices(valid_ranges, weights=valid_weights)[0]
        return self._wrap_null(random.randint(r[0], min(r[1], max_v)))
    
    def gen_score(self, max_v: float = 5.0, scale: int = 2) -> Optional[str]:
        return self._wrap_null(f"{round(random.uniform(1, max_v), scale):.{scale}f}")
    
    # ========== 字符串类型 ==========
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
        return (base_phone + suffix)[:30]  # 保持长度限制
    
    def gen_name(self) -> Optional[str]:
        # 组合姓名：从池中随机选择名和姓
        if self._should_be_null():
            return None
        # 20% 概率直接使用完整姓名，80% 概率重新组合
        if random.random() < 0.2:
            return self._pool_choice('names')[:100]
        else:
            first = self._pool_choice('first_names')
            last = self._pool_choice('last_names')
            return f"{first} {last}"[:100]
    
    def gen_address(self, max_len: int = 500) -> Optional[str]:
        return self._wrap_null(self._pool_choice('addresses')[:max_len])
    
    def gen_product_name(self) -> Optional[str]:
        brand = random.choice(self.brands)
        product = self._pool_choice('words').capitalize()
        return self._wrap_null(f"{brand} {product} {random.randint(1, 999)}"[:300])
    
    def _gen_manufacturer(self) -> Optional[str]:
        """生成制造商名称（添加分支/部门后缀以提高唯一性）"""
        if self._should_be_null():
            return None
        base_company = self._pool_choice('companies')
        # 30% 概率添加分支/部门后缀
        if random.random() < 0.3:
            suffix = random.choice(['Inc.', 'Corp.', 'Ltd.', 'LLC', 'Co.'])
            branch = random.choice(['', f' {random.choice(self.cities)} Branch', f' Division {random.randint(1,9)}'])
            # 移除原有后缀
            clean_name = base_company.replace(' Inc.', '').replace(' LLC', '').replace(' Corp.', '').replace(' Ltd.', '')
            return f"{clean_name} {suffix}{branch}"[:150]
        return base_company
    
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
    
    def gen_error_desc(self) -> Optional[str]:
        errors = ['Connection timeout', 'Resource not found', 'Permission denied', 'Invalid parameter',
                  'Internal error', 'Service unavailable', 'Rate limit exceeded', None]
        return self._wrap_null(random.choice(errors))
    
    def gen_uuid(self) -> Optional[str]:
        return self._wrap_null(uuid.uuid4().hex)
    
    def gen_version(self) -> Optional[str]:
        return self._wrap_null(f"{random.randint(1, 10)}.{random.randint(0, 20)}.{random.randint(0, 50)}")
    
    def gen_resolution(self) -> Optional[str]:
        resolutions = ['1920x1080', '1366x768', '2560x1440', '3840x2160', '375x667', '414x896']
        return self._wrap_null(random.choice(resolutions))
    
    # ========== 日期时间类型 ==========
    def gen_date(self, days_range: int = 365) -> Optional[str]:
        days_ago = random.randint(0, days_range)
        return self._wrap_null((datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d'))
    
    def gen_datetime_str(self, days_range: int = 30) -> Optional[str]:
        days_ago = random.randint(0, days_range)
        dt = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23),
                                         minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        return self._wrap_null(dt.strftime('%Y-%m-%d %H:%M:%S'))
    
    def gen_future_date(self, days_range: int = 365) -> Optional[str]:
        days_ahead = random.randint(1, days_range)
        return self._wrap_null((datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d'))
    
    # ========== 半结构化类型 (JSON) ==========
    def gen_json_event_data(self) -> Optional[str]:
        data = {
            "event_id": uuid.uuid4().hex[:16],
            "timestamp": int(datetime.now().timestamp() * 1000),
            "source": random.choice(["web", "ios", "android", "api"]),
            "ab_test": random.choice(["control", "variant_a", "variant_b"])
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_user_attrs(self) -> Optional[str]:
        data = {
            "age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
            "gender": random.choice(["M", "F", "O"]),
            "registered_days": random.randint(0, 1000),
            "total_orders": random.randint(0, 100)
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_product_attrs(self) -> Optional[str]:
        data = {
            "category": random.choice(["electronics", "fashion", "home", "sports"]),
            "brand": random.choice(self.brands),
            "price_range": random.choice(["low", "medium", "high", "premium"]),
            "in_stock": random.choice([True, False])
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_order_details(self) -> Optional[str]:
        items = [{"sku": f"SKU{random.randint(1000, 9999)}", "qty": random.randint(1, 5),
                  "price": round(random.uniform(10, 500), 2)} for _ in range(random.randint(1, 4))]
        return self._wrap_null(json.dumps({"items": items, "total": sum(i["price"] * i["qty"] for i in items)}))
    
    def gen_json_payment_info(self) -> Optional[str]:
        data = {
            "method": random.choice(self.payment_types),
            "status": random.choice(["pending", "completed", "failed", "refunded"]),
            "currency": random.choice(["CNY", "USD", "EUR", "GBP"])
        }
        return self._wrap_null(json.dumps(data))
    
    def gen_json_shipping_info(self) -> Optional[str]:
        data = {
            "method": random.choice(self.shipping_methods),
            "carrier": random.choice(["SF", "ZTO", "YTO", "EMS", "FedEx", "UPS"]),
            "tracking": f"TRK{uuid.uuid4().hex[:16].upper()}"
        }
        return self._wrap_null(json.dumps(data))
    
    # ========== 半结构化类型 (ARRAY/MAP/STRUCT for CSV) ==========
    def gen_array_tags(self) -> Optional[str]:
        if self._should_be_null():
            return None
        tags = [self._pool_choice('words') for _ in range(random.randint(1, 5))]
        return json.dumps(tags)
    
    def gen_array_products(self) -> Optional[str]:
        if self._should_be_null():
            return None
        products = [f"prod_{random.randint(10000, 99999)}" for _ in range(random.randint(1, 8))]
        return json.dumps(products)
    
    def gen_array_keywords(self) -> Optional[str]:
        if self._should_be_null():
            return None
        keywords = random.sample(['phone', 'laptop', 'shoes', 'dress', 'watch', 'camera', 'headphones'], random.randint(1, 4))
        return json.dumps(keywords)
    
    def gen_map_prefs(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({"theme": random.choice(["light", "dark"]), "lang": random.choice(["en", "zh"]),
                           "notify": random.choice(["on", "off"])})
    
    def gen_map_features(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({"color": random.choice(["red", "blue", "black"]), "size": random.choice(["S", "M", "L"]),
                           "stock": str(random.randint(0, 100))})
    
    def gen_map_session(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({"source": random.choice(["organic", "paid", "direct"]),
                           "medium": random.choice(["web", "app"]), "campaign": f"camp_{random.randint(100, 999)}"})
    
    def gen_struct_geo(self) -> Optional[str]:
        if self._should_be_null():
            return None
        return json.dumps({
            "country": random.choice(self.countries),
            "region": self._pool_choice('states'),
            "city": random.choice(self.cities),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6)
        })
    
    def gen_struct_device(self) -> Optional[str]:
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
            # 排序键列 (5列)
            'event_date': self.gen_event_date(),
            'tenant_id': self.gen_tenant_id(),
            'event_type': self.gen_event_type(),
            'region_code': self.gen_region_code(),
            'status_code': self.gen_status_code(),
            
            # 数值类型 - ID类 (7列)
            'user_id': self.gen_bigint(),
            'product_id': self.gen_bigint(),
            'session_id': self.gen_bigint(),
            'device_id': self.gen_bigint(),
            'page_id': self.gen_bigint(),
            'campaign_id': self.gen_bigint(),
            'content_id': self.gen_bigint(),
            
            # 数值类型 - 整数指标 (9列)
            'tinyint_metric1': self.gen_tinyint(),
            'tinyint_metric2': self.gen_tinyint(),
            'smallint_metric1': self.gen_smallint(),
            'smallint_metric2': self.gen_smallint(),
            'int_metric1': self.gen_int(),
            'int_metric2': self.gen_int(),
            'bigint_metric1': self.gen_bigint(),
            'bigint_metric2': self.gen_bigint(),
            'largeint_metric': self.gen_largeint(),
            
            # 数值类型 - DECIMAL (5列)
            'decimal_metric1': self.gen_decimal(10, 4, 0, 99999),
            'decimal_metric2': self.gen_decimal(15, 6, 0, 999999),
            'decimal_metric3': self.gen_decimal(20, 8, 0, 9999999),
            'decimal_metric4': self.gen_decimal(8, 2, 0, 99999),
            'decimal_metric5': self.gen_decimal(12, 0, 0, 999999999999),
            
            # 数值类型 - 浮点 (4列)
            'float_metric1': self.gen_float(),
            'float_metric2': self.gen_float(),
            'double_metric1': self.gen_double(),
            'double_metric2': self.gen_double(),
            
            # 数值类型 - 布尔 (4列) 
            'is_converted': self.gen_boolean(),
            'is_engaged': self.gen_boolean(),
            'is_bounced': self.gen_boolean(),
            'is_returning': self.gen_boolean(),
            
            # 数值类型 - 时间指标 (7列)
            'session_duration_seconds': self.gen_duration_sec(7200),
            'page_duration_seconds': self.gen_duration_sec(600),
            'time_on_site_seconds': self.gen_duration_sec(3600),
            'response_time_ms': self.gen_duration_ms(5000),
            'load_time_ms': self.gen_duration_ms(10000),
            'processing_time_ms': self.gen_duration_ms(3000),
            'latency_ms': self.gen_duration_ms(1000),
            
            # 数值类型 - 计数 (4列) - 已移除 engagement_count
            'view_count': self.gen_count(100000),
            'click_count': self.gen_count(10000),
            'impression_count': self.gen_count(1000000),
            'conversion_count': self.gen_count(1000),
            
            # 数值类型 - 比率 (8列)
            'scroll_depth_percent': self.gen_decimal(5, 2, 0, 100),
            'click_through_rate': self.gen_rate(1, 4),
            'conversion_rate': self.gen_rate(0.2, 4),
            'bounce_rate': self.gen_rate(1, 4),
            'exit_rate': self.gen_rate(1, 4),
            'engagement_rate': self.gen_rate(1, 4),
            'retention_rate': self.gen_rate(1, 4),
            'churn_rate': self.gen_rate(0.3, 4),
            
            # 数值类型 - 金额 (11列)
            'revenue_amount': self.gen_amount(1000000, 2),
            'cost_amount': self.gen_amount(500000, 2),
            'profit_amount': self.gen_amount(500000, 2),
            'margin_amount': self.gen_amount(100000, 2),
            'tax_amount': self.gen_amount(50000, 2),
            'discount_amount': self.gen_amount(10000, 2),
            'shipping_amount': self.gen_amount(1000, 2),
            'ad_spend': self.gen_amount(100000, 2),
            'cpc_cost': self.gen_decimal(10, 4, 0.01, 100),
            'cpm_cost': self.gen_decimal(10, 4, 0.1, 500),
            'cpa_cost': self.gen_decimal(10, 2, 1, 1000),
            
            # 数值类型 - ROI (2列)
            'roi_ratio': self.gen_decimal(8, 4, 0, 100),
            'roas_ratio': self.gen_decimal(8, 4, 0, 50),
            
            # 数值类型 - 库存 (7列)
            'stock_quantity': self.gen_int(0, 10000),
            'reserved_quantity': self.gen_int(0, 1000),
            'available_quantity': self.gen_int(0, 10000),
            'reorder_level': self.gen_smallint(0, 500),
            'safety_stock': self.gen_smallint(0, 200),
            'min_stock_level': self.gen_smallint(0, 100),
            'max_stock_level': self.gen_int(0, 50000),
            
            # 数值类型 - 用户行为 (11列)
            'login_count': self.gen_count(1000),
            'purchase_count': self.gen_count(500),
            'cart_add_count': self.gen_count(2000),
            'wishlist_count': self.gen_count(500),
            'review_count': self.gen_count(100),
            'rating_avg': self.gen_score(5, 2),
            'rating_count': self.gen_count(10000),
            'share_count': self.gen_count(5000),
            'like_count': self.gen_count(50000),
            'comment_count': self.gen_count(10000),
            'follow_count': self.gen_count(100000),
            
            # 数值类型 - 系统性能 (10列)
            'cpu_usage_percent': self.gen_decimal(5, 2, 0, 100),
            'memory_usage_mb': self.gen_int(100, 16000),
            'disk_usage_gb': self.gen_decimal(8, 2, 0, 1000),
            'network_bytes_sent': self.gen_bigint(0, 10000000000),
            'network_bytes_received': self.gen_bigint(0, 10000000000),
            'request_count': self.gen_count(1000000),
            'error_count': self.gen_count(10000),
            'success_count': self.gen_count(1000000),
            'failure_count': self.gen_count(10000),
            'timeout_count': self.gen_count(1000),
            
            # 数值类型 - 数据质量 (6列)
            'cache_hit_rate': self.gen_rate(1, 3),
            'data_quality_score': self.gen_rate(1, 3),
            'accuracy_score': self.gen_rate(1, 3),
            'completeness_score': self.gen_rate(1, 3),
            'consistency_score': self.gen_rate(1, 3),
            'timeliness_score': self.gen_rate(1, 3),
            
            # 字符串类型 - 用户 (4列)
            'user_name': self.gen_name(),
            'user_email': self.gen_email(),
            'user_phone': self.gen_phone(),
            'user_address': self.gen_address(),
            
            # 字符串类型 - 产品 (6列)
            'product_name': self.gen_product_name(),
            'product_description': self.gen_varchar(500),
            'product_brand': self._wrap_null(random.choice(self.brands)),
            'product_manufacturer': self._gen_manufacturer(),
            'product_category': self._wrap_null(random.choice(['electronics', 'fashion', 'home', 'sports'])),
            'product_type': self._wrap_null(random.choice(['physical', 'digital', 'service'])),
            
            # 字符串类型 - 订单 (3列)
            'order_type': self._wrap_null(random.choice(self.order_types)),
            'payment_type': self._wrap_null(random.choice(self.payment_types)),
            'shipping_method': self._wrap_null(random.choice(self.shipping_methods)),
            
            # 字符串类型 - 地理 (5列)
            'country_name': self._wrap_null(random.choice(self.countries)),
            'state_name': self._wrap_null(self._pool_choice('states')),
            'city_name': self._wrap_null(random.choice(self.cities)),
            'postal_code': self._wrap_null(
                self._pool_choice('postcodes') if random.random() < 0.5 
                else f"{random.randint(10000, 99999)}" if random.random() < 0.5 
                else f"{random.randint(10000, 99999)}-{random.randint(1000, 9999)}"
            ),
            'timezone_name': self._wrap_null(random.choice(['UTC', 'Asia/Shanghai', 'America/New_York'])),
            
            # 字符串类型 - 技术 (8列)
            'ip_address': self.gen_ip_address(),
            'user_agent': self.gen_user_agent(),
            'referrer_domain': self._wrap_null(self._pool_choice('domain_names')),
            'landing_page_url': self.gen_url(),
            'exit_page_url': self.gen_url(),
            'current_page_url': self.gen_url(),
            'previous_page_url': self.gen_url(),
            'next_page_url': self.gen_url(),
            
            # 字符串类型 - 营销 (6列)
            'campaign_name': self._wrap_null(f"Campaign_{self._pool_choice('words')}"[:200]),
            'ad_group_name': self._wrap_null(f"AdGroup_{self._pool_choice('words')}"[:150]),
            'keyword_text': self._wrap_null(self._pool_choice('words')[:100]),
            'creative_name': self._wrap_null(f"Creative_{random.randint(100, 999)}"[:100]),
            'audience_segment': self._wrap_null(random.choice(['new', 'returning', 'loyal', 'churned'])),
            'user_tier': self._wrap_null(random.choice(self.user_tiers)),
            
            # 字符串类型 - 会员 (3列)
            'membership_level': self._wrap_null(random.choice(self.membership_levels)),
            'subscription_type': self._wrap_null(random.choice(['free', 'monthly', 'annual', 'lifetime'])),
            'billing_cycle': self._wrap_null(random.choice(['monthly', 'quarterly', 'annual'])),
            
            # 字符串类型 - 设备 (11列)
            'device_type': self._wrap_null(random.choice(self.device_types)),
            'device_model': self._wrap_null(f"{random.choice(self.brands)} Model-{random.randint(100, 999)}"[:80]),
            'os_name': self._wrap_null(random.choice(self.os_names)),
            'os_version': self.gen_version(),
            'browser_name': self._wrap_null(random.choice(self.browsers)),
            'browser_version': self.gen_version(),
            'screen_resolution': self.gen_resolution(),
            'connection_type': self._wrap_null(random.choice(self.connection_types)),
            'carrier_name': self._wrap_null(random.choice(self.carriers)),
            'app_name': self._wrap_null(f"App_{self._pool_choice('words')}"[:100]),
            'app_version': self.gen_version(),
            
            # 字符串类型 - 版本 (3列)
            'sdk_version': self.gen_version(),
            'api_version': self._wrap_null(f"v{random.randint(1, 3)}.{random.randint(0, 9)}"[:10]),
            'protocol_version': self._wrap_null(f"{random.randint(1, 2)}.{random.randint(0, 2)}"[:10]),
            
            # 字符串类型 - 消息 (5列)
            'status_message': self.gen_status_message(),
            'error_description': self.gen_error_desc(),
            'warning_message': self._wrap_null(random.choice(['Low memory', 'Rate limit warning', None])),
            'info_message': self._wrap_null(random.choice(['Processing', 'Queued', 'Scheduled', None])),
            'debug_message': self._wrap_null(f"Debug: {self._pool_choice('sentences')}" if random.random() > 0.8 else None),
            
            # 字符串类型 - 标识符 (6列)
            'transaction_id': self._wrap_null(f"TXN{uuid.uuid4().hex[:20].upper()}"),
            'batch_id': self._wrap_null(f"BAT{datetime.now().strftime('%Y%m%d%H%M%S')}"),
            'request_id': self.gen_uuid(),
            'correlation_id': self.gen_uuid(),
            'trace_id': self.gen_uuid(),
            'span_id': self.gen_uuid(),
            
            # 字符串类型 - URL (5列)
            'download_url': self.gen_url(),
            'image_url': self._wrap_null(f"https://cdn.example.com/img/{uuid.uuid4().hex[:8]}.jpg"),
            'video_url': self._wrap_null(f"https://cdn.example.com/video/{uuid.uuid4().hex[:8]}.mp4"),
            'document_url': self._wrap_null(f"https://cdn.example.com/doc/{uuid.uuid4().hex[:8]}.pdf"),
            'thumbnail_url': self._wrap_null(f"https://cdn.example.com/thumb/{uuid.uuid4().hex[:8]}.jpg"),
            
            # 日期时间类型 (20列)
            'event_timestamp': self.gen_datetime_str(30),
            'session_start_time': self.gen_datetime_str(30),
            'session_end_time': self.gen_datetime_str(30),
            'page_view_time': self.gen_datetime_str(30),
            'click_time': self.gen_datetime_str(30),
            'conversion_time': self.gen_datetime_str(30),
            'purchase_time': self.gen_datetime_str(30),
            'registration_date': self.gen_date(1095),
            'last_login_date': self.gen_date(30),
            'first_purchase_date': self.gen_date(365),
            'last_purchase_date': self.gen_date(30),
            'campaign_start_date': self.gen_date(90),
            'campaign_end_date': self.gen_future_date(90),
            'promotion_start_date': self.gen_date(30),
            'promotion_end_date': self.gen_future_date(30),
            'subscription_start_date': self.gen_date(365),
            'subscription_end_date': self.gen_future_date(365),
            'created_date': self.gen_date(),
            'modified_date': self.gen_date(),
            'processed_date': self.gen_date(),
            
            # 半结构化类型 (15列)
            'event_data': self.gen_json_event_data(),
            'user_attributes': self.gen_json_user_attrs(),
            'product_attributes': self.gen_json_product_attrs(),
            'order_details': self.gen_json_order_details(),
            'payment_info': self.gen_json_payment_info(),
            'shipping_info': self.gen_json_shipping_info(),
            'user_tags': self.gen_array_tags(),
            'product_tags': self.gen_array_tags(),
            'viewed_products': self.gen_array_products(),
            'searched_keywords': self.gen_array_keywords(),
            'user_preferences': self.gen_map_prefs(),
            'product_features': self.gen_map_features(),
            'session_attributes': self.gen_map_session(),
            'geo_location': self.gen_struct_geo(),
            'device_info': self.gen_struct_device(),
        }
    
    # ========== Parquet专用方法 ==========
    def _gen_decimal_parquet(self, scale: int, min_v: float = 0, max_v: float = 10000) -> Optional[Decimal]:
        if self._should_be_null():
            return None
        return Decimal(str(round(random.uniform(min_v, max_v), scale)))
    
    def generate_row_for_parquet(self) -> dict:
        """生成Parquet格式数据"""
        row = self.generate_row()
        
        # 转换DECIMAL
        decimal_cols = ['decimal_metric1', 'decimal_metric2', 'decimal_metric3', 'decimal_metric4',
                        'decimal_metric5', 'scroll_depth_percent', 'click_through_rate', 'conversion_rate',
                        'bounce_rate', 'exit_rate', 'engagement_rate', 'retention_rate', 'churn_rate',
                        'revenue_amount', 'cost_amount', 'profit_amount', 'margin_amount', 'tax_amount',
                        'discount_amount', 'shipping_amount', 'ad_spend', 'cpc_cost', 'cpm_cost', 'cpa_cost',
                        'roi_ratio', 'roas_ratio', 'cpu_usage_percent', 'disk_usage_gb', 'cache_hit_rate',
                        'data_quality_score', 'accuracy_score', 'completeness_score', 'consistency_score',
                        'timeliness_score', 'rating_avg']
        for col in decimal_cols:
            if row.get(col) is not None:
                row[col] = Decimal(row[col])
        
        # 转换ARRAY
        array_cols = ['user_tags', 'product_tags', 'viewed_products', 'searched_keywords']
        for col in array_cols:
            if row.get(col) is not None:
                row[col] = json.loads(row[col])
        
        # 转换MAP
        map_cols = ['user_preferences', 'product_features', 'session_attributes']
        for col in map_cols:
            if row.get(col) is not None:
                data = json.loads(row[col])
                row[col] = list(data.items())
        
        # 转换STRUCT
        struct_cols = ['geo_location', 'device_info']
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
    DuplicateDataGenerator._data_pool = None
    generator = DuplicateDataGenerator(null_ratio=null_ratio, seed=seed)
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
    geo_type = pa.struct([('country', pa.string()), ('region', pa.string()), ('city', pa.string()),
                           ('latitude', pa.float64()), ('longitude', pa.float64())])
    device_type = pa.struct([('type', pa.string()), ('brand', pa.string()), ('model', pa.string()),
                              ('os', pa.string()), ('browser', pa.string())])
    
    # 定义schema - 200列
    schema = pa.schema([
        # 排序键 (5)
        ('event_date', pa.string()), ('tenant_id', pa.int32()), ('event_type', pa.string()),
        ('region_code', pa.string()), ('status_code', pa.int8()),
        # ID类 (7)
        ('user_id', pa.int64()), ('product_id', pa.int64()), ('session_id', pa.int64()),
        ('device_id', pa.int64()), ('page_id', pa.int64()), ('campaign_id', pa.int64()), ('content_id', pa.int64()),
        # 整数 (9)
        ('tinyint_metric1', pa.int8()), ('tinyint_metric2', pa.int8()),
        ('smallint_metric1', pa.int16()), ('smallint_metric2', pa.int16()),
        ('int_metric1', pa.int32()), ('int_metric2', pa.int32()),
        ('bigint_metric1', pa.int64()), ('bigint_metric2', pa.int64()), ('largeint_metric', pa.int64()),
        # DECIMAL (5)
        ('decimal_metric1', pa.decimal128(10, 4)), ('decimal_metric2', pa.decimal128(15, 6)),
        ('decimal_metric3', pa.decimal128(20, 8)), ('decimal_metric4', pa.decimal128(8, 2)),
        ('decimal_metric5', pa.decimal128(12, 0)),
        # 浮点 (4)
        ('float_metric1', pa.float32()), ('float_metric2', pa.float32()),
        ('double_metric1', pa.float64()), ('double_metric2', pa.float64()),
        # 布尔 (4)
        ('is_converted', pa.bool_()), ('is_engaged', pa.bool_()), ('is_bounced', pa.bool_()),
        ('is_returning', pa.bool_()),
        # 时间 (7)
        ('session_duration_seconds', pa.int32()), ('page_duration_seconds', pa.int32()),
        ('time_on_site_seconds', pa.int32()), ('response_time_ms', pa.int32()),
        ('load_time_ms', pa.int32()), ('processing_time_ms', pa.int32()), ('latency_ms', pa.int32()),
        # 计数 (4)
        ('view_count', pa.int32()), ('click_count', pa.int32()), ('impression_count', pa.int32()),
        ('conversion_count', pa.int32()),
        # 比率 (8)
        ('scroll_depth_percent', pa.decimal128(5, 2)), ('click_through_rate', pa.decimal128(5, 4)),
        ('conversion_rate', pa.decimal128(5, 4)), ('bounce_rate', pa.decimal128(5, 4)),
        ('exit_rate', pa.decimal128(5, 4)), ('engagement_rate', pa.decimal128(5, 4)),
        ('retention_rate', pa.decimal128(5, 4)), ('churn_rate', pa.decimal128(5, 4)),
        # 金额 (13)
        ('revenue_amount', pa.decimal128(15, 2)), ('cost_amount', pa.decimal128(15, 2)),
        ('profit_amount', pa.decimal128(15, 2)), ('margin_amount', pa.decimal128(12, 2)),
        ('tax_amount', pa.decimal128(12, 2)), ('discount_amount', pa.decimal128(10, 2)),
        ('shipping_amount', pa.decimal128(10, 2)), ('ad_spend', pa.decimal128(12, 2)),
        ('cpc_cost', pa.decimal128(10, 4)), ('cpm_cost', pa.decimal128(10, 4)), ('cpa_cost', pa.decimal128(10, 2)),
        ('roi_ratio', pa.decimal128(8, 4)), ('roas_ratio', pa.decimal128(8, 4)),
        # 库存 (7)
        ('stock_quantity', pa.int32()), ('reserved_quantity', pa.int32()), ('available_quantity', pa.int32()),
        ('reorder_level', pa.int16()), ('safety_stock', pa.int16()),
        ('min_stock_level', pa.int16()), ('max_stock_level', pa.int32()),
        # 行为 (11)
        ('login_count', pa.int32()), ('purchase_count', pa.int32()), ('cart_add_count', pa.int32()),
        ('wishlist_count', pa.int32()), ('review_count', pa.int32()), ('rating_avg', pa.decimal128(3, 2)),
        ('rating_count', pa.int32()), ('share_count', pa.int32()), ('like_count', pa.int32()),
        ('comment_count', pa.int32()), ('follow_count', pa.int32()),
        # 性能 (10)
        ('cpu_usage_percent', pa.decimal128(5, 2)), ('memory_usage_mb', pa.int32()),
        ('disk_usage_gb', pa.decimal128(8, 2)), ('network_bytes_sent', pa.int64()),
        ('network_bytes_received', pa.int64()), ('request_count', pa.int32()),
        ('error_count', pa.int32()), ('success_count', pa.int32()),
        ('failure_count', pa.int32()), ('timeout_count', pa.int32()),
        # 质量 (6)
        ('cache_hit_rate', pa.decimal128(5, 3)), ('data_quality_score', pa.decimal128(5, 3)),
        ('accuracy_score', pa.decimal128(5, 3)), ('completeness_score', pa.decimal128(5, 3)),
        ('consistency_score', pa.decimal128(5, 3)), ('timeliness_score', pa.decimal128(5, 3)),
        # 字符串 - 用户 (4)
        ('user_name', pa.string()), ('user_email', pa.string()), ('user_phone', pa.string()), ('user_address', pa.string()),
        # 产品 (6)
        ('product_name', pa.string()), ('product_description', pa.string()), ('product_brand', pa.string()),
        ('product_manufacturer', pa.string()), ('product_category', pa.string()), ('product_type', pa.string()),
        # 订单 (3)
        ('order_type', pa.string()), ('payment_type', pa.string()), ('shipping_method', pa.string()),
        # 地理 (5)
        ('country_name', pa.string()), ('state_name', pa.string()), ('city_name', pa.string()),
        ('postal_code', pa.string()), ('timezone_name', pa.string()),
        # 技术 (8)
        ('ip_address', pa.string()), ('user_agent', pa.string()), ('referrer_domain', pa.string()),
        ('landing_page_url', pa.string()), ('exit_page_url', pa.string()), ('current_page_url', pa.string()),
        ('previous_page_url', pa.string()), ('next_page_url', pa.string()),
        # 营销 (6)
        ('campaign_name', pa.string()), ('ad_group_name', pa.string()), ('keyword_text', pa.string()),
        ('creative_name', pa.string()), ('audience_segment', pa.string()), ('user_tier', pa.string()),
        # 会员 (3)
        ('membership_level', pa.string()), ('subscription_type', pa.string()), ('billing_cycle', pa.string()),
        # 设备 (11)
        ('device_type', pa.string()), ('device_model', pa.string()), ('os_name', pa.string()),
        ('os_version', pa.string()), ('browser_name', pa.string()), ('browser_version', pa.string()),
        ('screen_resolution', pa.string()), ('connection_type', pa.string()), ('carrier_name', pa.string()),
        ('app_name', pa.string()), ('app_version', pa.string()),
        # 版本 (3)
        ('sdk_version', pa.string()), ('api_version', pa.string()), ('protocol_version', pa.string()),
        # 消息 (5)
        ('status_message', pa.string()), ('error_description', pa.string()), ('warning_message', pa.string()),
        ('info_message', pa.string()), ('debug_message', pa.string()),
        # 标识符 (6)
        ('transaction_id', pa.string()), ('batch_id', pa.string()), ('request_id', pa.string()),
        ('correlation_id', pa.string()), ('trace_id', pa.string()), ('span_id', pa.string()),
        # URL (5)
        ('download_url', pa.string()), ('image_url', pa.string()), ('video_url', pa.string()),
        ('document_url', pa.string()), ('thumbnail_url', pa.string()),
        # 日期时间 (20)
        ('event_timestamp', pa.string()), ('session_start_time', pa.string()), ('session_end_time', pa.string()),
        ('page_view_time', pa.string()), ('click_time', pa.string()), ('conversion_time', pa.string()),
        ('purchase_time', pa.string()), ('registration_date', pa.string()), ('last_login_date', pa.string()),
        ('first_purchase_date', pa.string()), ('last_purchase_date', pa.string()),
        ('campaign_start_date', pa.string()), ('campaign_end_date', pa.string()),
        ('promotion_start_date', pa.string()), ('promotion_end_date', pa.string()),
        ('subscription_start_date', pa.string()), ('subscription_end_date', pa.string()),
        ('created_date', pa.string()), ('modified_date', pa.string()), ('processed_date', pa.string()),
        # 半结构化 (15)
        ('event_data', pa.string()), ('user_attributes', pa.string()), ('product_attributes', pa.string()),
        ('order_details', pa.string()), ('payment_info', pa.string()), ('shipping_info', pa.string()),
        ('user_tags', pa.list_(pa.string())), ('product_tags', pa.list_(pa.string())),
        ('viewed_products', pa.list_(pa.string())), ('searched_keywords', pa.list_(pa.string())),
        ('user_preferences', pa.map_(pa.string(), pa.string())),
        ('product_features', pa.map_(pa.string(), pa.string())),
        ('session_attributes', pa.map_(pa.string(), pa.string())),
        ('geo_location', geo_type), ('device_info', device_type),
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
    DuplicateDataGenerator._data_pool = None
    generator = DuplicateDataGenerator(null_ratio=null_ratio, seed=seed)
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
    parser = argparse.ArgumentParser(description='生成 duplicate_key_optimized 表数据 (5排序键+95数值+65字符串+20日期+15半结构化=200列)',
                                      formatter_class=argparse.RawDescriptionHelpFormatter,
                                      epilog='''
使用示例:
  python duplicate_data_generator.py -n 3 -r 10000
  python duplicate_data_generator.py -n 5 -s 100 -t 8
  python duplicate_data_generator.py -n 1 -r 100000 --format parquet
  python duplicate_data_generator.py -n 3 -r 100000 --fast  # 使用多进程加速
''')
    parser.add_argument('-n', '--num-files', type=int, default=1, help='文件数量 (默认: 1)')
    parser.add_argument('-r', '--rows', type=int, help='每文件行数')
    parser.add_argument('-s', '--size-mb', type=float, help='每文件大小限制(MB)')
    parser.add_argument('-o', '--output-dir', default='./generated_data_duplicate', help='输出目录')
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
