# 复杂数据类型说明

## 概述

本工具生成的parquet文件包含多种复杂数据类型。为了确保最大兼容性，这些复杂类型以JSON字符串格式存储。

## 支持的复杂类型

### 1. ARRAY 类型 - `product_id_list`

**原始SQL定义**: `ARRAY<BIGINT>`

**存储格式**: JSON字符串

**示例**:
```python
import pandas as pd
import json

df = pd.read_parquet('test_data.parquet')

# 读取ARRAY数据
product_ids_json = df['product_id_list'].iloc[0]  # '[945270, 674532, 905247]'
product_ids = json.loads(product_ids_json)         # [945270, 674532, 905247]

# 处理数据
for product_id in product_ids:
    print(f"商品ID: {product_id}")
```

### 2. MAP 类型 - `ext_kv_map`

**原始SQL定义**: `MAP<STRING, STRING>`

**存储格式**: JSON字符串

**示例**:
```python
import pandas as pd
import json

df = pd.read_parquet('test_data.parquet')

# 读取MAP数据
kv_map_json = df['ext_kv_map'].iloc[0]  # '{"source": "value_77", "medium": "value_42"}'
kv_map = json.loads(kv_map_json)        # {"source": "value_77", "medium": "value_42"}

# 访问键值
source = kv_map.get('source')
medium = kv_map.get('medium')
```

### 3. STRUCT 类型 - `user_profile`

**原始SQL定义**: `STRUCT<age INT, gender STRING, level INT>`

**存储格式**: JSON字符串

**示例**:
```python
import pandas as pd
import json

df = pd.read_parquet('test_data.parquet')

# 读取STRUCT数据
profile_json = df['user_profile'].iloc[0]  # '{"age": 39, "gender": "男", "level": 4}'
profile = json.loads(profile_json)         # {"age": 39, "gender": "男", "level": 4}

# 访问字段
age = profile['age']
gender = profile['gender']
level = profile['level']
```

### 4. JSON 类型 - `ext_json`

**原始SQL定义**: `JSON`

**存储格式**: JSON字符串

**示例**:
```python
import pandas as pd
import json

df = pd.read_parquet('test_data.parquet')

# 读取JSON数据
ext_json_str = df['ext_json'].iloc[0]
ext_data = json.loads(ext_json_str)

# 访问自定义字段
field1 = ext_data.get('extra_field_1')
field2 = ext_data.get('extra_field_2')
```

## 批量处理示例

### 解析所有复杂类型

```python
import pandas as pd
import json

df = pd.read_parquet('test_data.parquet')

# 批量解析ARRAY类型
df['product_id_list_parsed'] = df['product_id_list'].apply(
    lambda x: json.loads(x) if pd.notna(x) else None
)

# 批量解析MAP类型
df['ext_kv_map_parsed'] = df['ext_kv_map'].apply(
    lambda x: json.loads(x) if pd.notna(x) else None
)

# 批量解析STRUCT类型
df['user_profile_parsed'] = df['user_profile'].apply(
    lambda x: json.loads(x) if pd.notna(x) else None
)

# 批量解析JSON类型
df['ext_json_parsed'] = df['ext_json'].apply(
    lambda x: json.loads(x) if pd.notna(x) else None
)

# 展开STRUCT字段
df['profile_age'] = df['user_profile_parsed'].apply(
    lambda x: x['age'] if x else None
)
df['profile_gender'] = df['user_profile_parsed'].apply(
    lambda x: x['gender'] if x else None
)
df['profile_level'] = df['user_profile_parsed'].apply(
    lambda x: x['level'] if x else None
)
```

## 在Spark中使用

### 读取并解析JSON字符串

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").getOrCreate()

# 读取parquet文件
df = spark.read.parquet('test_data.parquet')

# 定义ARRAY schema并解析
df = df.withColumn(
    'product_id_list_parsed',
    from_json(col('product_id_list'), ArrayType(LongType()))
)

# 定义MAP schema并解析
df = df.withColumn(
    'ext_kv_map_parsed',
    from_json(col('ext_kv_map'), MapType(StringType(), StringType()))
)

# 定义STRUCT schema并解析
profile_schema = StructType([
    StructField('age', IntegerType()),
    StructField('gender', StringType()),
    StructField('level', IntegerType())
])
df = df.withColumn(
    'user_profile_parsed',
    from_json(col('user_profile'), profile_schema)
)

# 访问STRUCT字段
df = df.withColumn('profile_age', col('user_profile_parsed.age'))
df = df.withColumn('profile_gender', col('user_profile_parsed.gender'))
df = df.withColumn('profile_level', col('user_profile_parsed.level'))

df.show()
```

## 在Hive/Presto中使用

### 创建表并解析JSON

```sql
-- 创建外部表
CREATE EXTERNAL TABLE test_data (
    biz_id INT,
    user_id BIGINT,
    channel_code STRING,
    event_date STRING,
    -- ... 其他字段 ...
    product_id_list STRING,      -- JSON字符串
    ext_kv_map STRING,            -- JSON字符串
    user_profile STRING,          -- JSON字符串
    ext_json STRING               -- JSON字符串
)
STORED AS PARQUET
LOCATION '/path/to/parquet/files';

-- 解析ARRAY类型
SELECT 
    biz_id,
    cast(get_json_object(product_id_list, '$[0]') as bigint) as first_product_id
FROM test_data;

-- 解析MAP类型
SELECT 
    biz_id,
    get_json_object(ext_kv_map, '$.source') as source,
    get_json_object(ext_kv_map, '$.medium') as medium
FROM test_data;

-- 解析STRUCT类型
SELECT 
    biz_id,
    cast(get_json_object(user_profile, '$.age') as int) as profile_age,
    get_json_object(user_profile, '$.gender') as profile_gender,
    cast(get_json_object(user_profile, '$.level') as int) as profile_level
FROM test_data;
```

## 为什么使用JSON字符串格式？

### 优点

1. **兼容性好**: JSON是通用格式，几乎所有工具都支持
2. **易于调试**: 可以直接查看数据内容
3. **灵活性高**: 容易修改schema结构
4. **工具支持**: pandas、Spark、Hive等都有很好的JSON解析支持

### 注意事项

1. **需要解析**: 使用时需要先解析JSON字符串
2. **性能**: 解析JSON会有一定的性能开销
3. **空值处理**: 需要判断空值后再解析

## 性能优化建议

1. **批量解析**: 使用vectorized操作而不是逐行处理
2. **缓存结果**: 解析一次后缓存结果，避免重复解析
3. **按需解析**: 只解析需要使用的字段
4. **使用Spark**: 大数据量时使用Spark处理，性能更好

## 示例脚本

完整的使用示例请参考 `read_example.py`。
