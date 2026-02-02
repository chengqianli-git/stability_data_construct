# 快速入门指南

## 安装

```bash
pip install pandas pyarrow
```

或

```bash
pip install -r requirements.txt
```

## 快速开始

### 1. 生成测试数据

最简单的方式（生成10,000行）：
```bash
python3 generate_test_data.py
```

生成指定行数：
```bash
python3 generate_test_data.py -n 100000
```

生成指定大小的文件（推荐）：
```bash
# 生成约100MB的文件
python3 generate_test_data.py -s 100MB

# 生成约1GB的文件
python3 generate_test_data.py -s 1GB
```

使用多进程加速（推荐）：
```bash
python3 generate_test_data.py -n 1000000 -p 8
# 或按大小生成
python3 generate_test_data.py -s 500MB -p 8
```

### 2. 生成多个文件

```bash
# 生成10个文件，每个约10MB
python3 generate_test_data.py -s 100MB -f 10

# 生成5个文件，总共50万行
python3 generate_test_data.py -n 500000 -f 5
```

### 3. 读取和验证数据

```bash
python3 read_example.py test_data.parquet
```

### 3. 在Python中使用

```python
import pandas as pd

# 读取parquet文件
df = pd.read_parquet('test_data.parquet')

# 查看数据
print(f"数据量: {len(df):,} 行")
print(df.head())

# 检查必填字段
required_fields = ['biz_id', 'user_id', 'channel_code', 'event_date']
for field in required_fields:
    null_count = df[field].isnull().sum()
    print(f"{field}: {null_count} 个空值")  # 应该都是0

# 查看数据分布
print(df['channel_code'].value_counts())
print(df['platform'].value_counts())
```

## 参数说明

| 参数 | 说明 | 示例 |
|------|------|------|
| `-n, --rows` | 生成行数（与-s二选一） | `-n 1000000` (生成100万行) |
| `-s, --size` | 目标文件大小（与-n二选一） | `-s 100MB` 或 `-s 1GB` |
| `-f, --files` | 生成文件数量 | `-f 10` (生成10个文件) |
| `--per-file` | 指定-s为每个文件的大小 | `-s 100MB -f 10 --per-file` (每个100MB) |
| `-p, --processes` | 进程数 | `-p 8` (使用8个进程) |
| `-o, --output` | 输出文件名 | `-o data.parquet` |
| `--format` | 复杂类型格式 | `--format native` (StarRocks/Spark) 或 `--format json` (兼容模式) |

## 常见用例

### 测试场景（小文件）
```bash
# 按行数
python3 generate_test_data.py -n 100 -o test_small.parquet

# 按大小（推荐）
python3 generate_test_data.py -s 500KB -o test_small.parquet
```

### 开发场景（中等文件）
```bash
# 按行数
python3 generate_test_data.py -n 50000 -p 4 -o test_dev.parquet

# 按大小（推荐）
python3 generate_test_data.py -s 10MB -p 4 -o test_dev.parquet
```

### 性能测试（大文件）
```bash
# 按行数
python3 generate_test_data.py -n 5000000 -p 16 -o test_perf.parquet

# 按大小（推荐）
python3 generate_test_data.py -s 1GB -p 16 -o test_perf.parquet
```

### 导入StarRocks（推荐使用native格式）
```bash
# 生成适合StarRocks的数据（默认就是native格式）
python3 generate_test_data.py -n 1000000 -p 8 -o starrocks_data.parquet

# 或显式指定
python3 generate_test_data.py -n 1000000 -p 8 -o starrocks_data.parquet --format native
```

## 数据特点

✅ **必填字段**: `biz_id`, `user_id`, `channel_code`, `event_date` 保证不为空
✅ **数据均匀**: 所有枚举值和数值均匀分布
✅ **空值率**: 非必填字段约5%为空
✅ **复杂类型**: 支持ARRAY、MAP、STRUCT、JSON
✅ **压缩格式**: GZIP压缩，压缩率高

## 按大小生成说明

当使用 `-s/--size` 参数时：
1. 程序会先生成样本数据来测量平均行大小（样本量根据目标大小自动调整）：
   - **小文件（<10MB）**: 1,000-5,000行样本
   - **中等文件（10MB-100MB）**: 5,000-10,000行样本
   - **大文件（100MB-1GB）**: 10,000行样本
   - **超大文件（>1GB）**: 20,000行样本（提高精度）
2. 根据文件大小采用不同的精度策略：
   - **小文件（<10MB）**: 使用完整迭代，精度±2%以内
   - **中等文件（10MB-100MB）**: 使用快速测试（10%样本），精度±5%以内
   - **大文件（>100MB）**: 跳过迭代，使用大样本估算，精度±5%以内
3. 实际文件大小与目标大小的偏差通常在 **±5%以内**（小文件在±2%以内）

**精度示例**:
```bash
# 小文件（100KB）- 完整迭代
$ python3 generate_test_data.py -s 100KB
迭代 1: 生成 170 行, 实际大小: 126.57 KB, 偏差: +26.6%
迭代 2: 生成 134 行, 实际大小: 107.31 KB, 偏差: +7.3%
迭代 3: 生成 124 行, 实际大小: 101.24 KB, 偏差: +1.2%
✓ 精度满足要求（偏差 +1.2%）
# 最终文件: 100.01 KB (偏差 +0.01%)

# 中等文件（100MB）- 快速测试
$ python3 generate_test_data.py -s 100MB
快速测试: 生成 18,698 行 (9.35MB), 推算需要: 200,039 行
# 最终文件: 约100MB (偏差±5%)

# 大文件（1GB）- 使用大样本估算
$ python3 generate_test_data.py -s 1GB
使用样本量: 20,000 行（目标: 1024.00 MB）
样本文件大小: 10186.66 KB (20000 行)
平均每行: 521.56 字节
使用样本估算: 2,058,723 行 → 调整后: 2,161,659 行（精度约±5%）
# 估算耗时: <4秒，使用20,000行样本提高精度
# 最终文件: 0.96GB (偏差 -3.96%) ✓
```

**支持的大小单位**:
- `KB` 或 `K`: 千字节
- `MB` 或 `M`: 兆字节（默认）
- `GB` 或 `G`: 吉字节
- `TB` 或 `T`: 太字节

**示例**:
```bash
python3 generate_test_data.py -s 100MB    # 约100MB
python3 generate_test_data.py -s 1.5GB   # 约1.5GB
python3 generate_test_data.py -s 500M    # 约500MB
```

## 生成多个文件

使用 `-f/--files` 参数可以一次生成多个文件：

**文件命名规则**:
- 单文件: `test_data.parquet`
- 多文件: `test_data_part_001.parquet`, `test_data_part_002.parquet`, ...

**数据分配**:
- 按行数生成: 总行数平均分配到各个文件
- 按总大小生成: 每个文件大小约为 `目标大小 / 文件数`
- 按单文件大小生成（`--per-file`）: 每个文件大小约为指定大小

**适用场景**:
- ✅ 分布式系统测试（HDFS、S3等）
- ✅ 并行导入测试（多线程/多进程导入）
- ✅ 数据分片场景模拟
- ✅ 避免单文件过大

**示例1: 按总大小生成**
```bash
# 生成10个文件，总共1GB（每个约100MB）
python3 generate_test_data.py -s 1GB -f 10

# 生成3个文件，总共500KB（每个约167KB）
python3 generate_test_data.py -s 500KB -f 3
```

**示例2: 按每个文件大小生成（新功能）** ✨
```bash
# 生成10个文件，每个100MB（总共约1GB）
python3 generate_test_data.py -s 100MB -f 10 --per-file

# 生成5个文件，每个200MB（总共约1GB）
python3 generate_test_data.py -s 200MB -f 5 --per-file

# 生成3个文件，每个500KB（总共约1.5MB）
python3 generate_test_data.py -s 500KB -f 3 --per-file
```

**示例3: 按行数生成**
```bash
# 生成5个文件，总共100万行（每个约20万行）
python3 generate_test_data.py -n 1000000 -f 5

# 生成3个文件用于StarRocks分片导入
python3 generate_test_data.py -s 500MB -f 3 -o sr_data.parquet
# 输出: sr_data_part_001.parquet, sr_data_part_002.parquet, sr_data_part_003.parquet
```

**对比说明**:
```bash
# 不使用 --per-file: 总大小500KB，分3个文件
python3 generate_test_data.py -s 500KB -f 3
# 结果: 每个文件约167KB，总共约500KB

# 使用 --per-file: 每个文件500KB，共3个文件
python3 generate_test_data.py -s 500KB -f 3 --per-file
# 结果: 每个文件约500KB，总共约1.5MB
```

## 性能参考

| 数据量 | 进程数 | 预计耗时 | 文件大小 |
|--------|--------|----------|----------|
| 1万行 | 8 | 2-3秒 | ~3MB |
| 10万行 | 8 | 15-20秒 | ~30MB |
| 100万行 | 8 | 2-3分钟 | ~300MB |
| 1000万行 | 16 | 15-20分钟 | ~3GB |

*性能取决于CPU核心数和磁盘速度*

## 故障排除

### 1. 导入错误
```
ModuleNotFoundError: No module named 'pandas'
```
解决方法：
```bash
pip install pandas pyarrow
```

### 2. 内存不足
生成大量数据时可能内存不足，建议分批生成：
```bash
# 分10次生成，每次100万行
for i in {1..10}; do
    python3 generate_test_data.py -n 1000000 -o test_part_$i.parquet
done
```

### 3. 磁盘空间不足
生成前检查磁盘空间：
```bash
df -h .
```
每100万行约需要300MB空间。

## 下一步

- 查看 [README.md](README.md) 了解完整功能
- 查看 [usage_examples.sh](usage_examples.sh) 了解更多示例
- 修改 `generate_test_data.py` 自定义数据生成逻辑
