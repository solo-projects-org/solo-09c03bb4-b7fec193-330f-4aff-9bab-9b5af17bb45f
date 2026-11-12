# 渡轮结算 · Iceberg v2 票号删除核对

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记。网页接收有效文件清单和对应 Parquet 副本，输入整数
票号，逐条给出原文件、从 0 起算的文件内行号、是否已删除以及命中的删除文件。

网页还提供 **等值删除 → 位置删除转换入口**：批量退票若以（票号、售票渠道）
等值删除（equality delete）落湖，核查页无法识别，可在本页转换成位置删除
ZIP，再与原数据和已有位置删除副本一起交给核查页。


## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 等值删除 → 位置删除转换

### 输入要求

上传 **同一分区定义、同一航次日** 的一组文件：

1. 与核对页同格式的 Iceberg v2 有效文件清单（CSV 或 JSON），其中恰好包含
   **一个** `equality_delete` 条目；
2. 清单中的 **全部数据 Parquet**；
3. 唯一的 **等值删除 Parquet** 副本；
4. 票号字段 ID 与售票渠道字段 ID（整数）。

清单里已有的 `position_delete` 条目原样保留，不要求上传其 Parquet 副本；
未出现在清单中的上传文件会被忽略并给出警告。

### 键列绑定规则

- 按 **Iceberg 字段 ID（Parquet `field_id` 元数据）** 在每个文件的顶层列中
  找键列，不按列名匹配；
- 每个字段 ID 必须在每个文件中 **唯一绑定** 到一个顶层 `int32` 或 `int64`
  列。找不到、绑定到多列、类型不是 int32/int64，都会中止并指出文件名；
- 售票渠道列允许跨文件 **异名**（如数据里叫 `sales_channel`，删除文件里叫
  `channel_id`）；数据副本之间的票号列必须 **同名**；
- 两个字段 ID 不能相同。

### 匹配与序列号规则

- 等值删除的一行键（票号、渠道）与数据行 **两个键值都相等** 才算命中，
  采用 SQL 空值语义：**空值只与空值相等**；
- 仅当等值删除文件的数据序列号 **严格大于** 数据文件的数据序列号时，才为
  命中行写出位置删除；序列号相等或更小的数据文件整体跳过并在结果页说明；
- 重复的等值删除行按去重后的键处理，同一数据行最多产生一条位置删除；
- 写出的每条记录包含数据文件的 **原路径** 和从 0 起算的 **文件内行号**。

### 分批与输出

- 数据与等值删除都按固定批大小（默认 10000 行，可在表单覆盖）分批读取；
  等值删除键落盘到请求临时目录中的 SQLite 库，**不在内存中攒整份数据或全部
  撤销键**，转换结束后临时库自动删除；
- 新位置删除 Parquet 为 `file_path`（字符串）+ `pos`（int64）两列，文件名由
  等值删除文件名派生（`delete-eq-*` → `delete-pos-*`）；
- 更新后的清单把该等值删除条目 **替换为 `position_delete` 条目，沿用原删除
  序列号**，其他条目（路径、文件名、类型、序列号）一律不变；
- 下载的 ZIP 内含新 Parquet 与 `manifest.csv`，可与原数据和已有位置删除
  副本一同上传到核对页。即使没有任何命中，也会生成不含记录的空位置删除
  文件并给出提示。
- **全有或全无**：缺件、键列无法唯一绑定、任一文件读写失败都会中止，明确
  列出文件名，不产出任何转换包。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此被忽略。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

### 转换样例（`sample-convert/`）

由 `tools/make_sample_convert.py` 生成，点网页上的 **“载入转换样例”** 运行：

- 字段 ID `10` 是票号（数据副本列名均为 `ticket_no`），字段 ID `11` 是渠道：
  数据文件里叫 `sales_channel` / `sale_channel`，等值删除文件里叫
  `channel_id`——**渠道改名仍按字段 ID 匹配**；
- 包含空渠道键，演示空值只与空值相等；
- `data-cv-0003.parquet` 的数据序列号（12）与等值删除序列号（12）相同，
  其中的同键票据 **不会新增删除记录**；
- 等值删除里有一行重复键，去重后只产生一条位置删除；
- 清单中已有的 `delete-pos-existing-0001.parquet` 位置删除条目原样保留。

重新生成转换样例：

```bash
python3 tools/make_sample_convert.py
```

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 64 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                        Flask 路由、上传处理、票号解析、转换 ZIP 下载
checker/manifest.py           清单解析与校验
checker/engine.py             PyArrow 行组跳读、行号计算、位置删除匹配
checker/converter.py          等值删除→位置删除：字段 ID 绑定、分批流式转换
checker/errors.py             CheckError（始终带文件名）
templates/index.html          网页（核对页 + 转换入口）
static/style.css              样式
sample/                       核对内置样例（清单 + Parquet）
sample-convert/               转换内置样例（含渠道异名、空值、同序列号场景）
tools/make_sample.py          核对样例生成脚本
tools/make_sample_convert.py  转换样例生成脚本
tests/                        pytest 测试
Dockerfile                    容器镜像
docker-compose.yml            启动配置
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；适用于常见规模。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
