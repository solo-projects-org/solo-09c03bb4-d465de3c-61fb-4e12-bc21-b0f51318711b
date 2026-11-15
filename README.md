# 地震波形原档接收台

面向地震台数据员的卫星链路波形归档服务：波形原档被切成 **256 字节定长帧**分批送达，
服务逐帧接收、按校验帧恢复单帧丢失、按原档字节数截断并以 **SHA-256** 核验通过后，
才原子发布可供震相分析下载的完整档案。**丢一帧不再丢弃整档。**

技术栈：FastAPI · SQLite（WAL）· Jinja2 网页 · 原生文件系统分片存储 · Docker Compose。

---

## 一分钟启动

```bash
docker compose up --build
# 打开 http://localhost:8080
```

数据（SQLite 元数据、分片、已发布档案）保存在命名卷 `archive-data` 的 `/data`；
`./samples` 以只读方式挂载到容器内 `/app/samples`。修改端口：`INTAKE_PORT=9000 docker compose up`。

不用 Docker 的本地运行：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8080
# 数据目录默认 ./data，可用 ARCHIVE_DATA_DIR=/srv/archive 覆盖
```

---

## 使用流程

1. **登记传输号**：填写传输号、数据帧数（2～16）、原档真实字节数、截断后的 SHA-256。
   登记参数不可变；同传输号重复登记相同参数是幂等的，参数不同会拒收，避免串档。
2. **分批上传 CSV**：CSV 每行一帧，载荷为该帧 256 字节的十六进制（**512 个十六进制字符**）。
3. **（可选）上传校验帧**：所有数据帧（含末帧补零）逐字节异或得到的 256 字节。
4. 系统自动判定：齐帧直接组装；缺一帧且已有校验帧则恢复后组装；缺多帧则继续等待补传。
5. 组装结果按原档字节数截断并核对 SHA-256，通过后原子发布并可下载；不通过则保留全部分片，
   等待补帧后重新核验（网页上有「重新核验现有分片」按钮）。

### CSV 格式

允许两种列布局，表头可选，空行与 `#` 注释会被忽略：

```csv
transfer_id,frame_no,payload_hex
STA-20261115-001,1,6bfffb01...（512 个十六进制字符）
STA-20261115-001,2,...
STA-20261115-001,P,...        # 校验帧，帧号用 P / parity / xor / 校验 均可
```

```csv
frame_no,payload_hex
1,6bfffb01...
```

- 三列时第一列是传输号，必须与本次上传的传输号一致；两列时传输号取自 URL。
- 一帧一行；同一批内帧号重复且内容不同、或与已存分片冲突，**整批拒收**。
- 校验帧可以混在数据帧 CSV 中，也可以单独上传一帧。

### 状态含义

| 状态 | 含义 | 能否下载 |
| --- | --- | --- |
| 等待补帧 | 缺 2 帧及以上（或只缺 1 帧但无校验帧） | 否 |
| 摘要不符 | 分片已齐（含校验帧恢复）但截断后摘要不匹配；分片保留 | 否 |
| 已核验发布 | 截断后摘要匹配，档案已原子发布 | 是 |

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 传输台账网页 |
| POST | `/deliveries` | 网页表单登记（303 跳转） |
| POST | `/deliveries/{tid}/frames` | 网页表单上传 CSV（文件或粘贴，multipart） |
| POST | `/deliveries/{tid}/parity` | 网页表单上传校验帧 |
| POST | `/deliveries/{tid}/verify` | 重新核验现有分片 |
| GET | `/deliveries/{tid}/download` | 下载已发布档案，响应头带 `X-Archive-SHA256` |
| POST | `/api/deliveries` | JSON 登记 |
| POST | `/api/deliveries/{tid}/frames` | JSON 上传 `{"frames":[{"frame_no":1,"payload_hex":"..."}]}` |
| POST | `/api/deliveries/{tid}/parity` | JSON 上传 `{"payload_hex":"..."}` |
| GET | `/api/deliveries` / `/api/deliveries/{tid}` | 台账 / 单档状态（含缺帧、恢复帧、实际摘要） |
| GET | `/api/deliveries/{tid}/download` | 下载 |
| GET | `/docs` | 交互式接口文档 |

错误约定：参数错误 `400`，同帧号不同内容等冲突 `409`，未知传输号 / 未发布 `404`。

---

## 样例（虚构波形）

`samples/` 下四组确定性生成的**合成**波形（16 位小端、带 P 波包络的伪噪声，非真实台站数据），
每组含 `meta.json`、完整原档 `expected.bin`、按卫星送达顺序切好的 CSV：

| 目录 | 场景 | 预期 |
| --- | --- | --- |
| `STA-20261115-001` | 4 帧分两批完整送达 | 第二批后发布 |
| `STA-20261115-002` | 缺第 3 帧，带校验帧 | 上传 `parity.csv` 后恢复并发布 |
| `STA-20261115-003` | 缺第 2、5 帧，带校验帧 | 只能等待补传，不发布 |
| `STA-20261115-004` | 帧齐全但声明摘要是错的 | 摘要不符，保留分片、拒绝下载 |

用网页逐批上传，或用自带脚本一键送达：

```bash
python3 scripts/send_sample.py samples/STA-20261115-002 --base-url http://localhost:8080
```

重新生成样例（不要提交运行数据目录）：

```bash
python3 scripts/make_samples.py
```

---

## 可靠性与并发保证

- **原子发布**：先写入 `tmp/*.part`，`fsync` 后 `rename` 到暂存名，再 `rename` 为
  `published/<传输号>/<传输号>-<sha256>.bin`，并 `fsync` 目录，最后才在 SQLite 中记录发布。
  任何时刻中断，下载端只会看到「不存在」或「完整文件」，绝不会拿到半截档。
- **重启自愈**：启动时清理临时分片文件；对已具备组装条件的等待档重试发布，
  覆盖「文件已 rename 但数据库提交前崩溃」的窗口。已发布但文件丢失/被篡改时下载失败关闭。
- **只发布一次**：发布行带唯一内容寻址文件名，发布后迟到、重复或并发的分片都不会改变已发布档案；
  下载前按登记大小与摘要再次核对。
- **并发安全**：进程内线程锁 + `fcntl` 文件锁串行化「写入 → 组装 → 发布」，SQLite 使用
  `BEGIN IMMEDIATE` 与 WAL。多 worker / 多进程同时补齐同一传输号也只产生一份发布。
- **冲突拒收**：同传输号同帧号不同内容（含同批内冲突、校验帧冲突）整批拒绝，已存分片不变。
- **恢复帧不落库**：校验帧恢复的帧只用于当次组装的内存。它不会被记为已收帧，
  真实帧补传时仍会被接受（可修正错误恢复），也就不会因「同帧号不同内容」而拒收。

### 一个明确的取舍

同帧号不同内容一律拒收，因此**摘要不符的档案不能原地覆盖损坏分片**：系统只能保留分片、
等待缺失帧补齐后重验。若确认某帧在源端损坏，请用新的传输号重新登记送达。
这是「拒收冲突」与「可覆盖修复」之间的有意取舍，优先保证同一传输号内容不可被悄悄替换。

---

## 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

覆盖：CSV 解析与校验、缺单帧校验帧恢复、缺多帧等待、末帧补零截断、摘要不符保留分片、
同帧号冲突拒收、幂等重传、同批冲突、发布后迟到分片不改档、写盘中断与重启自愈、
已发布文件被截断时拒绝下载、多线程/多进程并发补齐只发布一份。

---

## 目录结构

```
app/
  main.py        FastAPI 路由（网页 + JSON API）
  storage.py     分片入库、XOR 恢复、原子发布、SQLite 元数据
  csvintake.py   CSV 解析与帧校验
  templates/     Jinja2 页面
  static/        样式
samples/         虚构波形分片与期望原档
scripts/         样例生成、样例送达脚本
tests/           pytest 用例
```
