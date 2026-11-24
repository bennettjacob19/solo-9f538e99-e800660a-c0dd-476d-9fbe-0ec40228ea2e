# 跨机房镜像层分发演练

一个 FastAPI 应用，用网页复现"多台构建机同时拉同一个镜像层"的跨机房分发：
**只发起一次 HTTP 主下载**，快、慢两路同时消费同一份响应；慢端收到约 3/4 后暂停，
网关为它留存正文；快端收齐后慢端续传，缺的尾巴再用条件 Range 回源补齐。
最后两路都按**主下载当时那一版**的 SHA-256 校验。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

- 网页与 API：<http://localhost:8000/>
- 源站（独立服务）：<http://localhost:8001/origin/state>

compose 把源站（`app.origin_app`）和网关（`app.gateway_app`）拆成两个容器，
网关通过 `DRILL_ORIGIN_BASE=http://origin:8001` 回源，源站健康检查通过后网关才启动。

### 本地单进程（源站与网页同一进程）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

打开 <http://localhost:8000/> 即可。

### 本地双进程（与 compose 拓扑一致）

```bash
DRILL_LAYER_BYTES=8388608 uvicorn app.origin_app:app --port 8001 &
DRILL_ORIGIN_BASE=http://127.0.0.1:8001 uvicorn app.gateway_app:app --port 8000
```

## 演练流程

1. 每次开始演练先把源站重置为 `v1`（长度 = `size_bytes`），并记录基线。
2. 网关向源站发出**唯一一次**主下载 `GET /origin/layer`，响应带强 `ETag`
   （形如 `"v1-<sha256>"`，无 `W/` 前缀）、`Content-Length` 和
   `X-Content-SHA256`。
3. 同一份字节流同时喂给快、慢两个接收端（各自独立计算 SHA-256）。
4. 慢端到达 `pause_fraction`（默认 3/4）后暂停，之后网关把剩余正文尽量放进
   留存缓冲区：先占内存到 `retention_memory_bytes`，再溢写到匿名临时文件，
   两者合计不超过 `retention_bytes`；超出部分直接丢弃。
5. 快端收齐后，慢端从留存缓冲区续传。仍缺的尾段向源站发
   `Range: bytes=<start>-<end>` + `If-Range: <主下载的强 ETag>`。
   只有同时满足以下条件才接收补段：
   - 状态码 `206`（`If-Range` 失效时源站按 RFC 9110 返回 `200` 新版本）；
   - 响应 `ETag` 是强校验器且与主下载完全一致；
   - `Content-Range` 的起止与总长同请求及主下载一致；
   - 响应 `X-Content-SHA256` 等于主下载那一版的摘要。
   任一不符则**拒绝补段**，慢端只报缺段，绝不混版本。
6. 总额度判断：慢端暂停时的尾段长度若不超过 `留存额度 + 回源额度`，源站未换版时
   两路必须收齐；额度不足或版本已换成新版导致旧版补不齐时，只有慢端报缺段
   （`outcome = slow_missing`，`missing_segments` 精确到字节区间）。

网页实时显示：

- 两路**实收字节**与 SHA-256 校验结果（通过 / 未收齐 / 不匹配）；
- 慢端暂停点、续传状态、缺段区间；
- 补段的 ETag / 区间校验结果与额度余量；
- **源站实发字节**（主下载 + 补段累计，来自源站计数器）；
- **网关暂存峰值**（总峰值及内存 / 磁盘各自峰值）；
- 源站是否在暂停时被换成等长新版，以及主下载仍继续发原版的事实。

## 参数

| 参数 | 含义 |
| --- | --- |
| `size_bytes` | 层大小（1 KiB–128 MiB） |
| `chunk_size` / `chunk_delay_ms` | 主下载分块与节流，控制两路快慢节奏 |
| `retention_bytes` | 网关为慢端留存的正文额度（**内存 + 磁盘合计**） |
| `retention_memory_bytes` | 其中允许占用内存的上限，其余溢写磁盘 |
| `backfill_bytes` | 允许额外回源的正文额度 |
| `pause_fraction` | 慢端暂停点，默认 `0.75` |
| `auto_switch_on_pause` | 慢端暂停时把源站换成等长 `v2`（主下载仍发 `v1`） |

`retention_memory_bytes` 不得大于 `retention_bytes`，否则 422。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/drill` | 当前演练状态（含源站计数器、留存峰值） |
| `POST` | `/api/drill` | 开始演练，body 同参数表 |
| `POST` | `/api/drill/reset` | 取消当前演练并重置源站 |
| `POST` | `/api/drill/switch?version=v2` | 手动换版（演练中会记录来源 `manual`） |
| `GET` | `/api/origin` | 源站状态与两个版本 |
| `GET` | `/origin/layer` | 源站正文（支持单段 Range / If-Range） |
| `POST` | `/origin/switch` | 源站换版 |
| `POST` | `/origin/reset` | 源站重置（可带 `size_bytes`） |

## 验证

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # 单元 + 端到端（真实 HTTP 主下载/补段）
python scripts/selftest.py   # 双进程起 origin+gateway，跑 5 个场景并打印表格
```

`scripts/selftest.py` 覆盖：

1. 额度够补尾段 → 两路都收齐、两路 SHA-256 通过、源站只收到 1 次主下载；
2. 留存 + 回源额度都不足 → 快端收齐，**只有慢端报缺段**；
3. 暂停时换等长新版、尾段未全部留存 → 补段被拒（ETag 不符 / 源站回 200），
   慢端报缺段，快端仍按原版校验通过；
4. 暂停时换版但尾段全在留存缓冲区 → 不回源，两路都按原版收齐；
5. 暂停中手动换版 → 记录 `manual`，仍然只有慢端受影响。

## 已知限制

- 演练中的"快端/慢端"是同进程内的两个消费者，用来模拟同机房快机与远机房慢机；
  网络层的真实丢包/重传不在本次范围。
- 源站的版本切换与计数器都是进程内状态，重启即清零；多网关实例共享同一源站时
  计数器会累加，需要各自重置后观察。
- 补段按"一片连续尾段"处理；当前设计慢端只会缺尾部连续区间，够用但未实现任意
  多段 holes 的并行补齐。

## 自测记录（2026-11-23）

- `python -m pytest -q`：8 passed（留存缓冲区溢写/上限、等长双版本、端到端两路收齐、
  额度不足、暂停换版、If-Range 失效回 200）。
- `python scripts/selftest.py`：5 个场景全部通过，上表即其输出。
- 网页渲染另用 jsdom 对 `slow_missing` 场景做了字段级检查（实收字节、校验结果、
  缺段区间、补段 ETag/区间、源站实发、暂存峰值），全部命中。
- 未执行项：本机无 Docker，`docker compose up --build` 未实际构建验证；compose 的
  YAML、健康检查与双服务拆分已核对，并以同拓扑的双进程方式手工跑通。
