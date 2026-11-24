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

源站在 compose 中设有 **96 MiB 内存上限**（`mem_limit: 96m` 与
`deploy.resources.limits.memory: 96M`，二者都解析为 `100663296` 字节）。
为了在该上限内仍能演练 **128 MiB** 镜像层，源站不再把整层 `bytes` 放进内存：
它用固定种子、按 64 KiB 块（4 字节的整数倍，分块 `randbytes` 与一次性
`randbytes(size)` 逐字节相同）生成正文并哈希，落到
`DRILL_ORIGIN_DATA_DIR`（compose 中为 `/tmp/origin-layers`，容器可写层磁盘，
**不是 tmpfs**）的两个层文件；发请求时按区间读文件并逐字节计量。源站实测
峰值 RSS 约 50–52 MiB；compose 健康检查刻意只用 `socket` 发一个最小
HTTP 探测（不导入 urllib/ssl，临时进程 VmRSS 约 11–12 MiB），保守同峰
合计约 63–66 MiB，低于 96 MiB。

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

### 小区间读取用量与重设大小的代际回收

`GET /origin/state` 的 `state.requests[]` 为每个 `/origin/layer` 请求给出实际
“定位 + 发送”正文用量：

- `body_bytes_used`（同义别名 `work_bytes`）= `positioning_bytes`
  + `storage_read_bytes` + `generated_bytes`；
- 文件 `seek` 定位不读正文，因此 `positioning_bytes = 0`，206 小区间的用量
  就等于实际读出的区间长度；
- 当响应为 `206` 且 `range_length <= 65536`（64 KiB）时，
  `work_limit_bytes = 1048576`（1 MiB），并用布尔 `work_within_limit`
  标明 `body_bytes_used <= 1 MiB`；其它请求该判定为 `null`（规则不适用）；
- 同一份字段还放在 `requests[].work` 嵌套对象里。

`state.storage` 给出层文件代际（generation）信息：`generations[].users`
是仍在发送中的租约数，`retired` 表示已被一次成功的重设大小取代，
`reclaimed_generations` 是发完/断开后已删文件并回收的旧代数。

重设大小的语义：

- 新大小与当前不同：先在锁外生成新一代 `v1/v2` 层文件并算好摘要，再原子换入；
  **成功后新请求取新层**（新长度 / 新 ETag）。
- 已经开始的响应持有旧代际租约与独立文件句柄，按**原响应头**（原
  `Content-Length` / 强 ETag / `X-Content-SHA256`）把旧正文发完；
  最后一个旧下载正常结束或断开（生成器收到 `GeneratorExit` /
  `CancelledError`）时释放租约，旧代际文件立即 unlink 回收。
- 生成新一代失败（如磁盘/权限错误）时 `/origin/reset` 返回 500，旧代际
  不动，原层仍可正常 GET / Range 下载。
- 相同大小的 reset 沿用原层文件（与旧实现“同尺寸不重建版本”一致）。

## 验证

```bash
pip install -r requirements-dev.txt
python -m pytest -q          # 单元 + 端到端（真实 HTTP 主下载/补段/代际回收）
python scripts/selftest.py   # 双进程起 origin+gateway，跑 5 个场景并打印表格
python scripts/verify_delivery.py  # 128 MiB 大层/尾段/重设续传/断开回收/失败保留/内存
```

`verify_delivery.py` 用与 compose 相同的双进程拓扑，实测并断言：

1. compose 源站内存上限解析为恰好 `100663296` 字节（96 MiB，若设置
   `COMPOSE_PLUGIN` 指向 compose 插件二进制，会用真 `docker compose config`
   归一化校验，否则用文本规则核对）；
2. 128 MiB `v1/v2` 的 SHA-256 与强 ETag 等于改动前固定种子的金值；
3. 整层 GET 实收 128 MiB 且 SHA-256 命中；
4. 单字节、尾后缀 `-1`、末尾 64 KiB、非对齐小区间：字节全对、206，
   `body_bytes_used <= 区间长度` 且远小于 1 MiB；
5. `If-Range` 命中→206，失配→整层 200；
6. 128 MiB 下载到一半把源站重设为 64 MiB：旧连接仍按原头收齐 128 MiB 且
   SHA-256 正确，随后旧代际文件被删除、目录只剩当前代际两个文件；
7. 下载中重设后客户端提前断开：旧代际被回收；
8. 把数据目录改成不可写模拟重设失败：reset 返回 500，原 32 MiB 层仍可 206；
9. 经网关真实跑一次 128 MiB 演练（1 MiB 块、0 延迟、33 MiB 留存）：
   `both_complete`，快慢两端 SHA-256 都等于金值，源站主下载计数为 1；
10. 采样源站进程峰值 RSS（约 50 MiB），加上健康检查临时进程的保守估计
    （约 14 MiB）后仍 < 96 MiB。

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
- 为满足 96 MiB 内存上限，源站层正文是磁盘文件而非内存对象：每个当前代际保留
  v1/v2 两个文件，重设大小会短暂并存新旧代际，旧代际在最后一个旧下载结束或
  断开后删除；异常 `SIGKILL` 可能在数据目录留下 `layer-g*.bin`，重启后可清理。

## 自测记录（2026-11-23）

- `python -m pytest -q`：8 passed（留存缓冲区溢写/上限、等长双版本、端到端两路收齐、
  额度不足、暂停换版、If-Range 失效回 200）。
- `python scripts/selftest.py`：5 个场景全部通过，上表即其输出。
- 网页渲染另用 jsdom 对 `slow_missing` 场景做了字段级检查（实收字节、校验结果、
  缺段区间、补段 ETag/区间、源站实发、暂存峰值），全部命中。
- 未执行项：本机无 Docker，`docker compose up --build` 未实际构建验证；compose 的
  YAML、健康检查与双服务拆分已核对，并以同拓扑的双进程方式手工跑通。

## 交付核对记录（2026-11-24，96 MiB 源站 / 128 MiB 大层）

### 改了什么

- `docker-compose.yml`：源站增加 `mem_limit: 96m` 与
  `deploy.resources.limits.memory: 96M`（`docker compose config` 归一化均为
  `100663296` 字节），加 `DRILL_ORIGIN_DATA_DIR=/tmp/origin-layers` 与
  `MALLOC_ARENA_MAX=2`，健康检查加 `start_period: 10s`。
- `app/origin.py`：源站由“启动/重置时把 v1、v2 两个整层 `randbytes` 放进内存”
  改为**固定种子、64 KiB 分块生成 + 哈希 + 落盘**，请求时按区间读文件并计量；
  新增代际（generation）租约/回收、重设大小的原子换入与失败保留旧层、
  每条请求记录的 `body_bytes_used`/`work` 用量。HTTP 路由、状态码、响应头
  （强 ETag、`Content-Range`、`X-Content-SHA256`、`X-If-Range-Matched`）
  与解析规则保持不变；旧的内存版 `build_version()` 原样保留。
- `app/main.py`：单进程模式下把源站 reset 放到线程（`asyncio.to_thread`），
  避免生成大层时卡住事件循环；远程 reset 超时放宽到 60s。
- `app/drill.py`：回源 POST 超时由 10s 放宽到 30s（慢盘生成 128 MiB 的余量），
  演练/校验/缺段逻辑一行未动。
- `app/static/index.html` + `app.js`：层大小输入标注 128 MiB 上限，新增
  “填入 128 MiB 大层预设”按钮，源站卡片新增“最近 206 定位+发送用量”。
- `tests/test_origin_lifecycle.py`（新增 24 个用例）与
  `scripts/verify_delivery.py`（新增交付实测脚本）。

### 实测结果

- 兼容性：分块生成在 Python **3.11.2 与 3.12.16**（Docker 同款 3.12）上对
  0–300 连续尺寸及多个大块尺寸，均与一次性 `Random(seed).randbytes(size)`
  逐字节相同；1024 / 512 KiB / 8 MiB / 128 MiB 的 v1、v2 SHA-256 与强 ETag
  与改动前金值完全一致（如 128 MiB v1
  `734546cb6c9f493ed…e9b4cef`、v2 `85aba783…eb379`；8 MiB v1
  `8ba3b294…727b6d`）。
- `python -m pytest -q`：**32 passed**（原 8 个 + 新增 24 个），在 3.11 与
  3.12.16 各跑一遍均通过。
- `python scripts/selftest.py`：原 5 场景全部通过，表格数值与改动前一致；
  3.11 与 3.12.16 均验证。
- `python scripts/verify_delivery.py`：**11 项全部 PASS**（3.11 与 3.12.16
  双解释器各一次），包含：整层 GET 128 MiB SHA 正确；尾段/单字节/64 KiB
  区间字节全对且 `body_bytes_used` 恰为区间长度（≤65536 ≪ 1 MiB）；
  If-Range 206/200；128 MiB 下到一半重设 64 MiB 后旧连接按原头收齐
  134217728 字节且 SHA 一致、旧代际文件回收仅剩 2 个当前文件；提前断开也
  回收；不可写数据目录导致 reset 500 后旧 32 MiB 层仍可 206；经网关
  128 MiB 演练 `both_complete`、双端 SHA 命中、主下载计数 1（0 延迟预设下
  约 1.4–1.5 s）。
- 内存：源站空闲 RSS ≈ 49 MiB，128 MiB reset + 区间压测采样峰值
  **49.5–49.8 MiB（3.11）/ 51.5 MiB（3.12）**；compose 已把健康检查改成
  纯 `socket` 探测，该短进程实测 VmRSS 约 11–12 MiB（urllib 版约 20 MiB），
  保守同峰合计约 63–65 MiB，低于 96 MiB 上限约 30 MiB 余量。健康检查的
  compose 渲染参数已取出并对真实源站执行通过。
- 前端：jsdom 字段级渲染检查 10 项全部 PASS（含新用量字段与 128 MiB 预设）。
- Compose：用真实 Docker Compose v5.8.0 插件 `docker compose config`
  校验本文件，语法/模式合法、无告警，两处内存限制均归一化为
  `100663296`。

### 未完成 / 受限事项

- 本沙箱 **没有 Docker daemon、无 root、cgroup 只读且无 capabilities**，
  因此未能真正执行 `docker compose up --build`，也无法建一个 96 MiB
  memory.max 的真 cgroup 做 OOM 实测。已用：真实 compose 插件做配置归一化
  校验、Python 3.12.16（与 `python:3.12-slim` 同 minor）双进程同拓扑实测、
  `/proc` RSS 峰值采样加健康检查进程估算，来替代验证；首次在真实 Docker
  主机上仍建议跑一次 `docker compose up --build` 后执行
  `COMPOSE_PLUGIN='docker compose' python scripts/verify_delivery.py`
  （或直接网页点“填入 128 MiB 大层预设”）。
- 层正文现在占容器磁盘（每个当前代际 v1+v2，128 MiB 尺寸约 256 MiB；
  重设瞬间可能有新旧两代，旧代际发完/断开即删），不占内存；请确保容器可写层
  或该数据目录卷有相应磁盘空间。
- 旧的已知限制不变：快慢端为同进程消费者、源站状态进程内、补段仅一片连续
  尾段。
