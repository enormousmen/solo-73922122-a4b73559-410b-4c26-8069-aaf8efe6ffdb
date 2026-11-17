# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随自己的代际文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除（代际隔离）**：每代固定自己的数据路径 `<key>.part-<代>`；
  完整代发布时在 `<key>` 上增加一个硬链接，代际链接保留给该代的读者。
  清除返回的瞬间 `<key>` 与该代 meta 即消失（重启后也不会被当作命中），
  已开始的旧下载仍从自己的代际链接读完清除前的原字节；旧填充完成也不会
  重新登记。旧代回收只删除它自己拥有的文件：自己的代际链接，以及仍属于
  它自己的 inode/版本号的 `<key>` 与 meta。**旧读者退出不会删除新代的
  缓存文件与元数据**，新代在重启后仍命中而不回源。
  首个新下载重新回源，并发请求继续共用这一次新填充（单飞不变）。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。清除/填充
  交错时同样成立。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` / `.final-*` /
  `.meta.tmp-*` 临时文件与孤儿 meta；数据与 meta 的 inode/版本不一致时
  丢弃 meta 而不误报内容。404 不做负缓存。
* **保留名**：包名不得以 `.meta` 结尾或形如 `.part-<hex>` / `.final-<hex>` /
  `.meta.tmp-<hex>`，避免与缓存旁文件冲突（返回 400）。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、**清除/填充交错的代际回收**（慢下载 + 源包换
内容：旧读者读旧字节、新读者只读新字节、旧读者退出不删新代、重启后仍
命中）、交错时断流不缓存半包、清除后旧读者未退出即重启、重启保留/清理、
内存有界、404、路径穿越、保留名与嵌套路径。`tests/test_cache_ownership.py`
用临时目录直接驱动编排逻辑（含无硬链接文件系统的回退路径）。
`tests/run_server.py` 是测试专用启动器（本沙箱禁止进程间信号，用控制文件
代替 SIGTERM 停服），交付应用本身不含测试钩子。
