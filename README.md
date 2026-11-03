# simple-cloud-storage

独立 Go 云盘存储库，已实现公共骨架及夸克 Web 驱动，支持统一 prefix、流式 Writer、分片上传、上传/下载 checkpoint 和运行时 Cookie 配置。当前并发数为 1。

阶段 3 按需求先于阶段 2 实施，阿里云盘 Open 尚未实现。夸克已通过模拟协议与故障测试，以及真实视频上传、Cookie 重连、Range 和下载哈希验证；真实中断续传仍待验收。协议参考 [AList Quark 驱动](https://github.com/AlistGo/alist/tree/main/drivers/quark_uc)，运行时不依赖 AList。

## 夸克：运行时配置与 Cookie 持久化

使用 `drivers.Connect` 在建立连接时选择驱动，`quark.Connect` 则直接连接夸克。Cookie 可由外部配置传入，也可完全通过环境变量提供：

```go
package example

import (
    "context"
    cloudstorage "github.com/Niexiawei/simple-cloud-storage"
    "github.com/Niexiawei/simple-cloud-storage/drivers"
    "github.com/Niexiawei/simple-cloud-storage/quark"
)

func connect(ctx context.Context, cookiesFromConfig string) (drivers.Client, error) {
    return drivers.Connect(ctx, drivers.Config{
        Provider: "quark", // 留空则读取 CLOUD_STORAGE_PROVIDER，默认 quark。
        Storage: cloudstorage.Config{Prefix: "apps/my-app"},
        Quark: quark.Config{
            Cookies: cookiesFromConfig, // 留空则读取环境变量或本地已持久化 Cookie。
        },
    })
}
```

连接后使用 `defer client.Close()`，或取消传入的连接生命周期 context，以停止并等待刷新任务结束。HTTP Client 与私密会话 TokenStore 可在 `quark.Config` 中注入。

默认控制面请求限时 30 秒；文件数据传输不设整个请求的固定时限，由调用方 context 控制，避免大文件读写被 30 秒总超时截断。默认网络传输保留连接、TLS 和响应头超时；显式注入 HTTPClient.Timeout 时也会用于数据传输。

| 配置 | 行为 |
| --- | --- |
| `quark.Config.Cookies` | 显式传入完整 Cookie 字符串，优先于环境变量 |
| `QUARK_COOKIES` | 首选 Cookie 环境变量；兼容别名 `QUARK_COOKIE` |
| `quark.Config.CookieFile` | 覆盖 Cookie 持久化文件路径 |
| `QUARK_COOKIE_FILE` | 未设置 CookieFile 时使用；默认是**当前运行目录的 `quark-cookies.json`** |
| `quark.Config.StateDir` | checkpoint 与私密上传会话目录，默认当前运行目录的 `.simple-cloud-storage/` |
| `quark.Config.RefreshInterval` | 零值默认每 100 分钟刷新；失败后最多 5 分钟再尝试，负值非法 |

默认连接与参考驱动一致，使用网盘 `/config` 初始化并刷新会话 Cookie，成功后保存文件；不依赖 `pan.quark.cn/account/info`。仅显式设置 `quark.Config.AccountURL` 时才额外调用账号接口。后续可只用文件启动。若参数/环境变量的初始 Cookie 未变化，会优先加载文件中较新的刷新值，避免重启时被旧环境变量覆盖；传入不同 Cookie 则按新凭据重新初始化。

续传的账号标识优先取 Cookie 中的 `__uid`；没有此字段时使用初始 Cookie 指纹派生的隔离标识，随 Cookie 文件持久化，不随会话刷新变化。缺少 `__uid` 时，重新提供不同初始 Cookie 会创建独立续传命名空间。

刷新会移除本次请求中的 `__puus` 以请求服务端重新下发，并合并 API 响应中的认证 Cookie；合并与文件替换按顺序执行，持久化失败会返回错误。后台错误可通过 `Provider.LastRefreshError()` 检查。长期登录凭据失效时需要更新 Cookie 并重连，会话刷新不等同于重新登录。不同账号应使用独立 CookieFile。

提供可编译示例 [examples/quark/main.go](examples/quark/main.go)，构建一次即可改变驱动、Cookie 和操作参数：

```powershell
go build -o quark-example.exe ./examples/quark
$env:CLOUD_STORAGE_PROVIDER = 'quark'
$env:QUARK_COOKIES = '<完整 Cookie 字符串>'
.\quark-example.exe -prefix apps/demo -key docs/a.bin -upload .\a.bin
.\quark-example.exe -prefix apps/demo -key docs/a.bin -download .\download.bin
```

示例默认启用续传，重试同一命令即可读取 checkpoint；使用 `-resume=false` 可关闭。凭据通过运行时配置加载，无需修改源码或重新编译。

## 最小配置

以下函数接收已初始化的 Provider；HTTP Client 和 TokenStore 应由具体 Provider 的构造配置接收，公共 Client 不提供不会生效的认证/HTTP 占位字段。

```go
package example

import (
    "time"
    cloudstorage "github.com/Niexiawei/simple-cloud-storage"
)

func newStorage(provider cloudstorage.Provider, stateDir string) (*cloudstorage.Client, error) {
    checkpoints, err := cloudstorage.NewFileCheckpointStore(stateDir)
    if err != nil {
        return nil, err
    }
    return cloudstorage.NewClient(provider, cloudstorage.Config{
        Prefix: "apps/my-app",
        PartSize: 20 << 20,
        MultipartThreshold: 20 << 20,
        Concurrency: 1,
        CheckpointStore: checkpoints,
        CleanupTimeout: 5 * time.Second,
    })
}
```

调用 Storage 方法时始终传逻辑键，例如 `documents/a.mp4`。prefix 创建后不可变；首尾连续空白与 `/` 会被移除，内部目录段不会自动改名。

## 当前行为与限制

- 仅支持 `Concurrency=1`，其他值返回 `ErrUnsupported`。
- `CleanupTimeout` 零值为 5 秒，负值非法。自动清理使用独立期限；显式 `Abort(ctx)` 同时遵守调用方期限。Provider 必须响应 context 取消。
- 未知大小、需要完整哈希等输入使用本地临时文件暂存；`Close` 负责最终提交，失败后可调用 Abort 重试清理。Writer 的方法需串行调用。
- `UploadOptions.Resume`、`WriterOptions.Resume`、`DownloadOptions.Resume` 默认关闭。启用时必须有 CheckpointStore，Writer 还须提供稳定 ResumeID；`quark.Connect`/`drivers.Connect` 默认提供文件 Store。不支持恢复的自定义 Provider 会在传输 I/O 前返回 `ErrUnsupported`。
- 夸克按服务器返回的 PartSize 切片，协议要求完整大小和 MD5/SHA-1，因此 Writer 先暂存再上传。`DisableMultipart` 不适用于夸克。顺序上传内存有界，不把完整文件放入内存。
- 上传恢复先校验完整 SHA-256 和大小，再复用原任务；只跳过本地 checkpoint 已确认的分片，缺失确认的片会按原编号重传。每次分片重新获取签名。PRE 不自动重放，也不跟随重定向。
- 只有确认原 FID 不存在时才允许放弃失效会话；无法证明旧任务未完成时返回错误并保留状态，避免生成重复文件。非官方协议的既有会话恢复需真实账号验证。
- Writer 的完整暂存源在可恢复的 Close 失败后保留，可通过 `writer.(cloudstorage.PendingUpload).ResumeSource()` 获取，再用相同逻辑键和 `UploadOptions{Resume:true, ResumeID:原标识}` 上传该文件。上传输入文件不会被库删除，恢复后由调用方清理旧源副本。显式 Abort 会放弃会话并清理其 checkpoint 与暂存文件。
- 续传下载使用 `dst.part` 和配置的 CheckpointStore，从本地大小与已确认偏移的较小值继续。严格校验 206、Content-Range 和长度；对象版本或目标变化时拒绝追加，完成后校验可用哈希再改名。
- `DownloadOptions.KeepPartial` 控制非续传下载失败是否保留 `.part`；续传模式始终保留已确认进度。没有 checkpoint 的 `.part` 不会被盲目追加。
- 下载 URL 每次重新获取，实际下载使用生成 URL 时的 Cookie 快照；关闭 Reader 会取消读取。尚未提供通用残留暂存文件扫描、多实例上传锁或并行下载。

## 文件存储与格式变更

`NewFileTokenStore(root)` 写入 `root/tokens/`，`NewFileCheckpointStore(root)` 写入 `root/checkpoints/`，两者可以共用根目录与 key。文件名是 key 的 SHA-256，不使用原始账号或逻辑路径作文件名。

checkpoint 外壳版本为 1，绑定操作类型、Provider、账号、云盘路径和可选 ResumeID；源内容、分片进度、云端版本和下载目标另行验证。Store 校验外壳，`DecodeUpload`/`DecodeDownload` 校验操作状态。checkpoint 只保存非敏感任务标识；夸克 `auth_info` 和 callback 单独保存在 TokenStore，Cookie 保存于 CookieFile，不写入 checkpoint 或日志。

这是发布前格式调整：不自动读取根目录旧记录，也不自动迁移或删除它们。旧测试数据需显式重建，旧凭据需显式重新导入 TokenStore。结构损坏返回 `ErrInvalidCheckpoint`；文件不存在、取消和磁盘错误分别保留其错误语义。

## 验证

```text
go test ./...
go test -race ./...
go vet ./...
```

Windows 的 ThreadSanitizer 若出现地址分配失败，可使用 `go test -race -p 1 ./...` 逐包执行。模拟协议测试使用内存 HTTP 传输，无需外网或本地 TCP 权限。

真实账号只读验证需主动开启并在本机配置 Cookie。在 PowerShell 中执行 `$env:QUARK_LIVE_TEST='1'`，再运行 `go test ./quark -run TestLiveAccountReadOnly -v`。该测试只校验账号、刷新和落盘，不上传或删除文件。真实上传、跨进程恢复与下载应在专用 prefix 下另行验收。

完整真实文件测试使用固定只读源 `D:\input.mp4`，覆盖上传、刷新落盘、仅用 CookieFile 重连、首/中/尾 Range、下载 SHA-256 对比以及删除测试对象：

```powershell
Set-Location D:\golang\simple-cloud-storage
$env:QUARK_COOKIES = '<完整 Cookie 字符串>'
$env:QUARK_LIVE_FILE_TEST = '1'
go test -tags=integration ./quark -run '^TestLiveQuarkInputMP4$' -count=1 -v -timeout=2h
```

测试默认使用独立的 `integration/simple-cloud-storage/<时间与随机标识>/media/input.mp4`，可用 `QUARK_TEST_PREFIX` 指定其父目录。测试工作目录设为模块根目录，默认 CookieFile 和状态目录也位于这里。日志每约 10% 报告进度，不输出 Cookie。源文件不修改，下载副本放在自动清理的临时目录。

重新检查失败任务时，可将 `QUARK_TEST_RUN_ID` 设置为日志中远端路径的时间与随机标识目录名，以复用该对象和上传 checkpoint。例如 `$env:QUARK_TEST_RUN_ID='20261103T025949-334663ef3b99b0cc'`；不设置则每次生成新目录。

成功后默认删除测试对象并验证不存在，空测试目录可能保留；设置 `$env:QUARK_TEST_KEEP_REMOTE='1'` 可保留云端文件。失败时保留云端对象和持久化状态用于排查；临时下载副本仍由 Go 测试清理。此测试通过 `Resume:true` 运行，但不主动制造断流，不等同于跨进程故障恢复验收。

2026-11-03 已使用 77,850,522 字节的 `D:\input.mp4` 完成真实验证，下载 SHA-256 与源文件一致。上传支持 PRE 返回的夸克 PDS 和阿里云 OSS 地址，统一使用 HTTPS；获取下载链接与数据读取均使用参考驱动的夸克客户端 User-Agent。

详细范围及后续安排见 [PLAN.md](PLAN.md)。
