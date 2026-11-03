# simple-cloud-storage 实施计划

> 状态：阶段 1（含补充）已完成；阶段 3 代码、模拟及基础真实文件验证已完成，真实中断续传待验收；阶段 2 暂缓；阶段 4 实施中
> 目标项目：`D:/golang/simple-cloud-storage`  
> 协议参考：`D:/golang/alist`  
> 范围：仅包含独立第三方库自身的设计、实现、测试与发布，不包含任何业务项目对接。  
> 实现状态以各阶段验收记录为准；后续阶段的方案条目不代表功能已经实现。

## 1. 背景与目标

本项目建设独立的 Go 第三方库 `simple-cloud-storage`，不依赖 AList 或具体业务项目，首批支持：

- 阿里云盘开放平台（Aliyundrive Open）。
- 夸克网盘 Web 协议（Quark）。

核心能力：

1. 上传断点续传，支持进程退出后的恢复。
2. 下载断点续传，支持进程退出后的恢复。
3. 大文件自动分片上传。
4. 文件流式下载及 Range 读取。
5. 基于 `io.WriteCloser` 的流式上传，支持可配置自动切片。
6. 客户端级统一 `prefix`，通过同一客户端写入云盘的文件全部位于同一个云盘目录或其子目录下。

## 2. 现状调研结论

### 2.1 AList 可复用内容

阿里云盘优先参考：

- `drivers/aliyundrive_open/upload.go`：create、分片 PUT、上传 URL 刷新、complete。
- `drivers/aliyundrive_open/util.go`：OAuth、令牌刷新、分页和请求错误处理。
- `drivers/aliyundrive_open/types.go`：开放平台 DTO。
- `drivers/aliyundrive_open/limiter.go`：分类限速思路。

旧版 `drivers/aliyundrive` 已废弃，不作为新库基础。

夸克优先参考：

- `drivers/quark_uc/driver.go`：上传和下载编排。
- `drivers/quark_uc/util.go`：PRE、Hash、Auth、Part、Commit、Finish。
- `drivers/quark_uc/upload_reliability.go`：控制面重试和完成结果确认。

### 2.2 不能直接复用的内容

AList 的目标驱动没有完整的跨进程断点续传：

- 每次上传都重新创建会话，没有恢复既有 `upload_id/task_id`。
- 没有持久化已完成分片及 ETag。
- 下载 Range 和断流重试主要在单次请求内完成，没有磁盘 checkpoint。
- 认证和配置持久化依赖 `op.MustSaveDriverStorage`。
- 文件模型、上传流、缓存和限速器与 AList 框架耦合。

因此，新库只参考协议和状态机，续传、checkpoint、流控及公共 API 均重新实现。

## 3. 范围边界

### 3.1 第一版包含

- 统一客户端、逻辑对象键和错误模型。
- 阿里云盘 Open 与夸克两个 Provider。
- 文件上传、流式 Writer、自动分片、上传 checkpoint。
- 流式下载、Range 下载、下载到本地文件及下载 checkpoint。
- `Stat`、`Delete` 基础对象操作。
- 统一 `prefix` 目录隔离。
- 可注入 HTTP Client、TokenStore、CheckpointStore、日志和进度回调。
- 单元测试、模拟 HTTP 服务测试、故障注入测试和真实账号手工集成测试。
- 独立使用文档、配置示例和版本发布说明。

### 3.2 第一版暂不包含

- 通用多云盘目录管理、移动、复制和分享。
- 对象追加写。
- 多实例分布式上传锁。
- 云端到云端的迁移。
- 多线程 Range 下载；先保证正确性，再评估并行下载。
- 把 AList 作为运行时依赖或直接复制其内部公共下载器。
- 任何业务项目的数据库、配置、路由或兼容性改造。

## 4. 总体结构

建议模块路径规范为：

```text
github.com/Niexiawei/simple-cloud-storage
```

建议包结构：

```text
simple-cloud-storage/
├── go.mod
├── storage.go              # ObjectInfo、公共接口和选项
├── client.go               # Client、prefix 和 Provider 编排
├── writer.go               # 流式 Writer
├── resume.go               # 续传模型和存储接口
├── errors.go               # 统一错误分类
├── alipan/                 # 阿里云盘 Open Provider
├── quark/                  # 夸克 Provider
└── internal/
    ├── multipart/          # 切片、任务调度、重试
    ├── checkpoint/         # 文件 checkpoint 实现
    ├── httpx/              # HTTP、退避、Range 校验
    ├── pathx/              # prefix 和逻辑键规范化
    └── iox/                # 有界缓存、临时文件和进度 Reader
```

依赖方向保持为：

```text
调用方 -> simple-cloud-storage 公共 API -> Provider -> 云盘 API
```

公共层不得导入 `alipan` 或 `quark` 的私有 DTO，Provider 不得依赖任何调用方项目。

## 5. 统一 prefix 设计

### 5.1 配置方式

`prefix` 是 Client 级配置，在创建客户端时确定，同一次客户端生命周期内不可变：

```go
cfg := cloudstorage.Config{
    Provider: "alipan",
    Prefix:   "apps/my-app",
}
```

第一版要求 `Prefix` 非空。调用方只传逻辑键：

```text
documents/2026/11/02/a.mp4
images/2026/11/02/a.jpg
```

实际云盘路径统一为：

```text
apps/my-app/documents/2026/11/02/a.mp4
apps/my-app/images/2026/11/02/a.jpg
```

这样同一客户端上传的所有文件都位于 `apps/my-app` 下，同时保留调用方的子目录结构。

### 5.2 路径规范化

prefix 和逻辑键统一使用 `/`，不接受反斜杠作为路径分隔符。

规范化规则：

1. 去除 prefix 首尾空白和首尾 `/`。
2. prefix 可以包含多级目录，例如 `apps/my-app`。
3. 拒绝空段、`.`、`..`、NUL、控制字符和路径穿越。
4. 逻辑键不得为空，不得以 `/` 开头。
5. 逻辑键规范化后必须仍位于当前 prefix 内。
6. 不对云盘名称进行未经确认的自动改名；非法名称返回明确错误。
7. 完整云盘路径只存在于库内部，公共 API 和续传调用始终使用逻辑键。

### 5.3 全操作一致生效

prefix 不能只在上传时拼接。以下操作必须使用同一个路径解析器：

- `Upload`、`UploadFile`、`NewWriter`。
- `Open`、`DownloadToFile`。
- `Stat`、`Delete`。
- 上传和下载 checkpoint 的身份计算。
- Provider 的目录 ID 解析和缓存。

否则会出现上传到一个目录、下载或删除却访问另一个目录的问题。

### 5.4 云盘目录映射

阿里云盘和夸克是文件 ID 模型，不是真正的对象存储扁平 key 模型：

1. Client 初始化或首次使用时解析 prefix 根目录 ID。
2. prefix 不存在时按层级创建目录。
3. 嵌套逻辑键按需创建父目录，并缓存“目录路径 -> 文件 ID”。
4. 并发创建同一目录时必须处理同名目录已存在，避免重复目录。
5. 缓存只保存目录 ID，不永久缓存会过期的下载 URL、上传 URL 或签名。
6. Provider 初始化不得因网络暂时失败留下半初始化的全局状态。

### 5.5 与续传的关系

- checkpoint 指纹必须包含 Provider、账号身份和规范化后的完整云盘路径。
- 修改 prefix 相当于切换命名空间，不自动迁移旧文件，也不复用旧 prefix 的上传会话。
- 切换 prefix 应创建新 Client，历史文件迁移由调用方另行处理。

## 6. 公共 API 草案

以下仅用于明确设计方向，实施时可通过小范围原型调整：

```go
type Storage interface {
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    Upload(ctx context.Context, key string, r io.Reader, opts UploadOptions) (ObjectInfo, error)
    UploadFile(ctx context.Context, key, src string, opts UploadOptions) (ObjectInfo, error)
    Open(ctx context.Context, key string, opts ReadOptions) (io.ReadCloser, error)
    DownloadToFile(ctx context.Context, key, dst string, opts DownloadOptions) error
    NewWriter(ctx context.Context, key string, opts WriterOptions) (io.WriteCloser, error)
    Delete(ctx context.Context, key string) error
}
```

公共模型至少包含：

- `ObjectInfo`：逻辑键、大小、文件 ID、ETag/哈希、修改时间。
- `UploadOptions`：已知大小、哈希、分片大小、并发数、是否允许续传、进度回调。
- `WriterOptions`：已知大小、自动分片阈值、分片大小、并发数、暂存目录、续传标识。
- `ReadOptions`：起始偏移、长度。
- `DownloadOptions`：是否续传、进度回调、临时文件策略。

统一错误分类至少包括：不存在、无权限、认证失效、冲突、限流、临时网络错误、协议不支持、checkpoint 无效和上下文取消。

## 7. 大文件分片与上传续传

### 7.1 上传状态机

标准流程：

```text
验证输入 -> 计算内容身份 -> 创建或恢复会话
-> 获取/刷新分片签名 -> 上传缺失分片
-> 记录分片 checkpoint -> Complete/Finish
-> 确认文件可见 -> 清理 checkpoint
```

分片规则：

- 可配置 `PartSize`、并发数和自动分片阈值。
- 遵守各 Provider 的最小分片、最大分片数和大小约束。
- 内存使用量保持有界，目标上限约为 `PartSize × 并发数` 加固定控制开销。
- 每个分片必须可重读，不能因重试把完整文件缓存进内存。
- 所有网络请求和调度均响应 `context.Context`。

### 7.2 上传 checkpoint

至少保存：

- 格式版本、Provider、账号身份、完整云盘路径。
- 文件大小、SHA-256、分片大小和分片总数。
- 阿里：`drive_id`、`file_id`、`upload_id`。
- 夸克：`task_id`、`fid`、`upload_id`、`obj_key`、`bucket`。
- 已完成分片号、ETag 和最后更新时间。

恢复规则：

1. 验证源文件大小和内容身份，源文件变化后禁止复用旧会话。
2. 优先使用 Provider 的服务端分片查询能力核实进度；若协议无法可靠查询，则按本地 checkpoint 恢复，并允许重传最后一个未确认分片。
3. 上传 URL 或签名过期时重新获取，不把临时 URL 当作永久凭据。
4. 云端会话不存在或失效时，废弃旧 checkpoint 并创建新会话。
5. Complete/Finish 响应丢失时，先按文件 ID 或任务 ID 确认结果，避免盲目创建重复文件。
6. 只有确认云端文件可读后才返回成功并删除 checkpoint。

### 7.3 重试原则

- 仅对明确可重试的限流、临时网络错误和部分 5xx 进行指数退避。
- 尊重 `Retry-After`，等待过程支持 context 取消。
- 创建会话、PRE、Finish 等非天然幂等操作不能简单全局自动重放。
- 认证刷新需要单飞控制，避免并发请求同时刷新令牌。

## 8. 流式 Writer 与自动切片

Go 中使用 `io.Writer/io.WriteCloser` 语义，不定义字面上的 `os.Write` 接口。

### 8.1 已知大小的输入

当调用方提供准确大小，且 Provider 不需要预先计算完整哈希时：

- Writer 按 `PartSize` 聚合数据。
- 达到自动分片阈值后走 multipart。
- 每个分片上传成功后持久化进度。
- `Close` 上传最后一片并完成云端提交。

### 8.2 未知大小或需要完整哈希的输入

以下情况使用本地临时文件暂存：

- 调用方不知道最终大小，Provider 又必须在创建会话时获得大小。
- 夸克 PRE 需要完整 MD5/SHA-1，调用方没有预先提供。
- 输入不可重读且当前协议要求预扫描。

暂存不是把整个文件放入内存。`Write` 仍按流式语义接收数据，`Close` 时根据暂存文件计算元数据并执行可续传上传。

### 8.3 Writer 生命周期

- `Write` 在发生错误后返回实际接收字节数和错误，不伪造成功。
- `Close` 负责最终分片、Complete/Finish 和本地资源释放。
- `Close` 未完成云端提交前不得返回成功。
- 需要中止时提供扩展的 `Abort` 能力；中止和“保留 checkpoint 以便恢复”必须是两种明确语义。
- 临时文件设置合理权限，并支持容量限制和残留清理。

## 9. 流式下载与下载续传

### 9.1 Open 和 Range

`Open` 每次根据逻辑键和 prefix 解析文件 ID，并重新获取临时下载地址。

- 默认从头返回 `io.ReadCloser`，不把完整文件读入内存。
- 指定偏移或长度时发送 HTTP Range。
- 严格校验响应状态、`Content-Range`、实际读取长度和对象身份。
- 调用方关闭 Reader 后应取消后续网络读取。

### 9.2 DownloadToFile

使用以下本地文件：

```text
目标文件.part
CheckpointStore 中的下载记录
目标文件
```

阶段 3 实现采用统一 CheckpointStore 保存下载记录；夸克连接默认写入当前运行目录的 `.simple-cloud-storage/checkpoints/`，不再额外维护目标旁的 `.resume.json` 副本。`.part` 始终位于目标文件旁，恢复前校验记录中的目标绑定。

checkpoint 至少记录：Provider、账号身份、完整云盘路径、文件 ID、大小、ETag/修改时间和已写偏移。

恢复流程：

1. 重新获取下载 URL，不复用过期 URL。
2. 从 `.part` 实际大小和 checkpoint 的共同有效偏移继续。
3. 请求 `Range: bytes=<offset>-`。
4. 续传响应必须为 `206`，且 `Content-Range` 起点、总大小和对象身份必须匹配。
5. 如果偏移大于零而服务端返回 `200`，禁止追加，返回明确错误或按策略从头下载。
6. 完成后校验大小及可用哈希，再原子改名为目标文件。

## 10. Provider 实施要点

### 10.1 阿里云盘 Open

- 使用官方 OAuth 刷新令牌，支持注入 TokenStore。
- 复用 create、getUploadUrl、分片 PUT、complete 的协议顺序。
- 支持按需计算 pre-hash、SHA-1 和 proof code；秒传失败可回退普通上传。
- 处理上传 URL 过期、访问令牌过期和 API 分类限流。
- 不在日志中输出 access token、refresh token 或完整签名 URL。

### 10.2 夸克

- 使用 Cookie 鉴权并支持 Cookie 更新持久化。
- 实现 PRE、Hash、Auth、Part、Commit、Finish 完整状态机。
- PRE 禁止未经控制的自动重试和重定向。
- 下载签名与 Cookie 使用一致快照。
- 完整 MD5/SHA-1 未提供时采用本地暂存。
- 明确标注非官方 Web API 的兼容性风险，真实账号验证通过前不承诺所有账号均可恢复既有上传会话。

## 11. 测试计划

### 11.1 公共层单元测试

- prefix 单级、多级、首尾斜杠和逻辑键拼接。
- 拒绝 `..`、绝对路径、反斜杠、空段和控制字符。
- 所有上传、下载、查询、删除和 checkpoint 使用一致路径。
- 分片边界：空文件、恰好一片、最后一片不足、阈值前后、大小未知。
- Writer 的 Write、Close、Abort、重复关闭和错误传播。
- Range 的 200、206、错误 Content-Range、短读和身份变化。

### 11.2 续传故障注入

- 第 N 片成功后进程退出，重启后跳过已确认分片。
- 分片成功但 checkpoint 未写入，允许安全重传该分片。
- 上传 URL、access token 和夸克签名过期。
- Complete/Finish 成功但响应丢失。
- 源文件修改后禁止复用 checkpoint。
- 下载中断后从 `.part` 偏移继续。
- 云端文件变化后禁止追加旧 `.part`。
- context 取消后无 goroutine 泄漏、无后台继续上传。

### 11.3 Provider 测试

- 使用模拟 HTTP 服务验证请求顺序、payload、签名和错误处理。
- 使用真实账号手工验证目录创建、prefix 隔离、Range 和会话恢复。
- 验证不同 Provider 对公共错误模型和 ObjectInfo 的一致映射。
- 执行 `go test ./...`、`go test -race ./...` 和静态检查。

## 12. 分阶段实施

### 阶段 1：公共骨架

- 规范模块路径。
- 完成公共模型、prefix 路径解析、错误模型和接口。
- 完成 CheckpointStore、TokenStore 及文件实现。
- 用模拟 Provider 验证自动分片和 Writer。

### 阶段 1 补充：公共骨架补齐与验收

> 状态：已完成（2026-11-02）。以下保留补充范围与验收要求，实际交付见本节末尾记录。

#### 补充范围与现状

- 保留现有模块路径、Client/Provider 分层、顺序分片和 Writer 三种模式，围绕已发现的问题做局部修复。
- 补充实施前，现有测试已通过，但临时回归检查发现四项缺陷：清理无超时、prefix 二次规范化、两类文件存储互相覆盖、checkpoint 缺少 Data 仍加载成功；本轮已修复并纳入正式测试。
- 补齐公共续传模型、能力边界和参数校验；真实云盘上传/下载恢复、OAuth、Cookie 和 HTTP Range 协议验证仍由阶段 2、3 实施。
- 本轮继续仅支持 `Concurrency=1`；其他值明确返回 `ErrUnsupported`，并发调度留待后续实现。

#### 1. 修复取消后的无界清理（P1）

- 为 Client 增加清理超时配置 `CleanupTimeout`：零值使用默认 5 秒，负值返回 `ErrInvalidArgument`。
- `Upload` 失败、Writer `Close` 失败及暂存 multipart 失败统一使用有超时的独立清理 context，保留必要的 context 值，确保调用其 cancel 函数。
- 显式 `Abort(ctx)` 同时遵守调用方的取消/截止时间与清理超时上限，不延长调用方指定的期限。
- 使用 `errors.Join` 保留原始错误与清理错误，确保仍能通过 `errors.Is` 判断取消、上传失败和清理超时。
- 远端清理失败后仍尝试释放本地句柄、清除应删除的临时文件；保留再次 Abort 的能力。
- Provider 必须遵守传入 context；不通过启动脱离管理的 goroutine 掩盖不响应取消的实现。

验收：模拟 Abort 等待 context 结束，确认取消后的 Upload/Close 在清理期限内返回；覆盖 Abort 超时、调用方提前取消、再次清理及本地资源释放。

#### 2. 修复 prefix 规范化与路径一致性（P2）

- 明确首尾连续出现的空白和 `/` 都属于 prefix 外围装饰，一次性移除；内部目录段的空白保持原样，不自动改名。
- 规范化必须幂等：对任一合法输入，`Prefix(Prefix(x)) == Prefix(x)`；处理后为空的输入在创建 Client 时拒绝。
- Client 保存唯一规范化结果，路径拼接不得再次改变该结果；所有操作和 checkpoint 身份计算共用这一结果。
- 保持现有路径穿越、绝对逻辑键、反斜杠、空段、无效 UTF-8 检查，并补齐 Unicode 控制字符检查。

验收：`"/ app /"`、`" /apps/demo/ "`、`"/ /"`、内部带空白的目录段及控制字符均有明确结果；逐项验证 Upload、UploadFile、NewWriter、Open、DownloadToFile、Stat、Delete 传给 Provider 的路径与 `Client.Prefix()` 一致。

#### 3. 隔离 Token 与 checkpoint 文件命名空间（P2）

- 在传入的存储根目录下分别使用 `tokens/` 和 `checkpoints/` 子目录，子目录内继续使用 key 的 SHA-256 文件名。
- 相同根目录、相同 key 的两类记录独立 Save、Load、Delete，互不影响；两类 Store 分别重建后仍能读取各自数据。
- 保留临时文件写入、Sync、关闭后替换及失败清理；在当前 Windows 环境验证覆盖更新，在支持的平台验证目录和文件权限。
- 当前格式尚未发布，本次不自动回退读取根目录中的旧文件，避免把旧 token 当作 checkpoint 或反向误读；在注释和示例中说明新布局及旧测试数据需显式重建。

验收：同根目录、同 key 写入不同内容后分别读取、覆盖、删除，并重建 Store 验证持久化；故障时旧记录仍完整，临时文件不被当作有效记录读取。

#### 4. 补齐 checkpoint 加载校验与错误分类（P2）

- Save 与 Load 共用校验规则，拒绝空 ID、缺失 Data、JSON `null`、损坏 JSON、ID 不匹配和不支持的格式版本。
- 通用 Store 校验记录外壳；上传/下载状态模型另行校验自身字段和进度约束，不能仅以 JSON 能解码作为可恢复依据。
- 只有记录不存在才返回 `ErrCheckpointNotFound`；结构或版本错误返回 `ErrInvalidCheckpoint`；取消、权限和磁盘 I/O 错误保留原始错误，不混同为内容损坏。
- 对于已存在但无效的 checkpoint，返回明确错误，由恢复策略决定后续处理，不静默删除或覆盖。

验收：覆盖 `{"id":"x"}`、`Data=null`、空文件、损坏 JSON、错误 ID、未知版本、context 取消和可注入的读写失败，验证 `errors.Is` 和旧记录保留行为。

#### 5. 补齐公共续传契约（P2）

- `UploadOptions` 增加是否续传的选项；`WriterOptions` 增加续传选项与稳定的 `ResumeID`；`DownloadOptions` 增加续传选项与临时文件保留策略。零值维持当前非续传行为。
- 定义参数组合：启用续传必须有 CheckpointStore；需要跨进程识别的 Writer 必须提供稳定 ResumeID，缺少必要配置返回 `ErrInvalidArgument`。
- 尚未实现恢复能力的操作显式返回 `ErrUnsupported`，且应在读取输入、创建远端会话或修改目标/临时文件之前拒绝；禁止接收选项后静默执行普通上传或覆盖下载。
- 增加统一 checkpoint 外壳：版本、操作类型、Provider、账号身份、规范化完整云盘路径、记录 ID 和更新时间；完整云盘路径仅供库内部身份校验使用，调用方继续传逻辑键。
- 定义上传状态：源大小、SHA-256、PartSize、分片总数、已确认分片号/大小/ETag，以及 Provider 的非敏感会话数据。
- 定义下载状态：文件 ID、大小、可用哈希/ETag/修改时间、已确认偏移，以及目标与 `.part` 文件的绑定信息；拒绝负偏移和越界进度。
- checkpoint ID 通过明确的结构化编码计算 SHA-256，包含操作类型、Provider、账号和完整云盘路径，必要时加入 ResumeID；源内容身份与下载目标绑定另行校验，不能只依赖路径相同。
- 用 Mock 验证相同输入身份稳定，不同 Provider、账号、prefix、逻辑键或 ResumeID 不会误用记录；源文件变化及下载目标不匹配必须被识别。
- 明确生命周期：Abort 表示主动放弃并清理；允许恢复的失败/暂停表示保留已确认状态，二者不能共用无条件删除逻辑。跨进程 Writer 恢复还必须具备可重读源或保留的完整暂存文件，仅有 ResumeID 不代表能恢复任意输入流。
- 不保存 token、Cookie、临时签名 URL；Provider 会话恢复、分片核实和完成确认通过后续最小能力接口接入，阶段 1 不预设具体云盘 DTO。
- 明确依赖注入归属：CheckpointStore 由公共编排层消费，HTTPClient、TokenStore 由 Provider 构造配置消费；补齐接线约定，调整或移除 Client 中不会生效的占位配置，避免误导调用方。

验收：模型往返序列化、身份隔离、版本校验和非法参数测试通过；不支持续传的 Mock 在收到续传请求时无远端或本地写入副作用。真实跨进程传输恢复不作为本轮完成声明。

#### 6. 固化回归测试并完成验收

- 将四项已复现缺陷纳入仓库正式回归测试，不依赖临时 overlay。
- 补齐 Writer 空文件、阈值前/等于/后、整片与尾片、未知大小、超量写入、短写、分片失败、Complete 失败、重复 Close、错误保留和 Abort 重试测试。
- 验证错误发生时 Write 的实际接收字节数、Close 不伪报成功、临时文件清理结果及进度回调的一致性；测试异常 context 时避免遗留后台任务。
- 为两个文件 Store 增加重新创建实例后的读写验证，并按需对底层文件操作设置最小故障注入点，验证替换失败不会破坏旧记录。
- 更新公共类型注释及最小配置示例，说明清理超时、存储布局、当前并发限制、续传未支持时的明确错误及后续阶段边界。
- 执行 `go test ./...`、`go test -race ./...`、`go vet ./...`；覆盖率用于检查遗漏分支，不以数字替代行为验收。

实施顺序：先修复 1、2，再完成 3、4，随后落实 5，最后完成 6。各步骤新增的回归测试应随对应修复一起提交。

本轮交付清单：

- [x] 所有自动清理路径有超时，原始错误和清理错误均可识别。
- [x] prefix 规范化幂等，所有操作和身份计算使用一致路径。
- [x] 两类文件存储在同根目录、同 key 下互不覆盖。
- [x] checkpoint Save/Load 校验一致，结构错误与运行环境错误正确区分。
- [x] 公共续传模型与参数契约补齐，未支持能力明确拒绝且无写入副作用。
- [x] 回归测试、竞态检测、静态检查通过，注释和示例与实际行为一致。

以上清单全部完成后，方可将阶段 1 标记为“已完成”；阶段 2、3 继续负责真实 Provider 的协议实现及恢复验证。

验收记录（2026-11-02）：

- `CleanupTimeout` 默认 5 秒，自动清理和显式 Abort 均有期限；暂存上传的会话由 Writer 持有，清理失败后可再次 Abort。
- prefix 已统一规范化；Token 与 checkpoint 分别写入 `tokens/`、`checkpoints/`，不自动读取旧布局。
- checkpoint 外壳使用版本 1，新增上传/下载状态校验、身份指纹、源内容匹配及下载对象/目标绑定校验。
- 增加 Resume、ResumeID 和 KeepPartial 契约；真实续传请求在传输 I/O 前明确拒绝。移除 Client.Config 中未生效的 HTTPClient、TokenStore 字段，注入归属在 Provider 接口注释和 README 中说明。
- 正式回归测试覆盖四项缺陷、Writer 分片/失败边界、Store 重建、替换失败与中途取消保留旧记录、下载临时文件策略，以及续传参数和身份隔离。
- `go test ./...`、`go test -race -p 1 ./...`、`go vet ./...` 均通过，`gofmt -l .` 无输出；可执行配置示例随普通测试通过。
- 默认逐包并行的竞态检查曾因 Windows ThreadSanitizer 地址分配失败而中断；改为 `-p 1` 后完整通过，未发现数据竞争。
- README 和包注释明确当前限制。真实 Provider、跨进程传输恢复、HTTP Range 协议验证及通用残留文件扫描仍待后续阶段实施。

### 阶段 2：阿里云盘 Open

> 按用户 2026-11-03 的要求暂缓：阿里云盘 Open 官方 API 已停止个人申请。本轮不实施该驱动，相关方案保留作后续参考。

- 完成 OAuth、目录 ID 解析、prefix 根目录管理。
- 完成上传、续传、Open、DownloadToFile、Stat、Delete。
- 完成模拟服务和真实账号验证。

### 阶段 3：夸克

> 按用户要求先于阶段 2 实施。状态：代码、模拟及基础真实文件验证已完成，真实中断续传和长期会话续期待验收。

- [x] Cookie 参数/环境变量配置、启动校验、刷新和默认持久化。
- [x] 目录 ID 解析、分页、prefix 根目录与子目录创建、并发建目录防重。
- [x] PRE、Hash、Auth、Part、Commit、Finish，服务器分片大小、秒传和完成可读性确认。
- [x] 暂存 Writer、完整源保留、上传 checkpoint、跨连接恢复和显式 Abort。
- [x] 流式 Open、严格 Range 校验、对象身份检查、下载续传与本地哈希校验。
- [x] 模拟协议/故障测试、竞态检测、静态检查和运行时配置示例。
- [x] 真实账号目录、视频上传/下载、Cookie 主动刷新落盘重连及 Range/哈希校验。
- [ ] 真实中断续传、跨进程未完成分片会话恢复及长期会话续期验证。

连接与 Cookie 约定：

- `drivers.Connect` 根据运行时 Config.Provider 或 `CLOUD_STORAGE_PROVIDER` 选择驱动，当前内置 `quark`；也可直接调用 `quark.Connect`，或用 `quark.New` 注入公共 Client。
- 连接参数 `quark.Config.Cookies` 优先；未提供时读取 `QUARK_COOKIES`，兼容 `QUARK_COOKIE`，再尝试本地已持久化 Cookie。凭据在建立连接时读取，不需要修改源码或重新编译。
- 默认 Cookie 文件是当前运行目录下的 `quark-cookies.json`，可通过 CookieFile 或 `QUARK_COOKIE_FILE` 覆盖。相同初始 Cookie 再次启动时优先加载文件中的较新刷新值；不同初始 Cookie 重新校验账号。
- 启动流程与参考驱动一致，使用网盘 `/config` 初始化并刷新会话 Cookie，默认不请求 `/account/info`；显式 AccountURL 可额外启用账号接口校验。续传标识优先使用 `__uid`，缺失时使用持久化初始 Cookie 指纹派生的隔离标识。默认每 100 分钟继续刷新；请求快照、响应 Cookie 合并及落盘保持一致，刷新不会回滚并发请求已更新的值。
- 凭据文件使用临时文件、Sync、关闭后替换，只有验证成功的初始凭据才会落盘；持久化失败向调用方返回，后台失败可查询 LastRefreshError。Close 或连接 context 取消负责终止刷新任务。
- 下载签名和数据请求始终使用同一 Cookie 快照；控制面与 PRE 不跟随重定向，下载只允许可信域名内的受限重定向。长期登录凭据失效需更新 Cookie 并重连。

上传/下载恢复约定：

- 公共层通过最小 ResumableUploader 接口接入恢复能力，其他 Provider 继续明确拒绝不支持的 Resume。夸克必须先得到完整哈希并使用服务端 PartSize；当前保持顺序分片。
- 上传 checkpoint 保存 Provider/账号/完整路径/内容身份及已确认分片和非敏感任务标识。`auth_info`、callback 等私密数据单独写入 TokenStore，默认位于当前运行目录的 `.simple-cloud-storage/tokens/`，checkpoint 不保存 Cookie、签名或临时 URL。
- 恢复时校验大小、SHA-256 与会话绑定；协议未提供可靠分片查询时使用已确认本地记录，未确认片可按原编号重传。签名每次重新获取；只在明确失效且原 FID 不存在时重建会话。
- Commit/Finish 的结果不确定时先确认原 FID 可读，避免盲目新建上传。确认成功后才清理 checkpoint/私密会话；源变化、对象变化和目标变化返回明确错误。
- 可恢复的 Close 失败保留完整暂存源，通过 PendingUpload.ResumeSource 获取；后续使用同键、同 ResumeID 重传完整源即可恢复云端进度。显式 Abort 放弃会话并清除其状态。对作为 UploadFile 输入的旧源副本不擅自删除，通用残留扫描仍属于阶段 4。
- 下载使用 `.part` 实际大小与 checkpoint 的共同有效偏移；必须校验 206、Content-Range、总大小与对象版本，拒绝把 200 追加到旧文件，完成后校验可用哈希并改名。

验收记录（2026-11-02）：

- 正式模拟测试覆盖 Cookie 环境变量/默认路径、刷新落盘与旧环境值回滚防护、乱序响应与持久化失败、刷新任务退出、并发目录创建、PRE 丢包/重定向禁止重放、签名重取与 Retry-After、秒传与空文件、跨连接上传恢复、失效会话重建、Commit/Finish 响应丢失、已提交任务继续 Finish、清理失败保留确认记录、Writer Abort、下载中断和错误 Range、长数据流不受控制面总超时截断、源/对象/目标变化、凭据脱敏与可信下载重定向。
- 已通过 `go test ./...`、`go test -race -p 1 ./...` 和 `go vet ./...`；运行时示例位于 `examples/quark/main.go`，配置说明位于 README。
- 当前执行沙箱禁止回环 TCP 连接，模拟服务通过内存 HTTP 传输验证相同处理逻辑。真实账号只读验证入口为 TestLiveAccountReadOnly，需主动设置 `QUARK_LIVE_TEST=1` 并配置本地 Cookie。
- 当前环境未配置真实账号 Cookie，真实服务验证未执行，阶段 3 不标记为全部验收完成；非官方 Web 协议对账号类型、会话有效期及恢复能力的限制仍需真实账号验证。

真实验收补充（2026-11-03）：

- 使用已持久化 Cookie 和 `D:\input.mp4`（77,850,522 字节）完成目录创建、分片上传、已完成任务跨进程确认、Cookie 刷新落盘重连、首/中/尾 Range、完整下载 SHA-256 比对及删除确认；`TestLiveQuarkInputMP4` 通过。
- 修正 PRE 实际返回 `http://pds.quark.cn` 被域名校验拒绝的问题，补齐夸克 PDS 上传域名并强制 HTTPS。
- 获取下载链接时按参考驱动覆盖为夸克客户端 User-Agent，与数据请求一致，修正真实服务返回 `23018` 导致上传可读性确认失败的问题。
- 补充上传/列表/Range 错误上下文及确认重试后的原始错误保留；不输出 Cookie、私密签名或响应正文。
- 测试支持 `QUARK_TEST_RUN_ID` 复用日志中的任务目录，避免排查时重复上传；真实未完成分片中断恢复和长期定时续期仍未验收。

### 阶段 4：稳定性收尾

> 状态：本地实现及模拟验证已完成；真实进程中断恢复待授权执行，不标记为全部验收完成。

- [x] 暂存容量限制：Config、UploadOptions、WriterOptions 提供 MaxTempFileSize；已知超量在创建暂存前拒绝，未知超量写入返回 ErrTempLimit 并保持实际接收字节数语义。
- [x] 临时文件维护：CleanupTempUploads 提供年龄筛选、DryRun、Keep 和 checkpoint 保护，只处理指定目录的上传暂存普通文件；跳过本进程活动 Writer、符号链接与子目录，checkpoint 损坏时停止清理。
- [x] 文件 CheckpointStore 提供可选 CheckpointLister，枚举时校验文件类型、外壳和文件名身份；清理时进一步验证操作状态并保留 Source。
- [x] 分片数据缓冲区上限为 128 MiB，同时约束配置和 Provider 返回的几何参数；小文件按实际大小分配，避免预分配后再次分配。
- [x] 下载正文、尾部与 Close 的传输错误脱敏；终止读取错误保持稳定，不暴露签名 URL。
- [x] 模拟故障覆盖分片响应成功但 checkpoint 写入失败后的安全重传；刷新测试验证 100 分钟周期、瞬时失败重试及取消后 Close 等待任务退出。
- [x] 真正的子进程中断模拟：分片 checkpoint 落盘后 os.Exit，新进程读取同一状态并复用会话；验证只创建一次 PRE、跳过已确认分片、内容一致及遗留源清理。
- [ ] 真实夸克未完成分片会话的跨进程上传/下载中断恢复及长期服务会话续期验收。

实现与验收说明（2026-11-03）：

- 普通测试使用内存 HTTP 传输；`TestUploadSurvivesProcessExit` 使用本机 HTTP 模拟服务与独立子进程，需允许回环连接，通过 `-tags=integration` 运行。
- `go test ./...`、`go test -race -p 1 ./...`、`go vet -tags=integration ./...` 已通过；跨进程模拟测试也已通过竞态检测。`gofmt -l .` 与 `git diff --check` 无格式错误。Windows 当前用户无法创建符号链接，对应符号链接回归自动跳过；目录、普通文件及保留源保护已实测。
- `TestLiveQuarkInterruptedResume` 已提供真实验收入口：制作 `D:\input.mp4` 的随机化副本以避免秒传，上传/下载均在已确认进度后退出子进程；新进程恢复后验证原 FID、大小、完整 SHA-256 及 checkpoint 清理。
- 真实测试默认关闭，需显式设置 `QUARK_LIVE_RESUME_TEST=1`；使用独立 prefix，成功后按配置删除本次测试文件，失败时保留本次源副本、云端对象和状态用于排查。本轮执行被自动审批拒绝，尚未上传此次测试副本或进行其删除操作。
- 清理要求停用共享目录的其他进程并提供所有相关 checkpoint/Keep；不实现跨进程文件锁、全局磁盘配额或远端过期会话扫描。MaxTempFileSize 为每次上传的暂存限额，零值保持不限额。
- 分片缓冲区限制不等于整个进程 RSS 上限；分片元数据仍随分片数增长。100 分钟刷新周期使用虚拟时钟模拟，不作为真实长期续期完成声明。

### 阶段 5：独立发布

- 补充公共 API 文档、快速开始、完整配置示例和 Provider 限制。
- 明确 checkpoint 格式兼容性、语义化版本和变更记录。
- 验证库可在全新业务模块中仅通过 Go 模块依赖独立使用。

## 13. 验收标准

1. 同一 Client 上传的文件全部位于配置的 prefix 下，嵌套逻辑键保持一致。
2. 上传、下载、查询、删除和续传不会因 prefix 解析不一致而访问错误目录。
3. 大文件上传不会把完整文件读入内存，进程重启后能够按 checkpoint 恢复。
4. 下载中断后能够从已确认偏移恢复，且不会把 200 响应追加到旧文件。
5. `Open` 和 Writer 均为流式接口；必须暂存的协议限制有明确说明。
6. 两种 Provider 的公共 API、错误模型和进度语义保持一致。
7. 日志和 checkpoint 不泄露访问令牌、Cookie、完整签名 URL 等敏感信息。
8. 库不依赖任何业务项目，并可在全新 Go 模块中独立构建、测试和使用。

## 14. 主要风险

- 夸克使用非官方 Web API，接口和鉴权可能变化。
- AList 没有现成的跨进程续传实现，Provider 的恢复能力必须重新验证。
- 云盘目录是文件 ID 模型，并发建目录和重名处理需要真实服务测试。
- 修改 prefix 会改变文件命名空间，必须提供明确配置说明。
- 本地暂存会占用磁盘，需要容量限制、清理策略和故障恢复机制。
