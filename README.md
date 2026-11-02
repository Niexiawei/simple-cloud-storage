# simple-cloud-storage

独立 Go 云盘存储库，目前完成阶段 1 公共骨架。提供 Client/Provider 接口、统一 prefix、顺序自动分片、流式 Writer、Token 与 checkpoint 文件存储。

阿里云盘 Open、夸克 Provider 和真实上传/下载续传将在阶段 2、3 实现；当前需注入自有 Provider 或使用测试 Mock。

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
- `UploadOptions.Resume`、`WriterOptions.Resume`、`DownloadOptions.Resume` 默认关闭。启用时必须配置 CheckpointStore，Writer 还必须有稳定 ResumeID；缺配置返回 `ErrInvalidArgument`，配置完整仍返回 `ErrUnsupported`，拒绝发生在传输 I/O 前。
- 配置 CheckpointStore 本身不启用续传。任意输入流不能仅靠 ResumeID 恢复，后续实现还需可重读源或保留的完整暂存文件。
- `DownloadOptions.KeepPartial` 默认关闭；开启后下载失败会保留 `.part`，但不会自动追加或恢复。默认失败清理 `.part`，成功后改名为目标文件。
- 当前阶段尚无 HTTP Range 响应校验、远端会话恢复、暂停保留状态和通用残留文件扫描能力。

## 文件存储与格式变更

`NewFileTokenStore(root)` 写入 `root/tokens/`，`NewFileCheckpointStore(root)` 写入 `root/checkpoints/`，两者可以共用根目录与 key。文件名是 key 的 SHA-256，不使用原始账号或逻辑路径作文件名。

checkpoint 外壳版本为 1，绑定操作类型、Provider、账号、云盘路径和可选 ResumeID；源内容、分片进度、云端版本和下载目标还需各自验证。Store 校验外壳，`DecodeUpload`/`DecodeDownload` 校验操作状态。Provider 会话数据只能保存非敏感标识，不得保存 token、Cookie 或签名 URL。

这是发布前格式调整：不自动读取根目录旧记录，也不自动迁移或删除它们。旧测试数据需显式重建，旧凭据需显式重新导入 TokenStore。结构损坏返回 `ErrInvalidCheckpoint`；文件不存在、取消和磁盘错误分别保留其错误语义。

## 验证

```text
go test ./...
go test -race ./...
go vet ./...
```

详细范围及后续安排见 [PLAN.md](PLAN.md)。
