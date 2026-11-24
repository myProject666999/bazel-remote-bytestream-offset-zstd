# ByteStream 偏移、压缩与大小边界约定

本文固定 `server/grpc_bytestream.go`（`Read` / `Write` /
`QueryWriteStatus`）、资源名解析（`parseReadResource` /
`parseWriteResource`）、`cache/disk/disk.go` 的 `get` 以及
`cache/disk/casblob` 几个层面在**偏移（offset）、读取上限（read_limit）、
压缩协商与大小校验**上的边界行为。所有结论均有
`server/zz_task15778_*_test.go` 中的进程内 bufconn gRPC + 临时磁盘缓存用例
覆盖。

协议依据：

- Google ByteStream `bytestream.proto` 的 `ReadRequest` 注释：
  > A `read_offset` that is negative or greater than the size of the
  > resource will cause an `OUT_OF_RANGE` error.
  > `read_limit` is the maximum number of `data` bytes the server is
  > allowed to return in the sum of all `ReadResponse` messages; zero means
  > no limit; a negative limit causes an error.
- REAPI `remote_execution.proto`（resource names 章节）：
  - compressed-blobs 的 `read_offset` 指**未压缩数据中的偏移**；
  - compressed-blobs 的 `read_limit` 必须为 0，非零返回
    `INVALID_ARGUMENT`；
  - compressed 上传已存在时 `WriteResponse.committed_size` 为 `-1`，
    非压缩上传为完整大小；
  - 解压后 digest（含大小）不匹配必须返回 `INVALID_ARGUMENT`（当前实现的
    实际状态码见下文“已知偏差”）。

## 1. 读偏移边界（以 10 字节 blob 为例）

未压缩资源名 `.../blobs/<hash>/10` 与压缩资源名
`.../compressed-blobs/zstd/<hash>/10` 期望状态码完全相同；压缩资源的“返回
字节内容”指 zstd 解码后的未压缩内容。

| ReadOffset | 期望状态 | 返回内容 | 用例 |
| --- | --- | --- | --- |
| `0` | OK | 全部 10 字节 | `.../offset0-all-ten` |
| `9` | OK | 最后 1 字节 | `.../offset9-last-byte` |
| `10`（== size） | OK，**0 条 `ReadResponse`**，空流正常结束 | 空 | `.../offset10-equals-size-empty-ok` |
| `11`（> size） | `OutOfRange` | — | `.../offset11-greater-out-of-range` |
| `-1` | `InvalidArgument` | — | `.../offset-1-negative-invalid-argument` |

零字节 blob（hash 必须是空 SHA256
`e3b0c442...b855`）：

| ReadOffset | 期望状态 | 说明 |
| --- | --- | --- |
| `0` | OK | 未压缩名：0 条消息；`compressed-blobs/zstd/.../0`：返回固定的 9 字节空 zstd 帧 `emptyZstdBlob`（历史兼容），解码后为空 |
| `1` | `OutOfRange` | 与“offset > size”一致 |
| `-1` | `InvalidArgument` | 同样先于 size==0 特判执行 |

### 状态码变化逐项列出

1. **offset == size（size > 0）：`InvalidArgument` → OK（空流）。**
   旧行为：gRPC 层只拒绝 `offset > size`，相等时放行；磁盘缓存
   `get` 用 `offset >= size` 拒绝并返回 `badReqErr("Invalid offset")`，
   经 `gRPCErrCode` 映射为 `InvalidArgument`。现在：
   - gRPC `Read` 在所有校验通过后，对 `offset == size` 直接记录
     `READ COMPLETED` 并返回 nil（压缩零 blob 仍发送 `emptyZstdBlob`）；
   - `cache/disk` 的边界同步放宽为 `offset > size` 才是 bad request，
     `offset == size` 返回空 reader，保证直接调用缓存层时语义一致。

2. **零大小 blob 的越界偏移：静默成功 → 明确报错。**
   旧行为：`size == 0` 在任何 offset/limit 校验之前提前返回，
   `ReadOffset=5` 也成功。现在所有 offset/limit 校验先于 size==0 特判：
   `offset >= 1` 为 `OutOfRange`，负偏移为 `InvalidArgument`，负 limit
   为 `OutOfRange`（压缩资源任意非零 limit 仍优先为
   `InvalidArgument`）。

3. **正 `ReadLimit` 的含义：由“跨块即 `OutOfRange`”改为协议规定的上限截断。**
   旧实现累计检查“本次读到的字节是否会让累计值超过 limit”，一旦超过
   直接 `OutOfRange` 且**当次字节不发送**。由于读缓冲最小按 blob 大小
   分配，10 字节 blob 配 `ReadLimit=1/3` 时客户端得到 0 字节加
   `OutOfRange`，不符合 ByteStream “maximum number of data bytes the
   server is allowed to return” 的上限语义。现在未压缩资源：
   - `limit == 0`：不限制；
   - `limit > 0`：最多发送 limit 字节，发满后以 OK 结束（读缓冲按剩余
     limit 收缩，不会超读）；若数据先到 EOF，则返回实际全部剩余字节并
     OK；
   - `limit < 0`：`OutOfRange`（保持不变）。
   压缩资源仍遵守 REAPI：`ReadLimit` 必须为 0，任意非零（含负数）为
   `InvalidArgument`。

### 对 Bazel 客户端的影响评估

- **offset < size 的路径完全未改**：校验顺序调整不改变这些请求的结果，
  正常下载与分块读取行为不变。
- **offset == size 是本次唯一放宽的偏移边界，也是断点续传的关键场景。**
  Bazel 远端下载在流失败重试时，会把“已经干净收到的字节数”作为新的
  `read_offset`。如果全部内容恰好已经收完、但流在收到正常结束信号前
  失败，重试偏移就可能等于整个 blob 大小。旧实现返回
  `InvalidArgument`（通常被客户端视为不可重试错误），可能导致整个构建
  失败；新实现返回空流并正常结束，客户端据此确认下载完成，正是更符合
  预期的行为。专门的验证即表中 `offset10-equals-size-empty-ok`：断言
  状态 OK 且 `ReadResponse` 消息数为 0，并在 zstd/uncompressed 两种
  存储模式、两种资源名下分别执行。
- **零 blob offset=1** 不是正常客户端会发出的请求（摘要大小为 0 时没有
  任何可恢复字节），把静默成功改为 `OutOfRange` 不会影响合法下载。
- **ReadLimit 截断**：主流 Bazel 客户端下载 CAS blob 时通常不设置
  `read_limit`（取默认值 0，不限制），断点恢复只使用 offset，因此该变化
  不影响主流 Bazel 下载；只有主动请求正 limit 的客户端能观察到差异，且
  变化方向是符合协议的（拿到恰好 limit 字节与 OK，而不是错误）。负
  limit 与压缩资源非零 limit 的状态码没有变化。

## 2. 压缩读取的偏移单位（已与协议核对，结论：实现一致）

REAPI 明确规定 compressed-blobs 的 `read_offset` 是**未压缩形式中的偏移**。
现有实现正是如此：

- `casblob.GetZstdReadCloser` / `GetUncompressedReadCloser` 用
  `offset / chunkSize` 与 `offset % chunkSize` 在 casblob 分块表中定位，
  分块表记录的是未压缩分块边界；
- 偏移落在分块边界（remainder=0）时直接从该压缩分块继续流式输出；
- 偏移落在分块中间时，先解码该分块、丢弃偏移之前的字节，zstd 读取路径
  会把剩余部分重新编码为一个 zstd 帧，再接续后续分块。

该结论已作为注释写入 `Read`。覆盖用例
`TestBnd15778_ChunkOffsetMatrix` 遍历：1 字节、
`chunkSize-1/chunkSize/chunkSize+1`、恰好两个分块等长度，以及分块边界、
边界前后一字节、最后一个分块内部、`offset == size` 等偏移；每种组合都
同时用 `blobs/`（未压缩输出）与 `compressed-blobs/zstd/`（解码后）比对
`data[offset:]`。

三种物理存储格式均覆盖：

1. **zstd-casblob**：默认存储模式，带头部分块表 + zstd 分块；
2. **legacy-v1**：`disk.WithStorageMode("uncompressed")` 写出的无头原始
   文件，文件名带 `.v1`，偏移即原始字节位置；zstd 请求经
   `GetLegacyZstdReadCloser` 现场压缩后缀。见
   `TestBnd15778_LegacyV1FileOffsets`；
3. **identity-header**：带 casblob 头但 `compression=Identity` 的容器，
   覆盖两个 `Get*ReadCloser` 的 Identity 分支。为此修复了
   `casblob.WriteAndClose` 的 Identity 分支：它原来不回写分块偏移表，
   产出的文件无法通过 `readHeader` 的严格校验（`offset table values
   should increase`）；现在会写入 `[headerSize, headerSize+n]`。

另有 `TestBnd15778_CasblobDirectOffsetReaders` 按要求**直接调用**
`casblob.GetZstdReadCloser` 与 `casblob.GetUncompressedReadCloser` 验证
上述偏移。

交叉形态“未压缩存储 + zstd 读取带偏移”和“压缩存储 + 未压缩读取带偏移”
由矩阵的 legacy-v1/zstd-casblob 行 × 两种资源名覆盖。

## 3. 写入方向的边界

### 3.1 不支持断点续传（有意设计取舍）

- blob **不存在**时，首个请求 `WriteOffset != 0` 直接返回哨兵
  `errWriteOffset`（`"bytestream writes from non-zero offsets are
  unsupported"`）。该 error 未经 gRPC status 包装，客户端观察到
  `codes.Unknown` 加该消息——这是当前固定下来的行为与状态码。
- blob **已存在**时，存在性检查先于 offset 检查，因此即使
  `WriteOffset != 0` 也按 REAPI 的重复上传约定成功结束：
  - 未压缩资源：`CommittedSize = <blob 大小>`；
  - 压缩资源：`CommittedSize = -1`。
  这与 REAPI 对 compressed 重复上传的 `-1` 约定一致，已由
  `.../existing-*-nonzero-offset-*` 用例固定。
- `QueryWriteStatus` 因此只有两种形态：未完成
  `{CommittedSize: 0, Complete: false}` 与已完成
  `{CommittedSize: size, Complete: true}`。未完成时报告的 0 正是服务端
  唯一接受的写入偏移，**不暗示存在可续传的部分游标**。代码注释已写明这
  一点，`TestBnd15778_QueryWriteStatusShape` 与各失败写入用例断言失败后
  查询仍为 `{0,false}`。

### 3.2 大小校验与临时文件残留

- **未压缩写入超长**：接收循环累计 `CommittedSize`，一旦超过声明 size
  立即向接收协程返回 `OutOfRange`（`Client sent more than N data! M`）。
  用可控分片（单次、`6+5`、逐字节等）验证结果与分片无关；失败后轮询
  临时目录，确认没有包含该 hash 的残留文件，且 QWS 仍为 `{0,false}`。
- **压缩写入长于声明（解压后）**：gRPC 层没有累计解压大小检查；
  `casblob.WriteAndClose` 按声明 size 规划分块，写满声明字节后用一次
  “多读”探测尾部多余数据。超出的部分**不会作为 chunk 写入磁盘**，只在
  内存探测中被发现，错误包装 `casblob.ErrBadInput`，经 `disk.Put` 的
  `internalErr` 变成 HTTP 500 类 `cache.Error`，最终 gRPC 状态码为
  **`Internal`**（`gRPCErrCode` 对非 400/404/507 的 cache error 回退到
  默认码）。临时文件由 `disk.Put` 的 defer 删除，测试断言无残留。
- **压缩写入短于声明**：在分块 `io.ReadFull` 处提前 EOF，同样包装
  `ErrBadInput` → `Internal`；只在流结束/解码器 EOF 时被发现，临时文件
  同样被清理干净。
- 上述“长/短”两种 Internal 是**当前实现的实际状态码，并用测试固定**
  （`TestBnd15778_WriteCompressedSizeMismatch`）。

#### 已知协议偏差（本次未改状态码）

REAPI 要求解压后大小/digest 不匹配返回 `INVALID_ARGUMENT`。当前磁盘层把
`casblob.ErrBadInput` 统一包成 `internalErr`（500），所以 gRPC 得到
`Internal`。若将来要对齐协议，应在 `disk.Put`/`writeAndCloseFile` 把
`ErrBadInput`（以及 `sha256verifier.ErrMismatch`）分类为
`badReqErr`，`gRPCErrCode` 即会自动映射为 `InvalidArgument`；这会同时
改变 HTTP 层状态码（500→400），属于独立的兼容性决策，因此本次只摸清并
固定现状，不擅自更改。

## 4. 压缩协商与资源名解析

- `parseReadResource` / `parseWriteResource` 只接受压缩器字面量
  **`zstd`**；`zstandard`、`Zstd`、`ZSTD`、`gzip`、`identity` 等均为
  `InvalidArgument`。
- `grpc.go` 的 `GetCapabilities` 静态声明
  `SupportedCompressors = [ZSTD]` 与
  `SupportedBatchUpdateCompressors = [ZSTD]`。
- `TestBnd15778_CompressorSetsMatchCapabilities` 遍历所依赖 REAPI
  genproto 中 `Compressor` 枚举的**全部**取值，证明：能力声明集合 ==
  Read 解析接受集合 == Write 解析（经 `QueryWriteStatus`）接受集合。
  将来新增压缩器时若只改能力声明或只改两个解析字面量中的一处，该测试
  立即失败。
- **元数据段**：写入资源名允许 size 之后跟任意额外路径段
  （`.../blobs/<hash>/<size>/optional/metadata/...`），读取资源名不允许
  额外段（`blobs` 要求恰好 2 个 rem 段，`compressed-blobs` 要求恰好 3
  个），多一段即 `InvalidArgument`。
- **实例名歧义**：解析器从左向右寻找第一个 `blobs` /
  `compressed-blobs` / `uploads` 独立路径段。REAPI 规定实例名路径段不
  得等于这些保留字，因此 `blobs/blobs/...`、`foo/blobs/blobs/...` 等均
  解析失败为 `InvalidArgument`；不含保留字段的多段实例名
  （`org/instance/name/blobs/<h>/<s>`）正常工作。见
  `TestBnd15778_ResourceNameMetadataAndAmbiguity`。

## 5. 存储模式切换等价性

`TestBnd15778_StorageModeSwitchEquivalence` 用相同的固定种子随机数据分别
填充默认 zstd 存储与 uncompressed（legacy `.v1`）存储的两个临时缓存，对
同一组 `compressed-blobs` / `blobs` 请求（含分块边界偏移）断言状态码与
（解码后的）字节内容完全一致——差异仅在物理存储格式，转换由 casblob
完成。

## 6. 日志格式修正

三处 `Printf` 原来格式串有 `%s %d` 两个占位符却只传入数值参数，输出含
`%!s(MISSING)` / `%!d(MISSING)`：

- `GRPC BYTESTREAM READ OFFSET INVALID: ...`
- `GRPC BYTESTREAM READ LIMIT INVALID: ...`（压缩资源非零 limit）
- `GRPC BYTESTREAM READ LIMIT OUT OF RANGE: ...`（负 limit）

现在补齐为 `req.ResourceName, req.ReadOffset/ReadLimit` 两个参数。日志
前缀（冒号之前的 `GRPC BYTESTREAM ...` 字样）逐字未动，现有日志处理脚本
的前缀匹配不受影响。`TestBnd15778_ValidationLogFormatting` 捕获日志，
断言不存在 `%!`/`MISSING` 且包含完整的“前缀 + 资源名 + 数值”行。

## 7. 测试数据与运行方式

- 固定种子 `math/rand` 生成不同长度随机数据：0、1、10 字节、
  `chunkSize-1/chunkSize/chunkSize+1`、恰好 2 个分块等；
- 写入用例使用可控分片，验证分片边界不影响结果；
- 压缩数据直接用依赖中的 `github.com/klauspost/compress/zstd` 在测试内
  编码，不依赖外部工具；
- 全部验证均为进程内 bufconn gRPC + `t.TempDir()` 磁盘缓存，无外部网络。

```sh
go test ./server/ -run 'TestBnd15778_' -count=1
go test -race ./server/ -run 'TestBnd15778_Write|TestBnd15778_QueryWriteStatus'
go test ./... -count=1
```
