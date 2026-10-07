# MQTT Device Gateway (Java Bootstrap)

基于 Spring Boot 的设备接入网关示例：用**本地内存通道**模拟设备连接与消息收发，
实现 MQTT 风格的会话管理与消息投递语义。网关作为 Spring 组件（`DeviceGateway`）
随应用一起启动，不依赖外部 MQTT broker 或其他中间件，也不需要额外进程。

## MQTT 语义范围

对齐常见 MQTT 约定的**语义层**，不包含 MQTT 报文的二进制编解码：

- 接入/断开：`connect(deviceId, cleanSession, will)` / `disconnect(deviceId, graceful)`
- 订阅/取消订阅/发布：`subscribe` / `unsubscribe` / `publish`（设备上行）与
  `publishFromPlatform`（平台下行，不受设备命名空间限制）
- 主题通配符：`+` 匹配单层，`#` 匹配自身及之后所有层级且必须是最后一个层级
- QoS 0（至多一次）与 QoS 1（至少一次，PUBACK 确认）；QoS 2 不在范围内
- 保留消息（retained）与遗嘱消息（will）

## 会话、投递与重发规则

- **持久会话**（`cleanSession=false`）：断开后保留订阅、未确认的 QoS 1 消息和离线积压消息；
  重连后自动恢复订阅，先以 `dup=true` 重发未确认消息（复用原消息 id，设备侧可据此去重），
  再按序补发离线积压消息。
- **清理会话**（`cleanSession=true`）：断开时会话状态（订阅、未确认、积压）被完整丢弃；
  以清理会话接入也会覆盖同设备的旧会话。
- **QoS 1 确认**：设备通过 `pubAck(deviceId, messageId)` 确认；重复确认幂等，
  已确认的消息不会重发，未确认的消息不会因掉线静默丢失，多次重连也不会重复堆积。
- **堆积上限**：单会话未确认+离线消息上限为 `DeviceGateway.MAX_PENDING_MESSAGES`（1000），
  超限丢弃最老消息并输出告警日志，保证无上限堆积不会发生。
- **保留消息**：订阅成功后立即下发过滤器命中的最新保留消息；空 payload 的保留发布清除该主题的保留消息。
- **遗嘱消息**：`graceful=false` 的异常掉线由网关代为发布遗嘱；正常断开（`graceful=true`）不触发。

## 权限控制

设备只能发布和订阅自身命名空间 `devices/{deviceId}/` 下的主题。所有非法操作抛出
`GatewayException`，携带可区分的 `RejectionReason`：

| 原因码 | 含义 |
| --- | --- |
| `EMPTY_TOPIC` | 主题为空 |
| `INVALID_TOPIC_FORMAT` | 空层级、`/` 开头结尾、层级或长度超限 |
| `WILDCARD_MISPLACED` | `#` 非最后层级、`+`/`#` 未独占层级 |
| `WILDCARD_IN_PUBLISH` | 发布主题含通配符 |
| `TOPIC_OUT_OF_NAMESPACE` | 越权访问其他设备命名空间 |
| `NOT_CONNECTED` | 设备未连接 |
| `SESSION_QUEUE_OVERFLOW` | 会话队列超限 |

单个设备的越权或非法操作只影响本次调用，不影响其他设备的会话。

## 本地验证

```bash
mvn test
```

测试按业务分组，日志中会输出每个业务的用例执行情况和通过/失败汇总：

- `SessionRecoveryTest` — 会话恢复与清理（持久会话恢复、离线补发、清理会话丢弃）
- `Qos1RedeliveryTest` — QoS1 重发与去重（dup 重发、确认幂等、堆积上限）
- `AclAndTopicValidationTest` — 主题权限与非法主题拒绝（越权、非法主题、通配符匹配）
- `RetainedAndWillTest` — 保留消息与遗嘱消息
- `ConcurrentPublishTest` — 多设备并发收发（不串会话、不丢消息）

## 代码结构

```
src/main/java/com/github/lechandonga/mqttdevicegatewayjava/gateway/
├── DeviceGateway.java    # 网关主体（Spring 组件，随应用启动）
├── DeviceSession.java    # 单设备会话：订阅、unacked、离线积压
├── DeviceChannel.java    # 设备侧内存收信通道
├── Message.java          # 消息模型（id/topic/payload/qos/retained/dup）
├── Topics.java           # 主题校验与通配符匹配
├── GatewayException.java # 拒绝异常
└── RejectionReason.java  # 可区分的拒绝原因码
```
