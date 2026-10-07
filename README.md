# 设备接入网关（本地内存版）

在 Spring Boot 应用内嵌实现的一套 MQTT 语义设备接入网关：用本地内存通道
模拟设备连接与收发，**不依赖外部 MQTT broker 或其他中间件，也不需要额外进程**。
网关随应用一起启动（`@SpringBootApplication` 扫描 `gateway` 包即可）。

> 范围说明：本工程只对齐 MQTT 3.1.1 的常见**会话语义**，不做 MQTT 报文的
> 二进制编解码（没有 CONNECT/PUBLISH 的字节打包解析），通道层传递的是
> `Packet` 的 Java 对象。

## 支持的 MQTT 语义范围

| 能力 | 支持情况 |
| --- | --- |
| CONNECT / CONNACK（含 `sessionPresent`） | 支持 |
| 清理会话 `cleanSession=true` | 支持，断开/重连均丢弃全部会话状态 |
| 持久会话 `cleanSession=false` | 支持，离线保留订阅与未完成的 QoS1 消息 |
| SUBSCRIBE / SUBACK（授予 QoS）、UNSUBSCRIBE | 支持 |
| 单层通配符 `+`、多层通配符 `#` | 按 MQTT 约定匹配（`#` 必须独占最后一层） |
| PUBLISH QoS 0 / QoS 1 | 支持；QoS 2 不在范围内 |
| PUBACK、DUP 重发标记、RETAIN 保留消息、Last Will 遗嘱 | 支持 |
| 同 clientId 重复接入（会话接管/takeover） | 旧连接静默关闭，不触发遗嘱 |
| MQTT 二进制报文编解码、TLS、真实 TCP 端口 | 不在范围内 |

### 主题约定

- 每台设备的命名空间固定为 `devices/<deviceId>/#`，设备只能发布/订阅其内部主题。
- 设备 ID 段必须是自己的精确字面量：`devices/+/cmd` 这类过滤器对设备是越权的。
- 跨设备的消息（例如平台下发指令、平台侧订阅观察）由服务端入口
  `MqttGateway.serverPublish(...)` / `serverSubscribe(...)` 发起，不受设备命名空间限制。

## 投递与重发规则

- **QoS 0**：尽力投递。设备在线就直接发；离线期间不排队、不补发。
- **QoS 1（至少一次）**：
  - 网关为每条出站消息分配**会话内唯一的 packetId**，消息在整个生命周期复用该 ID；
  - 已下发但未收到 PUBACK 的消息保存在会话队列里，**设备重连后以 `DUP=true` 重发**，
    不会静默丢失；设备按 packetId 识别重复投递，业务侧只处理一次；
  - 收到 PUBACK 后消息永久移除；重复 PUBACK 幂等，不会导致消息回潮；
  - 在途窗口 `gateway.mqtt.max-inflight`（默认 32）限制未确认消息数；
    持久队列 `gateway.mqtt.max-queued`（默认 1000）封顶，超出时记录错误日志，
    避免故障设备造成无上限堆积；
  - 设备重发同一入站 QoS1 报文（同 packetId）时，网关只分发一次业务消息，但每次都会回 PUBACK。
- **保留消息**：每个精确主题只保留最新一条 `retain=true` 的消息（空载荷=清除）；
  新订阅建立时，匹配过滤器的保留消息各下发一条并带 `retain=true`。
- **遗嘱（LWT）**：仅在**异常掉线**（`LocalDeviceChannel.crash()`，模拟 TCP 断开/进程消失）
  时发布遗嘱；正常 `DISCONNECT` 不发布；会话被同 clientId 新连接接管也不发布。
- **持久会话恢复**：重连时 CONNACK 返回 `sessionPresent=true`，原订阅继续生效，
  队列中未完成的消息立即重发；以 `cleanSession=true` 重新接入会废弃旧持久会话。

## 权限控制

主题先做合法性校验，再做命名空间授权。所有拒绝都通过
`MqttException(reason, message)` 返回**可区分的原因码**（见 `MqttRejectReason`）：

| 原因码 | 触发条件 |
| --- | --- |
| `EMPTY_TOPIC` | 主题为 null/空白 |
| `INVALID_TOPIC` | 首尾斜杠、空层级、控制字符等 |
| `WILDCARD_IN_PUBLISH` | 发布主题里出现 `+` 或 `#` |
| `BAD_MULTI_WILDCARD_POSITION` | `#` 没有独占最后一层（如 `a/#/b`、`a#`） |
| `BAD_SINGLE_WILDCARD_POSITION` | `+` 没有独占整层（如 `a+b/c`） |
| `NAMESPACE_DENIED` | 主题不在 `devices/<自己的ID>/#` 内 |

拒绝操作只影响当前调用，不改写任何会话状态，其他设备的会话与投递不受牵连。

## 代码结构

- `gateway/model/Packet.java`：Connect/Subscribe/Publish/PubAck/Disconnect 与
  ConnAck/SubAck/Deliver 的语义报文（sealed interface）。
- `gateway/transport/LocalDeviceChannel.java`：内存双向队列模拟的设备连接，
  支持正常收发、`crash()` 异常掉线、网关侧强制关闭（接管）。
- `gateway/session/DeviceSession.java`：订阅、QoS1 队列/在途表、packetId 分配、
  入站去重、遗嘱；所有状态在会话锁内变更。
- `gateway/session/SessionRegistry.java`、`RetainedMessageStore.java`：
  会话注册表（含离线持久会话、按设备 ID 的连接串行锁）与保留消息存储。
- `gateway/MqttGateway.java`：接入/订阅/发布/确认/断开/掉线的编排与消息路由。
- `gateway/api/GatewayClient.java`：设备侧测试/演示客户端。
- `gateway/web/GatewayRestController.java`：可选的 HTTP 手工验证入口（见下）。

## 本地验证

### 运行单元测试

```bash
mvn test
```

测试日志按业务编号输出用例与断言结果（`[业务NN-用例NN] ...`），共 6 组业务：

| 测试类 | 覆盖业务 |
| --- | --- |
| `SessionLifecycleTest`（业务01） | 持久会话恢复、清理会话丢弃、clean 重连废弃旧状态、会话接管不发遗嘱 |
| `DeliveryQosTest`（业务02） | QoS1 未确认重连 DUP 重发、按 packetId 去重、重复 PUBACK、入站重复报文、QoS0 离线不补发、保留消息下发与清除 |
| `TopicMatchingTest`（业务03） | `+` / `#` 通配符匹配边界（参数化） |
| `AuthorizationTest`（业务04） | 越权、空主题、非法层级、发布带通配符、通配符错位，以及拒绝不影响其他会话 |
| `LastWillTest`（业务05） | 异常掉线发遗嘱、正常 DISCONNECT 不发、非法遗嘱在 CONNECT 被拒 |
| `ConcurrentDeliveryTest`（业务06） | 8 设备×200 条并发不串会话不丢消息；同设备反复上下线 300 条最终收齐 |

### 启动应用并用 HTTP 手工验证

```bash
mvn spring-boot:run
# 或 mvn package 后 java -jar target/mqtt-device-gateway-java-1.0-SNAPSHOT.jar
```

`application.properties` 中 `server.port=0` 表示随机端口；指定端口可加
`--server.port=8080`。示例（端口以实际为准）：

```bash
# 设备接入（持久会话 + 遗嘱）
curl -XPOST localhost:8080/gateway/devices/sensor/connect \
  -H 'Content-Type: application/json' \
  -d '{"cleanSession":false,"willTopic":"devices/sensor/status","willMessage":"offline"}'

# 订阅自有主题（路径变量支持多层主题）
curl -XPOST localhost:8080/gateway/devices/sensor/subscribe/devices/sensor/status

# 发布 QoS1（返回体里带 packetId 与收到的 PUBACK）
curl -XPOST localhost:8080/gateway/devices/sensor/publish/qos1 \
  -H 'Content-Type: application/json' \
  -d '{"topic":"devices/sensor/self","message":"hello"}'

# 模拟确认丢失：不 ack 直接异常掉线，再用 cleanSession=false 重连，
# inbox 中会看到同一 packetId、dup=true 的重发
curl -XPOST localhost:8080/gateway/devices/sensor/crash
curl -XPOST localhost:8080/gateway/devices/sensor/connect \
  -H 'Content-Type: application/json' -d '{"cleanSession":false}'
curl localhost:8080/gateway/devices/sensor/inbox
curl -XPOST localhost:8080/gateway/devices/sensor/ack/1

# 越权订阅 -> HTTP 400，reason=NAMESPACE_DENIED
curl -XPOST localhost:8080/gateway/devices/other/subscribe/devices/sensor/status
```
