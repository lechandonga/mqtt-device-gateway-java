package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 设备接入网关：用本地内存通道模拟 MQTT broker 的会话与消息投递语义。
 * 作为 Spring 组件随应用一起启动，不依赖外部 broker 或额外进程。
 *
 * <p>支持能力：
 * <ul>
 *   <li>接入/断开、订阅/发布，主题通配符按 MQTT 的 {@code +}/{@code #} 约定匹配；</li>
 *   <li>持久会话（cleanSession=false）重连后恢复订阅并重发未确认的 QoS 1 消息，
 *       清理会话（cleanSession=true）在断开时完整丢弃会话状态；</li>
 *   <li>QoS 0 / QoS 1 投递，QoS 1 未确认消息重连后带 dup 标记重发，队列有上限；</li>
 *   <li>订阅时下发匹配的保留消息，异常掉线触发遗嘱消息，正常断开不触发；</li>
 *   <li>按设备身份的命名空间权限：设备只能发布/订阅 {@code devices/{deviceId}/} 下的主题。</li>
 * </ul>
 * 所有公共方法在内部全局锁下执行，多设备并发收发不会串会话或丢消息。
 */
@Component
public class DeviceGateway {

    private static final Logger log = LoggerFactory.getLogger(DeviceGateway.class);

    /** 单个会话允许堆积的未确认+离线消息上限。 */
    public static final int MAX_PENDING_MESSAGES = DeviceSession.MAX_PENDING;

    private final Map<String, DeviceSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, Message> retainedMessages = new ConcurrentHashMap<>();
    private final AtomicLong messageIds = new AtomicLong();

    /**
     * 设备接入。返回本次连接使用的收信通道。
     *
     * @param deviceId     设备身份
     * @param cleanSession true=清理会话（断开后丢弃全部状态）；false=持久会话（重连后恢复）
     * @param will         遗嘱消息，异常掉线时由网关代为发布；不需要可传 null
     */
    public synchronized DeviceChannel connect(String deviceId, boolean cleanSession, Message will) {
        requireDeviceId(deviceId);
        DeviceSession session;
        if (cleanSession) {
            session = new DeviceSession(deviceId, true);
            sessions.put(deviceId, session);
        } else {
            session = sessions.computeIfAbsent(deviceId, id -> new DeviceSession(id, false));
        }
        if (will != null) {
            Topics.validatePublishTopic(will.topic());
            checkNamespace(deviceId, will.topic());
        }
        session.will = will;
        DeviceChannel channel = new DeviceChannel(deviceId);
        List<Message> redelivered = session.onReconnect(channel);
        log.info("[gateway] device {} connected (cleanSession={}, subscriptions={}, redelivered={})",
                deviceId, cleanSession, session.subscriptions.size(), redelivered.size());
        return channel;
    }

    /**
     * 设备断开。
     *
     * @param graceful true=正常断开（不触发遗嘱）；false=异常掉线（触发遗嘱消息）
     */
    public synchronized void disconnect(String deviceId, boolean graceful) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null || !session.isOnline()) {
            return;
        }
        session.onDisconnect();
        Message will = session.will;
        session.will = null;
        if (session.cleanSession) {
            sessions.remove(deviceId);
        }
        log.info("[gateway] device {} disconnected (graceful={}, sessionKept={})",
                deviceId, graceful, !session.cleanSession);
        if (!graceful && will != null) {
            log.info("[gateway] device {} lost unexpectedly, publishing will to {}", deviceId, will.topic());
            doPublish(will.topic(), will.payloadAsString(), will.qos(), will.retained());
        }
    }

    /** 订阅主题过滤器。订阅成功后立即下发匹配的保留消息。 */
    public synchronized void subscribe(String deviceId, String filter) {
        DeviceSession session = requireOnline(deviceId);
        Topics.validateTopicFilter(filter);
        checkNamespace(deviceId, filter);
        session.subscriptions.add(filter);
        log.info("[gateway] device {} subscribed {}", deviceId, filter);
        for (Map.Entry<String, Message> entry : retainedMessages.entrySet()) {
            if (Topics.matches(filter, entry.getKey())) {
                session.enqueueOrDeliver(entry.getValue());
            }
        }
    }

    /** 取消订阅。 */
    public synchronized void unsubscribe(String deviceId, String filter) {
        DeviceSession session = requireOnline(deviceId);
        session.subscriptions.remove(filter);
        log.info("[gateway] device {} unsubscribed {}", deviceId, filter);
    }

    /**
     * 发布消息。主题必须通过格式校验且位于设备自身命名空间内。
     *
     * @return 网关分配的消息 id
     */
    public synchronized long publish(String deviceId, String topic, String payload, int qos, boolean retained) {
        requireOnline(deviceId);
        Topics.validatePublishTopic(topic);
        checkNamespace(deviceId, topic);
        checkQos(qos);
        return doPublish(topic, payload, qos, retained);
    }

    /** 设备确认收到一条 QoS 1 消息（PUBACK）。未知 id 直接忽略，保证幂等。 */
    public synchronized void pubAck(String deviceId, long messageId) {
        DeviceSession session = sessions.get(deviceId);
        if (session != null) {
            session.ack(messageId);
        }
    }

    /**
     * 平台侧（云端）下行发布，不受设备命名空间限制，用于向任意设备命名空间投递消息。
     * 设备离线时消息进入其持久会话的积压队列，重连后补发。
     *
     * @return 网关分配的消息 id
     */
    public synchronized long publishFromPlatform(String topic, String payload, int qos, boolean retained) {
        Topics.validatePublishTopic(topic);
        checkQos(qos);
        return doPublish(topic, payload, qos, retained);
    }

    /** 查询设备会话快照（测试与运维观测用）。 */
    public synchronized SessionView sessionView(String deviceId) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null) {
            return new SessionView(false, false, 0, 0, 0);
        }
        return new SessionView(true, session.isOnline(), session.subscriptions.size(),
                session.unacked.size(), session.backlog.size());
    }

    public record SessionView(boolean exists, boolean online, int subscriptions, int unacked, int backlog) {
    }

    private long doPublish(String topic, String payload, int qos, boolean retained) {
        long id = messageIds.incrementAndGet();
        Message message = Message.of(id, topic, payload, qos, retained, false);
        if (retained) {
            if (payload.isEmpty()) {
                retainedMessages.remove(topic);
            } else {
                retainedMessages.put(topic, message);
            }
        }
        int delivered = 0;
        for (DeviceSession session : sessions.values()) {
            if (session.subscriptions.stream().anyMatch(f -> Topics.matches(f, topic))) {
                if (!session.enqueueOrDeliver(message)) {
                    log.warn("[gateway] session {} pending queue full, message {} dropped", session.deviceId, id);
                }
                delivered++;
            }
        }
        log.info("[gateway] published {} -> {} session(s)", message, delivered);
        return id;
    }

    private void checkNamespace(String deviceId, String topicOrFilter) {
        String prefix = namespacePrefix(deviceId);
        if (!topicOrFilter.startsWith(prefix)) {
            throw new GatewayException(RejectionReason.TOPIC_OUT_OF_NAMESPACE,
                    "device %s may only use topics under %s, got: %s".formatted(deviceId, prefix, topicOrFilter));
        }
    }

    /** 设备允许使用的主题命名空间前缀：{@code devices/{deviceId}/}。 */
    public static String namespacePrefix(String deviceId) {
        return "devices/" + deviceId + "/";
    }

    private DeviceSession requireOnline(String deviceId) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null || !session.isOnline()) {
            throw new GatewayException(RejectionReason.NOT_CONNECTED, "device not connected: " + deviceId);
        }
        return session;
    }

    private static void requireDeviceId(String deviceId) {
        if (deviceId == null || deviceId.isBlank() || deviceId.contains("/")
                || deviceId.contains("+") || deviceId.contains("#")) {
            throw new GatewayException(RejectionReason.INVALID_TOPIC_FORMAT, "invalid deviceId: " + deviceId);
        }
    }

    private static void checkQos(int qos) {
        if (qos != 0 && qos != 1) {
            throw new GatewayException(RejectionReason.INVALID_TOPIC_FORMAT, "only QoS 0 and 1 are supported: " + qos);
        }
    }
}
