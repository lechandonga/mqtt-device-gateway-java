package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.OutboundMessage;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttException;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttRejectReason;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttTopics;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.DeviceSession;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.RetainedMessageStore;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.SessionRegistry;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.transport.DeviceTransport;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.transport.LocalDeviceChannel;

/**
 * 设备接入网关：纯内存实现，随 Spring 应用启动，不依赖外部 MQTT broker。
 *
 * <p>支持的语义见 README，核心规则：
 * <ul>
 *   <li>QoS 0 尽力投递；QoS 1 至少一次，未确认消息重连后带 DUP 重发，按 packetId 去重；</li>
 *   <li>清理会话断开即丢弃全部状态；持久会话保留订阅与未完成消息；</li>
 *   <li>新订阅下发匹配主题的最新保留消息；异常掉线发布遗嘱，正常 DISCONNECT 不发布；</li>
 *   <li>每台设备只能访问 {@code devices/<deviceId>/#}，非法/越权操作返回明确原因。</li>
 * </ul>
 */
@Component
public class MqttGateway {

    private static final Logger log = LoggerFactory.getLogger(MqttGateway.class);

    private final SessionRegistry sessions;
    private final RetainedMessageStore retainedStore;
    private final MqttGatewayProperties properties;

    private final List<ServerSubscription> serverSubscriptions =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    public MqttGateway(SessionRegistry sessions,
                       RetainedMessageStore retainedStore,
                       MqttGatewayProperties properties) {
        this.sessions = sessions;
        this.retainedStore = retainedStore;
        this.properties = properties;
    }

    /**
     * 设备接入。返回是否复用了已有持久会话（CONNACK sessionPresent）。
     * 同一设备 ID 的旧连接会被接管关闭（takeover），不触发遗嘱。
     */
    public boolean connect(LocalDeviceChannel channel, Packet.Connect connect) {
        String deviceId = nonEmpty(connect.clientId(), "clientId");
        connect.willTopic().ifPresent(topic -> {
            MqttTopics.validatePublishTopic(topic);
            MqttTopics.authorize(deviceId, topic);
        });

        DeviceSession session;
        boolean sessionPresent;
        synchronized (sessions.lockOf(deviceId)) {
            DeviceSession existing = sessions.get(deviceId);
            if (existing != null && !existing.cleanSession() && !connect.cleanSession()) {
                // 持久会话重连：复用同一会话实例（订阅、未确认消息都在里面）。
                session = existing;
                sessionPresent = true;
                DeviceTransport oldTransport = session.attach(channel,
                        connect.willTopic(), connect.willPayload());
                if (oldTransport instanceof LocalDeviceChannel oldChannel && oldChannel != channel) {
                    // 会话接管：旧连接静默关闭，旧遗嘱一并作废，不触发遗嘱发布。
                    oldChannel.forceCloseByGateway();
                }
            } else {
                // 清理会话，或旧会话本就是清理会话：旧状态整体丢弃。
                if (existing != null) {
                    sessions.take(deviceId);
                    DeviceSession.Detached old = existing.detach();
                    if (old.transport() instanceof LocalDeviceChannel oldChannel
                            && oldChannel != channel) {
                        oldChannel.forceCloseByGateway();
                    }
                }
                session = new DeviceSession(deviceId, connect.cleanSession());
                sessions.register(deviceId, session);
                sessionPresent = false;
                session.attach(channel, connect.willTopic(), connect.willPayload());
            }
        }
        channel.setCrashListener(c -> handleCrash(deviceId, c));
        channel.sendToDevice(new Packet.ConnAck(sessionPresent));
        log.info("device connected: id={}, cleanSession={}, sessionPresent={}",
                deviceId, connect.cleanSession(), sessionPresent);

        if (sessionPresent) {
            repumpAfterReconnect(session);
        }
        return sessionPresent;
    }

    /** 订阅。成功后立即下发匹配的最新保留消息，并补发积压。 */
    public Packet.SubAck subscribe(String deviceId, Packet.Subscribe subscribe) {
        String filter = nonEmpty(subscribe.filter(), "filter");
        MqttTopics.validateFilter(filter);
        MqttTopics.authorize(deviceId, filter);
        int granted = Math.min(1, Math.max(0, subscribe.maxQos()));

        DeviceSession session = requireConnected(deviceId);
        synchronized (session) {
            session.addSubscription(filter, granted);
            session.sendOnLock(new Packet.SubAck(filter, granted));
            // 新订阅：下发过滤器匹配到的最新保留消息
            for (RetainedMessageStore.Retained retained : retainedStore.matching(filter)) {
                routeToSession(session, retained.topic(), retained.payload(),
                        granted, true);
            }
        }
        log.info("subscribe: device={}, filter={}, grantedQos={}", deviceId, filter, granted);
        pumpSession(session);
        return new Packet.SubAck(filter, granted);
    }

    public void unsubscribe(String deviceId, Packet.Unsubscribe unsubscribe) {
        String filter = nonEmpty(unsubscribe.filter(), "filter");
        MqttTopics.validateFilter(filter);
        MqttTopics.authorize(deviceId, filter);
        DeviceSession session = requireConnected(deviceId);
        synchronized (session) {
            session.removeSubscription(filter);
        }
        log.info("unsubscribe: device={}, filter={}", deviceId, filter);
    }

    /**
     * 设备发布消息。
     *
     * @return QoS 1 时返回入站报文的 packetId（网关回 PUBACK）
     */
    public Integer publish(String deviceId, Packet.Publish publish) {
        MqttTopics.validatePublishTopic(publish.topic());
        MqttTopics.authorize(deviceId, publish.topic());
        int qos = Math.min(1, Math.max(0, publish.qos()));

        DeviceSession session = requireConnected(deviceId);

        if (qos == 1) {
            boolean firstTime;
            synchronized (session) {
                firstTime = session.markInboundReceived(publish.packetId());
            }
            if (!firstTime) {
                // 设备重发了同一个 QoS 1 报文：幂等处理，不再分发，只重发 PUBACK。
                log.info("duplicate inbound publish ignored: device={}, packetId={}",
                        deviceId, publish.packetId());
                session.transport().ifPresent(t -> t.sendToDevice(new Packet.PubAck(publish.packetId())));
                return publish.packetId();
            }
        }

        if (publish.retain()) {
            retainedStore.store(publish.topic(), publish.payload());
        }
        route(publish.topic(), publish.payload(), qos);
        if (qos == 1) {
            session.transport().ifPresent(t -> t.sendToDevice(new Packet.PubAck(publish.packetId())));
        }
        log.info("publish: device={}, topic={}, qos={}, retain={}",
                deviceId, publish.topic(), qos, publish.retain());
        return qos == 1 ? publish.packetId() : null;
    }

    /**
     * 服务端（平台后端）发布消息：不受设备命名空间授权限制，
     * 用于模拟平台向设备下发指令、服务端产生保留消息/遗嘱分发等场景。
     */
    public void serverPublish(String topic, byte[] payload, int qos, boolean retain) {
        MqttTopics.validatePublishTopic(topic);
        int effectiveQos = Math.min(1, Math.max(0, qos));
        if (retain) {
            retainedStore.store(topic, payload);
        }
        route(topic, payload, effectiveQos);
        log.info("server publish: topic={}, qos={}, retain={}", topic, effectiveQos, retain);
    }

    /**
     * 服务端订阅（平台后端视角）：不受设备命名空间限制，建立后立即收到
     * 匹配的最新保留消息，之后按 MQTT 投递规则接收后续消息。
     *
     * @return 服务端订阅句柄，取消订阅时调用 {@link ServerSubscription#close()}
     */
    public ServerSubscription serverSubscribe(String filter, int maxQos,
                                              java.util.function.Consumer<Packet.Deliver> sink) {
        MqttTopics.validateFilter(filter);
        int granted = Math.min(1, Math.max(0, maxQos));
        ServerSubscription subscription = new ServerSubscription(filter, granted, sink);
        synchronized (subscription) {
            serverSubscriptions.add(subscription);
            for (RetainedMessageStore.Retained retained : retainedStore.matching(filter)) {
                subscription.deliver(retained.topic(), retained.payload(),
                        granted, true, false, 0);
            }
        }
        log.info("server subscribe: filter={}, grantedQos={}", filter, granted);
        return subscription;
    }

    /** 服务端订阅句柄：服务端直接消费投递回调，QoS 1 由网关注释为“平台必达”，立即确认。 */
    public final class ServerSubscription {
        private final String filter;
        private final int maxQos;
        private final java.util.function.Consumer<Packet.Deliver> sink;
        private volatile boolean closed;

        private ServerSubscription(String filter, int maxQos,
                                   java.util.function.Consumer<Packet.Deliver> sink) {
            this.filter = filter;
            this.maxQos = maxQos;
            this.sink = sink;
        }

        public String filter() {
            return filter;
        }

        void deliver(String topic, byte[] payload, int qos, boolean retained,
                     boolean dup, int packetId) {
            if (!closed) {
                sink.accept(new Packet.Deliver(topic, payload, qos, retained, packetId, dup));
            }
        }

        public void close() {
            closed = true;
            serverSubscriptions.remove(this);
            log.info("server unsubscribe: filter={}", filter);
        }
    }

    /** QoS 1 下发消息的确认。重复 PUBACK 保持幂等。 */
    public void pubAck(String deviceId, Packet.PubAck ack) {
        DeviceSession session = requireConnected(deviceId);
        boolean removed;
        synchronized (session) {
            removed = session.acknowledge(ack.packetId());
        }
        log.info("puback: device={}, packetId={}, completed={}", deviceId, ack.packetId(), removed);
        pumpSession(session);
    }

    /** 正常断开：不触发遗嘱。持久会话保留，清理会话完整删除。 */
    public void disconnect(String deviceId) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null) {
            return;
        }
        boolean clean;
        synchronized (session) {
            DeviceSession.Detached detached = session.detach();
            clean = session.cleanSession();
            if (detached.transport() instanceof LocalDeviceChannel channel) {
                channel.forceCloseByGateway();
            }
        }
        if (clean) {
            sessions.discard(deviceId);
        }
        log.info("graceful disconnect: device={}, cleanSession={}, sessionKept={}",
                deviceId, clean, !clean);
    }

    /** 异常掉线：触发遗嘱；持久会话保留并等待重连重发。 */
    void handleCrash(String deviceId, LocalDeviceChannel crashed) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null) {
            return;
        }
        String willTopic;
        byte[] willPayload;
        boolean clean;
        synchronized (session) {
            if (session.transport().orElse(null) != crashed) {
                // 该连接已被新连接接管，旧连接崩溃不影响当前会话也不发遗嘱。
                return;
            }
            DeviceSession.Detached detached = session.detach();
            willTopic = detached.willTopic();
            willPayload = detached.willPayload();
            clean = session.cleanSession();
        }
        if (clean) {
            sessions.discard(deviceId);
        }
        log.warn("abnormal disconnect: device={}, will={}, cleanSession={}",
                deviceId, willTopic != null, clean);
        if (willTopic != null) {
            route(willTopic, willPayload, 0);
            log.info("last will published: device={}, topic={}", deviceId, willTopic);
        }
    }

    // ---------- 路由 ----------

    private void route(String topic, byte[] payload, int publishedQos) {
        List<DeviceSession> targets = new ArrayList<>();
        List<Integer> grantedQosList = new ArrayList<>();
        for (DeviceSession candidate : allSessions()) {
            Map<String, Integer> subs = candidate.snapshotSubscriptions();
            Integer granted = null;
            for (Map.Entry<String, Integer> entry : subs.entrySet()) {
                if (MqttTopics.matches(entry.getKey(), topic)) {
                    int g = Math.min(entry.getValue(), publishedQos);
                    granted = granted == null ? g : Math.max(granted, g);
                }
            }
            if (granted != null) {
                targets.add(candidate);
                grantedQosList.add(granted);
            }
        }
        for (int i = 0; i < targets.size(); i++) {
            routeToSession(targets.get(i), topic, payload, grantedQosList.get(i), false);
        }
        for (DeviceSession target : targets) {
            pumpSession(target);
        }
        for (ServerSubscription subscription : serverSubscriptions) {
            if (MqttTopics.matches(subscription.filter(), topic)) {
                subscription.deliver(topic, payload,
                        Math.min(subscription.maxQos, publishedQos), false, false, 0);
            }
        }
    }

    private void routeToSession(DeviceSession session, String topic, byte[] payload,
                                int qos, boolean retained) {
        if (qos <= 0) {
            synchronized (session) {
                // QoS 0：在线直接尽力投递；离线不排队
                session.sendOnLock(new Packet.Deliver(topic, payload == null ? new byte[0] : payload,
                        0, retained, 0, false));
            }
            return;
        }
        synchronized (session) {
            if (session.pendingCount() >= properties.getMaxQueued()) {
                log.error("outbound queue full, dropping QoS1 message: device={}, topic={}",
                        session.deviceId(), topic);
                return;
            }
            int packetId = session.allocatePacketId();
            OutboundMessage message = new OutboundMessage(topic, payload, 1, packetId, retained);
            session.enqueue(message);
        }
    }

    /** 在在途窗口内向连接推送可发送的消息；重发的消息带 DUP 标记。 */
    private void pumpSession(DeviceSession session) {
        List<OutboundMessage> batch;
        synchronized (session) {
            if (!session.connected()) {
                return;
            }
            batch = session.drainPending(properties.getMaxInflight());
            for (OutboundMessage message : batch) {
                session.sendOnLock(message.toDeliver(false));
            }
        }
        if (!batch.isEmpty()) {
            log.debug("delivered batch: device={}, size={}", session.deviceId(), batch.size());
        }
    }

    /**
     * 重连后补发：所有未完成消息（含之前在途的）都按 DUP=1 重发。
     * 超过在途窗口的部分留在队列，待 PUBACK 释放窗口后继续补发。
     */
    private void repumpAfterReconnect(DeviceSession session) {
        List<OutboundMessage> batch;
        synchronized (session) {
            if (!session.connected()) {
                return;
            }
            batch = session.drainAllForRedelivery(properties.getMaxInflight());
            for (OutboundMessage message : batch) {
                session.sendOnLock(message.toDeliver(true));
            }
        }
        if (!batch.isEmpty()) {
            log.info("redelivery after reconnect: device={}, size={}",
                    session.deviceId(), batch.size());
        }
    }

    private DeviceSession requireConnected(String deviceId) {
        DeviceSession session = sessions.get(deviceId);
        if (session == null || !session.connected()) {
            throw new IllegalStateException("device not connected: " + deviceId);
        }
        return session;
    }

    private List<DeviceSession> allSessions() {
        return sessions.all();
    }

    private static String nonEmpty(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new MqttException(MqttRejectReason.EMPTY_TOPIC, field + " must not be empty");
        }
        return value;
    }
}
