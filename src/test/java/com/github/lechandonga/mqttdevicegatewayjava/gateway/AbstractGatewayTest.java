package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.RetainedMessageStore;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.SessionRegistry;

/**
 * 不启动 Spring 容器，直接组装纯内存网关，单测更快且确定性强。
 */
abstract class AbstractGatewayTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected SessionRegistry registry;
    protected RetainedMessageStore retainedStore;
    protected MqttGateway gateway;

    @BeforeEach
    void setUp() {
        registry = new SessionRegistry();
        retainedStore = new RetainedMessageStore();
        MqttGatewayProperties properties = new MqttGatewayProperties();
        properties.setMaxInflight(1024);
        properties.setMaxQueued(10_000);
        gateway = new MqttGateway(registry, retainedStore, properties);
    }

    protected GatewayClient client(String deviceId) {
        return new GatewayClient(gateway, deviceId);
    }

    protected static byte[] bytes(String text) {
        return text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 平台侧观察者：服务端订阅，收集投递，便于测试断言跨设备的消息。 */
    protected static final class ServerInbox
            implements AutoCloseable {
        private final MqttGateway.ServerSubscription subscription;
        private final ConcurrentLinkedQueue<Packet.Deliver> received = new ConcurrentLinkedQueue<>();

        private ServerInbox(MqttGateway gateway, String filter) {
            this.subscription = gateway.serverSubscribe(filter, 1, received::add);
        }

        public List<Packet.Deliver> drain() {
            List<Packet.Deliver> all = new ArrayList<>();
            received.forEach(all::add);
            received.clear();
            return all;
        }

        @Override
        public void close() {
            subscription.close();
        }
    }

    protected ServerInbox watch(String filter) {
        return new ServerInbox(gateway, filter);
    }
}
