package com.github.lechandonga.mqttdevicegatewayjava.tests.gateway;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceChannel;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Business("多设备并发收发")
@ExtendWith(BusinessLogExtension.class)
class ConcurrentPublishTest {

    private final DeviceGateway gateway = new DeviceGateway();

    @Test
    @DisplayName("多设备并发发布订阅，消息不串会话、不丢失")
    void concurrentDevicesDoNotCrossSessionsOrLoseMessages() throws Exception {
        int deviceCount = 8;
        int messagesPerDevice = 50;
        Map<String, DeviceChannel> channels = new ConcurrentHashMap<>();
        List<String> devices = new ArrayList<>();
        for (int i = 0; i < deviceCount; i++) {
            String device = "dev-n" + i;
            devices.add(device);
            DeviceChannel channel = gateway.connect(device, false, null);
            channels.put(device, channel);
            gateway.subscribe(device, DeviceGateway.namespacePrefix(device) + "evt");
        }

        ExecutorService pool = Executors.newFixedThreadPool(deviceCount * 2);
        CountDownLatch done = new CountDownLatch(deviceCount * 2);
        for (String device : devices) {
            String topic = DeviceGateway.namespacePrefix(device) + "evt";
            pool.submit(() -> {
                try {
                    for (int m = 0; m < messagesPerDevice; m++) {
                        gateway.publish(device, topic, device + "-msg-" + m, 1, false);
                    }
                } finally {
                    done.countDown();
                }
            });
            pool.submit(() -> {
                try {
                    for (int m = 0; m < messagesPerDevice; m++) {
                        gateway.publishFromPlatform(topic, "platform-" + m, 0, false);
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS), "并发发布应在限定时间内完成");
        pool.shutdown();

        for (String device : devices) {
            DeviceChannel channel = channels.get(device);
            int own = 0;
            int platform = 0;
            Message msg;
            while ((msg = channel.poll()) != null) {
                String payload = msg.payloadAsString();
                assertTrue(payload.startsWith(device + "-msg-") || payload.startsWith("platform-"),
                        "设备 " + device + " 收到了不属于自己会话的消息: " + payload);
                if (payload.startsWith(device + "-msg-")) {
                    own++;
                } else {
                    platform++;
                }
            }
            assertEquals(messagesPerDevice, own, "设备 " + device + " 自己发布的 QoS1 消息不应丢失");
            assertEquals(messagesPerDevice, platform, "设备 " + device + " 的平台下行消息不应丢失");
        }
    }
}
