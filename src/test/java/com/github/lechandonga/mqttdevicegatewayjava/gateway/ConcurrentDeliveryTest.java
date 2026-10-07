package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;

/**
 * 业务：多设备并发收发 —— 不串会话、不丢消息、packetId 不冲突；
 * 以及同一设备反复上下线期间消息最终完整可达。
 */
class ConcurrentDeliveryTest extends AbstractGatewayTest {

    @Test
    @DisplayName("业务06-用例01 多线程多设备并发下发：每台订阅者恰好收到自己命名空间的全部消息")
    void concurrentPublishersDoNotCrossSessionsOrLoseMessages() throws Exception {
        int deviceCount = 8;
        int messagesPerDevice = 200;
        ExecutorService pool = Executors.newFixedThreadPool(deviceCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        List<GatewayClient> subscribers = new ArrayList<>();

        for (int d = 0; d < deviceCount; d++) {
            String id = "dev" + d;
            GatewayClient subscriber = client(id);
            subscriber.connect(false);
            subscriber.subscribe("devices/" + id + "/data");
            subscriber.drain();
            subscribers.add(subscriber);
        }

        for (int d = 0; d < deviceCount; d++) {
            int index = d;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < messagesPerDevice; i++) {
                    gateway.serverPublish("devices/dev" + index + "/data",
                            ("m" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8), 1, false);
                }
                return null;
            }));
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }

        for (int d = 0; d < deviceCount; d++) {
            GatewayClient subscriber = subscribers.get(d);
            List<Packet.Deliver> deliveries = subscriber.drainDeliveries();
            Set<String> bodies = new HashSet<>();
            Set<Integer> packetIds = new HashSet<>();
            for (Packet.Deliver deliver : deliveries) {
                assertTrue(deliver.topic().startsWith("devices/dev" + d + "/"),
                        "消息串到了别的设备会话: " + deliver.topic());
                bodies.add(new String(deliver.payload()));
                packetIds.add(deliver.packetId());
            }
            assertEquals(messagesPerDevice, bodies.size(),
                    "dev" + d + " 应收齐 " + messagesPerDevice + " 条不同消息，实际 " + bodies.size());
            assertEquals(messagesPerDevice, packetIds.size(),
                    "dev" + d + " 的 packetId 不能重复");
            log.info("[业务06-用例01] dev{} 收齐 {} 条，无串会话、无丢失、packetId 唯一",
                    d, messagesPerDevice);
        }
        pool.shutdownNow();
    }

    @Test
    @DisplayName("业务06-用例02 同一设备反复掉线重连 + 并发下发：最终收齐全部消息")
    void concurrentReconnectAndPublishKeepsConsistentState() throws Exception {
        int publisherThreads = 5;
        int messagesPerThread = 60;
        int total = publisherThreads * messagesPerThread;

        GatewayClient subscriber = client("target");
        subscriber.connect(false);
        subscriber.subscribe("devices/target/data");
        subscriber.drain();

        ExecutorService publishers = Executors.newFixedThreadPool(publisherThreads);
        ExecutorService reconnector = Executors.newSingleThreadExecutor();
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();

        // 重连必须在单线程内串行进行（一个物理设备同时只有一条连接）
        Future<?> reconnectTask = reconnector.submit(() -> {
            try {
                start.await();
                for (int r = 0; r < 14; r++) {
                    Thread.sleep(2);
                    subscriber.crash();
                    subscriber.connect(false);
                }
                Thread.sleep(2);
                subscriber.crash();
                // 结束时停留在“离线”状态，由主线程统一重连收尾，避免跨线程竞态
            } catch (Throwable error) {
                errors.add(error);
            }
        });

        for (int t = 0; t < publisherThreads; t++) {
            int idx = t;
            futures.add(publishers.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < messagesPerThread; i++) {
                        gateway.serverPublish("devices/target/data",
                                ("t" + idx + "-m" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                1, false);
                    }
                } catch (Throwable error) {
                    errors.add(error);
                }
            }));
        }

        start.countDown();
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        reconnectTask.get(30, TimeUnit.SECONDS);
        assertTrue(errors.isEmpty(), "并发上下线过程中不应出现异常: " + errors);
        publishers.shutdownNow();
        reconnector.shutdownNow();

        // 重连线程结束时连接为离线：重连一次触发全量 DUP 补发
        subscriber.connect(false);
        Set<String> all = new HashSet<>();
        List<Packet.Deliver> deliveries;
        do {
            for (Packet packet : subscriber.drain()) {
                if (packet instanceof Packet.Deliver deliver) {
                    all.add(new String(deliver.payload()));
                    subscriber.ack(deliver);
                }
            }
            deliveries = subscriber.drainDeliveries();
        } while (!deliveries.isEmpty());

        assertEquals(total, all.size(),
                "反复上下线后仍应收齐全部 " + total + " 条消息");
        log.info("[业务06-用例02] 反复上下线后收齐 {} 条不重复消息，无丢失", all.size());
    }
}
