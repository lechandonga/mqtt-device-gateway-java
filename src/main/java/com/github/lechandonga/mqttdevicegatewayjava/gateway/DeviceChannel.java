package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 设备侧的本地内存收信通道，模拟设备连接上的下行链路。
 * 网关在设备在线时把消息放进通道，设备（或测试）通过 {@link #poll()} 取走。
 */
public class DeviceChannel {

    private final String deviceId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    DeviceChannel(String deviceId) {
        this.deviceId = deviceId;
    }

    public String deviceId() {
        return deviceId;
    }

    void offer(Message message) {
        inbox.offer(message);
    }

    /** 非阻塞取一条下行消息，没有则返回 null。 */
    public Message poll() {
        return inbox.poll();
    }

    /** 最多等待 timeoutMillis 取一条下行消息。 */
    public Message poll(long timeoutMillis) throws InterruptedException {
        return inbox.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public int pendingCount() {
        return inbox.size();
    }
}
