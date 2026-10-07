package com.github.lechandonga.mqttdevicegatewayjava.gateway;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关可调参数，前缀 {@code gateway.mqtt}。
 */
@ConfigurationProperties(prefix = "gateway.mqtt")
public class MqttGatewayProperties {

    /** 单设备同时在途（未 PUBACK）的 QoS 1 消息上限。 */
    private int maxInflight = 32;

    /** 单设备持久化待投递 QoS 1 消息上限，防止无界堆积。 */
    private int maxQueued = 1000;

    public int getMaxInflight() {
        return maxInflight;
    }

    public void setMaxInflight(int maxInflight) {
        this.maxInflight = maxInflight;
    }

    public int getMaxQueued() {
        return maxQueued;
    }

    public void setMaxQueued(int maxQueued) {
        this.maxQueued = maxQueued;
    }
}
