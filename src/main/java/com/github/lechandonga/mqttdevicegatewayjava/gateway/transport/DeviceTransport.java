package com.github.lechandonga.mqttdevicegatewayjava.gateway.transport;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;

/**
 * 设备物理连接的服务端视图。生产实现可以是 MQTT TCP 连接，
 * 本工程用 {@link LocalDeviceChannel} 的内存队列模拟。
 */
public interface DeviceTransport {

    /** 网关向设备下发报文。连接已关闭时静默丢弃。 */
    void sendToDevice(Packet packet);
}
