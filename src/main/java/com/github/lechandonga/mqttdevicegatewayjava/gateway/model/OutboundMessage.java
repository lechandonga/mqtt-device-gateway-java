package com.github.lechandonga.mqttdevicegatewayjava.gateway.model;

/**
 * 等待投递给某台设备的消息。每个实例在整个生命周期内复用同一个 packetId，
 * 这样重发时设备可以按 packetId 去重。
 */
public final class OutboundMessage {

    private final String topic;
    private final byte[] payload;
    private final int qos;
    private final int packetId;
    private final boolean retained;
    private boolean inFlight;

    public OutboundMessage(String topic, byte[] payload, int qos, int packetId, boolean retained) {
        this.topic = topic;
        this.payload = payload == null ? new byte[0] : payload.clone();
        this.qos = qos;
        this.packetId = packetId;
        this.retained = retained;
    }

    public String topic() {
        return topic;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public int qos() {
        return qos;
    }

    public int packetId() {
        return packetId;
    }

    public boolean retained() {
        return retained;
    }

    public boolean inFlight() {
        return inFlight;
    }

    public void markInFlight() {
        this.inFlight = true;
    }

    public void markQueued() {
        this.inFlight = false;
    }

    public Packet.Deliver toDeliver(boolean dup) {
        return new Packet.Deliver(topic, payload(), qos, retained, packetId, dup);
    }
}
