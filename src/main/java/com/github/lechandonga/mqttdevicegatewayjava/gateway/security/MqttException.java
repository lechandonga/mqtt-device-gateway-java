package com.github.lechandonga.mqttdevicegatewayjava.gateway.security;

/**
 * 网关拒绝某次操作时抛出，携带可区分的 {@link MqttRejectReason}。
 */
public class MqttException extends RuntimeException {

    private final MqttRejectReason reason;

    public MqttException(MqttRejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public MqttRejectReason reason() {
        return reason;
    }
}
