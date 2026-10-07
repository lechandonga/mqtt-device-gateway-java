package com.github.lechandonga.mqttdevicegatewayjava.gateway;

/**
 * 网关拒绝设备操作时抛出的异常，携带可区分的原因码。
 */
public class GatewayException extends RuntimeException {

    private final RejectionReason reason;

    public GatewayException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason getReason() {
        return reason;
    }
}
