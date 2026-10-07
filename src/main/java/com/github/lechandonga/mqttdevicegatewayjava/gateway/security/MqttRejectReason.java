package com.github.lechandonga.mqttdevicegatewayjava.gateway.security;

/**
 * 可区分的拒绝原因，语义对齐 MQTT CONNACK / SUBACK 原因码的常见约定。
 */
public enum MqttRejectReason {

    /** 主题为空。 */
    EMPTY_TOPIC,
    /** 主题包含非法字符（控制字符、首尾斜杠、空层级等）。 */
    INVALID_TOPIC,
    /** 发布主题里出现了通配符。 */
    WILDCARD_IN_PUBLISH,
    /** 多层通配符 {@code #} 没有独占最后一层。 */
    BAD_MULTI_WILDCARD_POSITION,
    /** 单层通配符 {@code +} 没有独占一整层。 */
    BAD_SINGLE_WILDCARD_POSITION,
    /** 主题不属于设备自己的命名空间。 */
    NAMESPACE_DENIED
}
