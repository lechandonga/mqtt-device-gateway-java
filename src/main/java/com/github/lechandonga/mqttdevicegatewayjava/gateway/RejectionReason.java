package com.github.lechandonga.mqttdevicegatewayjava.gateway;

/**
 * 网关拒绝设备操作的可区分原因。
 */
public enum RejectionReason {
    /** 主题为空或仅由空白组成 */
    EMPTY_TOPIC,
    /** 主题格式非法（如以 / 开头或结尾、包含空层级、超长） */
    INVALID_TOPIC_FORMAT,
    /** 通配符位置不合法（# 不是最后层级、+ 或 # 未独占一个层级） */
    WILDCARD_MISPLACED,
    /** 发布主题中不允许出现通配符 */
    WILDCARD_IN_PUBLISH,
    /** 主题不在设备自身的命名空间内 */
    TOPIC_OUT_OF_NAMESPACE,
    /** 设备当前未连接 */
    NOT_CONNECTED,
    /** 会话待确认/待投递队列已满，最老的消息被丢弃 */
    SESSION_QUEUE_OVERFLOW
}
