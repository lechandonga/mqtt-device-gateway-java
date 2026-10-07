package com.github.lechandonga.mqttdevicegatewayjava.gateway.security;

import java.util.Arrays;
import java.util.List;

/**
 * 主题工具：合法性校验、命名空间授权、MQTT 单层/多层通配符匹配。
 *
 * <p>约定：每台设备的命名空间为 {@code devices/<deviceId>/#}，设备只能发布
 * 或订阅该前缀之内的主题；设备 ID 段必须是精确字面量。
 */
public final class MqttTopics {

    public static final String NAMESPACE_PREFIX = "devices/";

    private MqttTopics() {
    }

    /** 拆分主题层级，去掉首尾空白；空串返回空列表。 */
    public static List<String> split(String topic) {
        if (topic == null || topic.isEmpty()) {
            return List.of();
        }
        return Arrays.asList(topic.split("/", -1));
    }

    /**
     * 校验发布用主题（不允许任何通配符）。
     *
     * @throws MqttException 携带具体 {@link MqttRejectReason}
     */
    public static void validatePublishTopic(String topic) {
        List<String> levels = requireWellFormed(topic);
        for (String level : levels) {
            if (level.equals("+") || level.equals("#")) {
                throw new MqttException(MqttRejectReason.WILDCARD_IN_PUBLISH,
                        "publish topic must not contain wildcards: " + topic);
            }
        }
    }

    /**
     * 校验订阅过滤器（允许 {@code +} 与结尾独占一层的 {@code #}）。
     *
     * @throws MqttException 携带具体 {@link MqttRejectReason}
     */
    public static void validateFilter(String filter) {
        List<String> levels = requireWellFormed(filter);
        for (int i = 0; i < levels.size(); i++) {
            String level = levels.get(i);
            boolean last = i == levels.size() - 1;
            if (level.contains("#") && !(last && level.equals("#"))) {
                throw new MqttException(MqttRejectReason.BAD_MULTI_WILDCARD_POSITION,
                        "'#' must occupy the whole last level: " + filter);
            }
            if (level.contains("+") && !level.equals("+")) {
                throw new MqttException(MqttRejectReason.BAD_SINGLE_WILDCARD_POSITION,
                        "'+' must occupy a whole level: " + filter);
            }
        }
    }

    private static List<String> requireWellFormed(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new MqttException(MqttRejectReason.EMPTY_TOPIC, "topic must not be empty");
        }
        List<String> levels = split(topic);
        if (topic.startsWith("/") || topic.endsWith("/")) {
            throw new MqttException(MqttRejectReason.INVALID_TOPIC,
                    "topic must not start or end with '/': " + topic);
        }
        for (String level : levels) {
            if (level.isEmpty() || hasControlChar(level)) {
                throw new MqttException(MqttRejectReason.INVALID_TOPIC,
                        "topic contains empty or illegal level: " + topic);
            }
        }
        return levels;
    }

    private static boolean hasControlChar(String level) {
        for (int i = 0; i < level.length(); i++) {
            if (level.charAt(i) < 0x20) {
                return true;
            }
        }
        return false;
    }

    /**
     * 设备只能访问 {@code devices/<deviceId>/...}。恰好命名空间前缀本身（
     * 比如 {@code devices/dev1}）不属于可发布/订阅的具体主题。
     */
    public static void authorize(String deviceId, String topicOrFilter) {
        String prefix = NAMESPACE_PREFIX + deviceId + "/";
        List<String> levels = split(topicOrFilter);
        String deviceLevel = levels.size() >= 2 ? levels.get(1) : null;
        if (!topicOrFilter.startsWith(prefix)
                || deviceLevel == null
                || !deviceLevel.equals(deviceId)
                || levels.size() < 3) {
            throw new MqttException(MqttRejectReason.NAMESPACE_DENIED,
                    "device '" + deviceId + "' may only use " + prefix + "#");
        }
    }

    /**
     * MQTT 过滤器匹配：{@code +} 匹配单层，{@code #} 匹配剩余任意层。
     */
    public static boolean matches(String filter, String topic) {
        List<String> f = split(filter);
        List<String> t = split(topic);
        int i = 0;
        while (i < f.size()) {
            String fl = f.get(i);
            if (fl.equals("#")) {
                return true;
            }
            if (i >= t.size()) {
                return false;
            }
            if (!fl.equals("+") && !fl.equals(t.get(i))) {
                return false;
            }
            i++;
        }
        return i == t.size();
    }
}
