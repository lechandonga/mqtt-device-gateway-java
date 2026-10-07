package com.github.lechandonga.mqttdevicegatewayjava.gateway;

/**
 * MQTT 主题与主题过滤器的校验和匹配工具。
 * 语义对齐 MQTT：{@code +} 匹配单层，{@code #} 匹配自身及之后所有层级且必须是最后层级。
 */
public final class Topics {

    public static final int MAX_LEVELS = 32;
    public static final int MAX_LENGTH = 256;

    private Topics() {
    }

    /** 校验发布主题（不允许通配符）。 */
    public static void validatePublishTopic(String topic) {
        validateCommon(topic);
        if (topic.contains("+") || topic.contains("#")) {
            throw new GatewayException(RejectionReason.WILDCARD_IN_PUBLISH,
                    "publish topic must not contain wildcards: " + topic);
        }
    }

    /** 校验订阅过滤器（允许位置合法的通配符）。 */
    public static void validateTopicFilter(String filter) {
        validateCommon(filter);
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String level = levels[i];
            if (level.contains("#")) {
                if (!level.equals("#") || i != levels.length - 1) {
                    throw new GatewayException(RejectionReason.WILDCARD_MISPLACED,
                            "'#' must occupy the last level alone: " + filter);
                }
            }
            if (level.contains("+") && !level.equals("+")) {
                throw new GatewayException(RejectionReason.WILDCARD_MISPLACED,
                        "'+' must occupy a whole level: " + filter);
            }
        }
    }

    private static void validateCommon(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new GatewayException(RejectionReason.EMPTY_TOPIC, "topic must not be empty");
        }
        if (topic.length() > MAX_LENGTH) {
            throw new GatewayException(RejectionReason.INVALID_TOPIC_FORMAT, "topic too long: " + topic.length());
        }
        String[] levels = topic.split("/", -1);
        if (levels.length > MAX_LEVELS) {
            throw new GatewayException(RejectionReason.INVALID_TOPIC_FORMAT, "too many levels: " + topic);
        }
        for (String level : levels) {
            if (level.isEmpty()) {
                throw new GatewayException(RejectionReason.INVALID_TOPIC_FORMAT,
                        "topic must not contain empty levels or start/end with '/': " + topic);
            }
        }
    }

    /** 判断订阅过滤器是否匹配某个发布主题。 */
    public static boolean matches(String filter, String topic) {
        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);
        for (int i = 0; i < filterLevels.length; i++) {
            String f = filterLevels[i];
            if (f.equals("#")) {
                return true;
            }
            if (i >= topicLevels.length) {
                return false;
            }
            if (!f.equals("+") && !f.equals(topicLevels[i])) {
                return false;
            }
        }
        return filterLevels.length == topicLevels.length;
    }
}
