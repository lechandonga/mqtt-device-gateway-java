package com.github.lechandonga.mqttdevicegatewayjava.gateway.session;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttTopics;

/**
 * 保留消息存储：每个精确主题最多一条最新保留消息。
 *
 * <p>发布 retain=1 的消息会覆盖旧值；空载荷表示清除该主题的保留消息。
 * 新订阅建立时，按 MQTT 约定把过滤器匹配到的保留消息各下发一条。
 */
@Component
public class RetainedMessageStore {

    public record Retained(String topic, byte[] payload) {
        public Retained {
            payload = payload == null ? new byte[0] : payload.clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    /** topic 有序存储，便于按前缀缩小扫描范围，匹配仍走 {@link MqttTopics#matches}。 */
    private final NavigableMap<String, Retained> retained = new ConcurrentSkipListMap<>();

    /** 记录每个主题是否“存在保留消息”，区分未发布与空载荷清除。 */
    private final Map<String, Boolean> present = new ConcurrentHashMap<>();

    public void store(String topic, byte[] payload) {
        if (payload == null || payload.length == 0) {
            retained.remove(topic);
            present.remove(topic);
        } else {
            retained.put(topic, new Retained(topic, payload));
            present.put(topic, Boolean.TRUE);
        }
    }

    public Optional<Retained> get(String topic) {
        return Optional.ofNullable(retained.get(topic));
    }

    public List<Retained> matching(String filter) {
        String prefix = filterPrefix(filter);
        Map<String, Retained> candidate = prefix.isEmpty()
                ? retained
                : retained.subMap(prefix, true, prefix + Character.MAX_VALUE, true);
        List<Retained> result = new ArrayList<>();
        for (Retained message : candidate.values()) {
            if (MqttTopics.matches(filter, message.topic())) {
                result.add(message);
            }
        }
        return result;
    }

    public int size() {
        return retained.size();
    }

    private static String filterPrefix(String filter) {
        int cut = -1;
        for (int i = 0; i < filter.length(); i++) {
            char c = filter.charAt(i);
            if (c == '+' || c == '#') {
                cut = i;
                break;
            }
        }
        if (cut < 0) {
            return filter;
        }
        int slash = filter.lastIndexOf('/', cut);
        return slash < 0 ? "" : filter.substring(0, slash + 1);
    }
}
