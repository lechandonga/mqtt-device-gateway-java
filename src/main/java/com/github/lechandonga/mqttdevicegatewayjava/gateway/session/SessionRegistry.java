package com.github.lechandonga.mqttdevicegatewayjava.gateway.session;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 所有设备会话（含离线持久会话）的注册表。
 */
@Component
public class SessionRegistry {

    private final ConcurrentHashMap<String, DeviceSession> sessions = new ConcurrentHashMap<>();

    /** 按设备 ID 串行化连接建立，避免同一设备并发 CONNECT 互相覆盖。 */
    private final ConcurrentHashMap<String, Object> connectLocks = new ConcurrentHashMap<>();

    public DeviceSession get(String deviceId) {
        return sessions.get(deviceId);
    }

    public Object lockOf(String deviceId) {
        return connectLocks.computeIfAbsent(deviceId, id -> new Object());
    }

    public void register(String deviceId, DeviceSession session) {
        sessions.put(deviceId, session);
    }

    /** 仅在设备连接锁内调用：取出并移除当前会话。 */
    public DeviceSession take(String deviceId) {
        return sessions.remove(deviceId);
    }

    public List<DeviceSession> all() {
        return new ArrayList<>(sessions.values());
    }

    /** 清理会话：把该设备的会话状态完整丢弃。 */
    public void discard(String deviceId) {
        sessions.remove(deviceId);
    }

    public int size() {
        return sessions.size();
    }
}
