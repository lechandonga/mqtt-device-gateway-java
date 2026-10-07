package com.github.lechandonga.mqttdevicegatewayjava.gateway.transport;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地内存通道：用两个队列模拟设备连接的双向收发，不依赖任何外部中间件。
 *
 * <ul>
 *   <li>{@link #sendToDevice} 是网关侧下发，设备侧通过 {@link #pollFromGateway()} 读取；</li>
 *   <li>设备侧通过 {@link #sendToGateway} 上行报文；</li>
 *   <li>{@link #crash()} 模拟异常掉线（TCP 断开/进程被杀），会通知网关触发遗嘱；</li>
 *   <li>正常 DISCONNECT 走 {@code sendToGateway(Packet.Disconnect)}，不会触发遗嘱。</li>
 * </ul>
 */
public final class LocalDeviceChannel implements DeviceTransport {

    /** 掉线回调（由网关注入），异常关闭时触发遗嘱流程。 */
    @FunctionalInterface
    public interface CrashListener {
        void onCrash(LocalDeviceChannel channel);
    }

    private final String deviceId;
    private final ConcurrentLinkedQueue<Packet> gatewayToDevice = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Packet> deviceToGateway = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private volatile CrashListener crashListener;

    public LocalDeviceChannel(String deviceId) {
        this.deviceId = deviceId;
    }

    public String deviceId() {
        return deviceId;
    }

    // ---------- 网关侧 ----------

    @Override
    public void sendToDevice(Packet packet) {
        if (open.get()) {
            gatewayToDevice.add(packet);
        }
    }

    public void setCrashListener(CrashListener listener) {
        this.crashListener = listener;
    }

    /** 网关在会话接管时静默替换掉旧连接：旧连接关闭但不触发遗嘱。 */
    public void forceCloseByGateway() {
        open.set(false);
        gatewayToDevice.clear();
    }

    // ---------- 设备侧 ----------

    public void sendToGateway(Packet packet) {
        if (open.get()) {
            deviceToGateway.add(packet);
        }
    }

    public Optional<Packet> pollFromGateway() {
        return Optional.ofNullable(gatewayToDevice.poll());
    }

    public java.util.Queue<Packet> gatewayQueueView() {
        return gatewayToDevice;
    }

    public Optional<Packet> pollToGateway() {
        return Optional.ofNullable(deviceToGateway.poll());
    }

    public boolean isOpen() {
        return open.get();
    }

    /** 模拟设备异常掉线：不是 DISCONNECT，网关必须发布遗嘱。 */
    public void crash() {
        if (open.compareAndSet(true, false)) {
            CrashListener listener = crashListener;
            if (listener != null) {
                listener.onCrash(this);
            }
        }
    }
}
