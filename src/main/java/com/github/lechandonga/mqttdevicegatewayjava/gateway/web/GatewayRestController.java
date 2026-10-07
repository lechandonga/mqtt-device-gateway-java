package com.github.lechandonga.mqttdevicegatewayjava.gateway.web;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.github.lechandonga.mqttdevicegatewayjava.gateway.MqttGateway;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.api.GatewayClient;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.model.Packet;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.security.MqttException;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.RetainedMessageStore;
import com.github.lechandonga.mqttdevicegatewayjava.gateway.session.SessionRegistry;

/**
 * 可选的本地验证入口：用 HTTP 驱动内存网关，观察会话与投递结果。
 * 不参与 MQTT 报文处理，仅为无需写代码即可手工联调提供便利。
 */
@RestController
@RequestMapping("/gateway")
public class GatewayRestController {

    private final MqttGateway gateway;
    private final SessionRegistry registry;
    private final RetainedMessageStore retainedStore;
    private final Map<String, GatewayClient> clients = new ConcurrentHashMap<>();

    public GatewayRestController(MqttGateway gateway, SessionRegistry registry,
                                 RetainedMessageStore retainedStore) {
        this.gateway = gateway;
        this.registry = registry;
        this.retainedStore = retainedStore;
    }

    public record ConnectRequest(boolean cleanSession, String willTopic, String willMessage) {
    }

    public record PublishRequest(String topic, String message, boolean retain) {
    }

    @PostMapping("/devices/{deviceId}/connect")
    public Map<String, Object> connect(@PathVariable String deviceId,
                                       @RequestBody(required = false) ConnectRequest request) {
        boolean clean = request == null || request.cleanSession();
        GatewayClient client = new GatewayClient(gateway, deviceId);
        if (request != null && request.willTopic() != null && !request.willTopic().isBlank()) {
            byte[] will = request.willMessage() == null ? new byte[0]
                    : request.willMessage().getBytes(StandardCharsets.UTF_8);
            client.connectWithWill(clean, request.willTopic(), will);
        } else {
            client.connect(clean);
        }
        clients.put(deviceId, client);
        return readPackets(deviceId, client);
    }

    @PostMapping("/devices/{deviceId}/subscribe/{*topic}")
    public Map<String, Object> subscribe(@PathVariable String deviceId,
                                         @PathVariable String topic) {
        topic = topic.startsWith("/") ? topic.substring(1) : topic;
        GatewayClient client = requireClient(deviceId);
        client.subscribe(topic);
        return readPackets(deviceId, client);
    }

    @PostMapping("/devices/{deviceId}/publish/qos0")
    public Map<String, Object> publishQos0(@PathVariable String deviceId,
                                           @RequestBody PublishRequest request) {
        GatewayClient client = requireClient(deviceId);
        client.publishQos0(request.topic(),
                request.message() == null ? new byte[0] : request.message().getBytes(StandardCharsets.UTF_8),
                request.retain());
        return readPackets(deviceId, client);
    }

    @PostMapping("/devices/{deviceId}/publish/qos1")
    public Map<String, Object> publishQos1(@PathVariable String deviceId,
                                           @RequestBody PublishRequest request) {
        GatewayClient client = requireClient(deviceId);
        int packetId = client.publishQos1(request.topic(),
                request.message() == null ? new byte[0] : request.message().getBytes(StandardCharsets.UTF_8));
        Map<String, Object> result = readPackets(deviceId, client);
        result.put("packetId", packetId);
        return result;
    }

    @PostMapping("/devices/{deviceId}/ack/{packetId}")
    public Map<String, Object> ack(@PathVariable String deviceId,
                                   @PathVariable int packetId) {
        GatewayClient client = requireClient(deviceId);
        client.ack(new Packet.Deliver("", new byte[0], 1, false, packetId, false));
        return readPackets(deviceId, client);
    }

    @GetMapping("/devices/{deviceId}/inbox")
    public Map<String, Object> inbox(@PathVariable String deviceId) {
        return readPackets(deviceId, requireClient(deviceId));
    }

    @PostMapping("/devices/{deviceId}/disconnect")
    public Map<String, Object> disconnect(@PathVariable String deviceId) {
        GatewayClient client = requireClient(deviceId);
        client.gracefulDisconnect();
        clients.remove(deviceId);
        return Map.of("deviceId", deviceId, "graceful", true, "sessions", registry.size());
    }

    @PostMapping("/devices/{deviceId}/crash")
    public Map<String, Object> crash(@PathVariable String deviceId) {
        GatewayClient client = requireClient(deviceId);
        client.crash();
        clients.remove(deviceId);
        return Map.of("deviceId", deviceId, "abnormal", true, "sessions", registry.size());
    }

    @GetMapping("/sessions")
    public Map<String, Object> sessions() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("sessionCount", registry.size());
        out.put("retainedCount", retainedStore.size());
        return out;
    }

    @ExceptionHandler(MqttException.class)
    public ResponseEntity<Map<String, String>> rejected(MqttException error) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "rejected", "true",
                "reason", error.reason().name(),
                "message", error.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, String>> notConnected(IllegalStateException error) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "message", error.getMessage()));
    }

    private GatewayClient requireClient(String deviceId) {
        GatewayClient client = clients.get(deviceId);
        if (client == null) {
            throw new IllegalStateException("device not connected via REST: " + deviceId);
        }
        return client;
    }

    private Map<String, Object> readPackets(String deviceId, GatewayClient client) {
        List<String> packets = new ArrayList<>();
        for (Packet packet : client.drain()) {
            packets.add(describe(packet));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceId", deviceId);
        result.put("downstream", packets);
        return result;
    }

    private String describe(Packet packet) {
        if (packet instanceof Packet.ConnAck ack) {
            return "CONNACK(sessionPresent=" + ack.sessionPresent() + ")";
        }
        if (packet instanceof Packet.SubAck sub) {
            return "SUBACK(filter=" + sub.filter() + ",grantedQos=" + sub.grantedQos() + ")";
        }
        if (packet instanceof Packet.PubAck ack) {
            return "PUBACK(packetId=" + ack.packetId() + ")";
        }
        if (packet instanceof Packet.Deliver deliver) {
            String body = new String(deliver.payload(), StandardCharsets.UTF_8);
            return "DELIVER(topic=" + deliver.topic()
                    + ",qos=" + deliver.qos()
                    + ",packetId=" + deliver.packetId()
                    + ",dup=" + deliver.dup()
                    + ",retain=" + deliver.retain()
                    + ",payload=" + body + ")";
        }
        return packet.toString();
    }
}
