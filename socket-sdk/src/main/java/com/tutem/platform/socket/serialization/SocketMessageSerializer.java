package com.tutem.platform.socket.serialization;

/**
 * Strategy interface for serializing and deserializing socket message payloads.
 * Default implementation uses Jackson JSON.
 *
 * To use a different format (e.g. MessagePack, Protobuf), provide a bean
 * implementing this interface and mark it @Primary.
 */
public interface SocketMessageSerializer {

    <T> T deserialize(Object raw, Class<T> targetType);

    String serialize(Object payload);
}
