package com.tutem.platform.socket.dispatcher;

import com.corundumstudio.socketio.BroadcastOperations;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SocketSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessageDispatcherTest {

    private SocketIOServer server;
    private InMemorySessionManager sessionManager;
    private MessageDispatcher dispatcher;

    private final Object payload = Map.of("k", "v");

    @BeforeEach
    void setUp() {
        server = mock(SocketIOServer.class);
        sessionManager = new InMemorySessionManager();
        dispatcher = new MessageDispatcher(server, sessionManager);
    }

    private SocketIOClient saveSessionWithOpenClient(String userId) {
        UUID id = UUID.randomUUID();
        sessionManager.save(SocketSession.builder()
            .sessionId(id.toString()).userId(userId).build());
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(true);
        when(server.getClient(id)).thenReturn(client);
        return client;
    }

    @Test
    @DisplayName("sendToUser fans out to every live session of the user")
    void sendToUser_multipleSessions_sendsToAllOfThem() {
        SocketIOClient phone = saveSessionWithOpenClient("u1");
        SocketIOClient tablet = saveSessionWithOpenClient("u1");
        SocketIOClient someoneElse = saveSessionWithOpenClient("u2");

        dispatcher.sendToUser("u1", "rideAssigned", payload);

        verify(phone).sendEvent("rideAssigned", payload);
        verify(tablet).sendEvent("rideAssigned", payload);
        verify(someoneElse, never()).sendEvent(any(), any());
    }

    @Test
    @DisplayName("sendToUser for a user with no session is a silent no-op")
    void sendToUser_noSessions_isNoOp() {
        assertThatCode(() -> dispatcher.sendToUser("ghost", "rideAssigned", payload))
            .doesNotThrowAnyException();

        verify(server, never()).getClient(any());
    }

    @Test
    @DisplayName("sendToUser with a null userId does not throw")
    void sendToUser_nullUserId_isNoOp() {
        assertThatCode(() -> dispatcher.sendToUser(null, "evt", payload))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("sendToSession skips a client whose channel is closed")
    void sendToSession_closedChannel_doesNotSend() {
        UUID id = UUID.randomUUID();
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(false);
        when(server.getClient(id)).thenReturn(client);

        dispatcher.sendToSession(id.toString(), "evt", payload);

        verify(client, never()).sendEvent(any(), any());
    }

    @Test
    @DisplayName("sendToSession tolerates an unknown, null or non-UUID session id")
    void sendToSession_invalidSessionId_isNoOp() {
        assertThatCode(() -> dispatcher.sendToSession(null, "evt", payload))
            .doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.sendToSession("not-a-uuid", "evt", payload))
            .doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.sendToSession(UUID.randomUUID().toString(), "evt", payload))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("sendToSession with a null payload sends the bare event")
    void sendToSession_nullPayload_sendsEventWithoutData() {
        UUID id = UUID.randomUUID();
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(true);
        when(server.getClient(id)).thenReturn(client);

        dispatcher.sendToSession(id.toString(), "evt", null);

        verify(client).sendEvent("evt");
    }

    @Test
    @DisplayName("sendToRoom delegates to the room operations, with and without a payload")
    void sendToRoom_delegatesToRoomOperations() {
        BroadcastOperations operations = mock(BroadcastOperations.class);
        when(server.getRoomOperations("driverRoom")).thenReturn(operations);

        dispatcher.sendToRoom("driverRoom", "newRide", payload);
        dispatcher.sendToRoom("driverRoom", "poke", null);

        verify(operations).sendEvent("newRide", payload);
        verify(operations).sendEvent("poke");
    }

    @Test
    @DisplayName("broadcast delegates to the broadcast operations, with and without a payload")
    void broadcast_delegatesToBroadcastOperations() {
        BroadcastOperations operations = mock(BroadcastOperations.class);
        when(server.getBroadcastOperations()).thenReturn(operations);

        dispatcher.broadcast("systemAlert", payload);
        dispatcher.broadcast("systemPing", null);

        verify(operations).sendEvent("systemAlert", payload);
        verify(operations).sendEvent("systemPing");
    }
}
