package com.tutem.platform.socket.connection;

import com.corundumstudio.socketio.BroadcastOperations;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ConnectionManagerTest {

    private static final String ROOM = "drivers";

    private SocketIOServer server;
    private SessionManager sessionManager;
    private ConnectionManager connectionManager;

    @BeforeEach
    void setUp() {
        server = mock(SocketIOServer.class);
        sessionManager = mock(SessionManager.class);
        connectionManager = new ConnectionManager(server, sessionManager);
    }

    private static SocketSession session(String sessionId, String userId) {
        return SocketSession.builder().sessionId(sessionId).userId(userId).build();
    }

    /** Registers a live client for {@code id} and returns the mock. */
    private SocketIOClient liveClient(UUID id) {
        SocketIOClient client = mock(SocketIOClient.class);
        when(server.getClient(id)).thenReturn(client);
        return client;
    }

    // ------------------------------------------------------------------ delegation

    @Test
    @DisplayName("isConnected and getConnectedCount delegate to the SessionManager")
    void presenceQueries_delegateToSessionManager() {
        when(sessionManager.isConnected("u1")).thenReturn(true);
        when(sessionManager.getConnectedCount()).thenReturn(7);

        assertThat(connectionManager.isConnected("u1")).isTrue();
        assertThat(connectionManager.getConnectedCount()).isEqualTo(7);
    }

    @Test
    @DisplayName("getClientsInRoom reads the server's room operations")
    void getClientsInRoom_readsRoomOperations() {
        SocketIOClient client = mock(SocketIOClient.class);
        BroadcastOperations operations = mock(BroadcastOperations.class);
        when(operations.getClients()).thenReturn(List.of(client));
        when(server.getRoomOperations(ROOM)).thenReturn(operations);

        assertThat(connectionManager.getClientsInRoom(ROOM)).containsExactly(client);
    }

    // ------------------------------------------------------------------ rooms

    @Test
    @DisplayName("joinRoom and leaveRoom act on the live client for the session id")
    void roomMembership_appliedToLiveClient() {
        UUID id = UUID.randomUUID();
        SocketIOClient client = liveClient(id);

        connectionManager.joinRoom(id.toString(), ROOM);
        connectionManager.leaveRoom(id.toString(), ROOM);

        verify(client).joinRoom(ROOM);
        verify(client).leaveRoom(ROOM);
    }

    @Test
    @DisplayName("a session id naming no live client is a silent no-op, not an exception")
    void roomMembership_unknownSessionId_isNoOp() {
        UUID id = UUID.randomUUID();
        when(server.getClient(id)).thenReturn(null);

        assertThatCode(() -> connectionManager.joinRoom(id.toString(), ROOM))
            .doesNotThrowAnyException();
        assertThatCode(() -> connectionManager.leaveRoom(id.toString(), ROOM))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a non-UUID session id is swallowed, never surfaced as IllegalArgumentException")
    void roomMembership_malformedSessionId_isNoOp() {
        // A custom SessionManager can hand back ids this server never issued; the caller must
        // not have to defend against UUID.fromString blowing up.
        assertThatCode(() -> connectionManager.joinRoom("not-a-uuid", ROOM))
            .doesNotThrowAnyException();
        assertThatCode(() -> connectionManager.leaveRoom("not-a-uuid", ROOM))
            .doesNotThrowAnyException();

        verify(server, never()).getClient(any());
    }

    @Test
    @DisplayName("a null session id is a no-op and never reaches the server")
    void roomMembership_nullSessionId_isNoOp() {
        assertThatCode(() -> connectionManager.joinRoom(null, ROOM))
            .doesNotThrowAnyException();

        verifyNoInteractions(server);
    }

    // ------------------------------------------------------------------ disconnect

    @Test
    @DisplayName("disconnect closes every live session of the user (multi-device)")
    void disconnect_closesAllSessionsForUser() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        SocketIOClient phone = liveClient(first);
        SocketIOClient tablet = liveClient(second);
        when(sessionManager.findAllByUserId("u1"))
            .thenReturn(List.of(session(first.toString(), "u1"),
                session(second.toString(), "u1")));

        connectionManager.disconnect("u1");

        verify(phone).disconnect();
        verify(tablet).disconnect();
    }

    @Test
    @DisplayName("disconnect skips stale ids and still closes the sessions that are live")
    void disconnect_staleSessionId_doesNotStopTheRest() {
        UUID live = UUID.randomUUID();
        UUID stale = UUID.randomUUID();
        SocketIOClient client = liveClient(live);
        when(server.getClient(stale)).thenReturn(null);
        // Deliberately stale first: a store entry this server no longer owns must not abort
        // the loop before the genuinely live session is closed.
        when(sessionManager.findAllByUserId("u1"))
            .thenReturn(List.of(session(stale.toString(), "u1"),
                session(live.toString(), "u1")));

        connectionManager.disconnect("u1");

        verify(client).disconnect();
    }

    @Test
    @DisplayName("disconnect for a user with no sessions touches no client")
    void disconnect_noSessions_isNoOp() {
        when(sessionManager.findAllByUserId("ghost")).thenReturn(List.of());

        assertThatCode(() -> connectionManager.disconnect("ghost")).doesNotThrowAnyException();

        verify(server, never()).getClient(any());
    }
}
