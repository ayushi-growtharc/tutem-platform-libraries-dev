package com.tutem.platform.socket.heartbeat;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Heartbeat / ghost-cleanup contract.
 *
 * <p>Cleanup is driven by calling the public {@code cleanupStaleSessions()} directly, so
 * these tests are fully deterministic and never sleep. Only
 * {@link #scheduler_passThrows_keepsRunningOnLaterPasses()} exercises the real scheduler,
 * and it waits on a latch rather than sleeping.
 */
class HeartbeatManagerTest {

    private static final int STALE_SECONDS = 60;

    private SocketIOServer server;
    private InMemorySessionManager sessionManager;

    @BeforeEach
    void setUp() {
        server = mock(SocketIOServer.class);
        sessionManager = new InMemorySessionManager();
    }

    /** Default deployment shape: pod-local session store, so ghost eviction is on. */
    private HeartbeatManager manager(boolean disconnectStaleSessions) {
        return new HeartbeatManager(server, sessionManager, STALE_SECONDS, 30,
            disconnectStaleSessions, true);
    }

    /** Shared/distributed session store: {@code evict-ghost-sessions=false}. */
    private HeartbeatManager managerWithoutGhostEviction(boolean disconnectStaleSessions) {
        return new HeartbeatManager(server, sessionManager, STALE_SECONDS, 30,
            disconnectStaleSessions, false);
    }

    private SocketSession save(String userId, Instant lastActiveAt) {
        SocketSession session = SocketSession.builder()
            .sessionId(UUID.randomUUID().toString())
            .userId(userId)
            .lastActiveAt(lastActiveAt)
            .build();
        sessionManager.save(session);
        return session;
    }

    private SocketIOClient liveClientFor(SocketSession session) {
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(true);
        when(server.getClient(UUID.fromString(session.getSessionId()))).thenReturn(client);
        return client;
    }

    // ------------------------------------------------------------------ ghost eviction

    @Test
    @DisplayName("a session whose netty client is gone is evicted")
    void cleanupStaleSessions_clientMissing_evictsGhostSession() {
        SocketSession ghost = save("u1", Instant.now());
        // server.getClient(..) returns null by default -> the transport is gone.

        manager(false).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId(ghost.getSessionId())).isEmpty();
        assertThat(sessionManager.isConnected("u1")).isFalse();
    }

    @Test
    @DisplayName("a session whose netty channel is closed is evicted")
    void cleanupStaleSessions_channelClosed_evictsGhostSession() {
        SocketSession ghost = save("u1", Instant.now());
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(false);
        when(server.getClient(UUID.fromString(ghost.getSessionId()))).thenReturn(client);

        manager(false).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId(ghost.getSessionId())).isEmpty();
        verify(client, never()).disconnect();
    }

    @Test
    @DisplayName("a session id that is not a netty UUID is left alone")
    void cleanupStaleSessions_nonUuidSessionId_isLeftAlone() {
        sessionManager.save(SocketSession.builder()
            .sessionId("redis-backed-session").userId("u1").build());

        manager(true).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId("redis-backed-session")).isPresent();
        verify(server, never()).getClient(any());
    }

    @Test
    @DisplayName("a session id with a null value does not break the pass")
    void cleanupStaleSessions_sessionWithNullId_isSkipped() {
        SessionManager fake = mock(SessionManager.class);
        SocketSession nullIdSession = SocketSession.builder().sessionId(null).userId("u1").build();
        when(fake.getAll()).thenReturn(List.of(nullIdSession));

        HeartbeatManager manager = new HeartbeatManager(server, fake, STALE_SECONDS, 30, true, true);

        assertThatCode(manager::cleanupStaleSessions).doesNotThrowAnyException();
        verify(fake, never()).removeBySessionId(any());
    }

    // ----------------------------------------------------- distributed SessionManager guard

    @Test
    @DisplayName("evictGhostSessions=false leaves a session this server does not own alone")
    void cleanupStaleSessions_ghostEvictionDisabled_keepsSessionOwnedByAnotherPod() {
        // Regression guard for the multi-pod data loss: with a shared session store every
        // session belonging to another pod looks like a ghost to this pod's Netty server.
        SocketSession remote = save("u1", Instant.now());
        // server.getClient(..) returns null -> "not my client", which is normal here.

        managerWithoutGhostEviction(false).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId(remote.getSessionId())).isPresent();
        assertThat(sessionManager.isConnected("u1")).isTrue();
    }

    @Test
    @DisplayName("evictGhostSessions=false also spares a session whose channel reports closed")
    void cleanupStaleSessions_ghostEvictionDisabledAndChannelClosed_keepsSession() {
        SocketSession remote = save("u1", Instant.now());
        SocketIOClient client = mock(SocketIOClient.class);
        when(client.isChannelOpen()).thenReturn(false);
        when(server.getClient(UUID.fromString(remote.getSessionId()))).thenReturn(client);

        managerWithoutGhostEviction(true).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId(remote.getSessionId())).isPresent();
        verify(client, never()).disconnect();
    }

    @Test
    @DisplayName("evictGhostSessions=false still disconnects idle sessions this server owns")
    void cleanupStaleSessions_ghostEvictionDisabled_idleDisconnectStillApplies() {
        SocketSession idle = save("u1", Instant.now().minusSeconds(STALE_SECONDS * 10L));
        SocketIOClient client = liveClientFor(idle);

        managerWithoutGhostEviction(true).cleanupStaleSessions();

        verify(client).disconnect();
        assertThat(sessionManager.findBySessionId(idle.getSessionId())).isEmpty();
    }

    @Test
    @DisplayName("both sweeps disabled -> the session store is not even read")
    void cleanupStaleSessions_bothSweepsDisabled_isANoOp() {
        SessionManager fake = mock(SessionManager.class);
        HeartbeatManager manager = new HeartbeatManager(server, fake, STALE_SECONDS, 30, false, false);

        manager.cleanupStaleSessions();

        verify(fake, never()).getAll();
        verify(fake, never()).removeBySessionId(any());
    }

    // --------------------------------------------------------------- idle disconnection

    @Test
    @DisplayName("disconnectStaleSessions=false leaves an idle but connected session alone")
    void cleanupStaleSessions_idleSessionAndFlagDisabled_doesNotDisconnect() {
        // Regression guard: listen-only mobile clients never send inbound traffic, so they
        // look idle forever. Disconnecting them killed healthy connections.
        SocketSession idle = save("u1", Instant.now().minusSeconds(STALE_SECONDS * 10L));
        SocketIOClient client = liveClientFor(idle);

        manager(false).cleanupStaleSessions();

        verify(client, never()).disconnect();
        assertThat(sessionManager.findBySessionId(idle.getSessionId())).isPresent();
        assertThat(sessionManager.isConnected("u1")).isTrue();
    }

    @Test
    @DisplayName("disconnectStaleSessions=true disconnects and evicts an idle session")
    void cleanupStaleSessions_idleSessionAndFlagEnabled_disconnectsAndEvicts() {
        SocketSession idle = save("u1", Instant.now().minusSeconds(STALE_SECONDS * 10L));
        SocketIOClient client = liveClientFor(idle);

        manager(true).cleanupStaleSessions();

        verify(client).disconnect();
        assertThat(sessionManager.findBySessionId(idle.getSessionId())).isEmpty();
    }

    @Test
    @DisplayName("disconnectStaleSessions=true keeps a recently active session")
    void cleanupStaleSessions_recentlyActiveSession_isKept() {
        SocketSession active = save("u1", Instant.now());
        SocketIOClient client = liveClientFor(active);

        manager(true).cleanupStaleSessions();

        verify(client, never()).disconnect();
        assertThat(sessionManager.findBySessionId(active.getSessionId())).isPresent();
    }

    @Test
    @DisplayName("a null lastActiveAt does not NPE and the session is kept")
    void cleanupStaleSessions_nullLastActiveAt_doesNotThrow() {
        SocketSession session = save("u1", null);
        SocketIOClient client = liveClientFor(session);
        HeartbeatManager manager = manager(true);

        assertThatCode(manager::cleanupStaleSessions).doesNotThrowAnyException();

        verify(client, never()).disconnect();
        assertThat(sessionManager.findBySessionId(session.getSessionId())).isPresent();
    }

    @Test
    @DisplayName("a client that throws on disconnect does not abort the pass")
    void cleanupStaleSessions_disconnectThrows_stillEvictsAndContinues() {
        SocketSession idle = save("u1", Instant.now().minusSeconds(STALE_SECONDS * 10L));
        SocketIOClient client = liveClientFor(idle);
        org.mockito.Mockito.doThrow(new IllegalStateException("channel gone"))
            .when(client).disconnect();

        HeartbeatManager manager = manager(true);
        assertThatCode(manager::cleanupStaleSessions).doesNotThrowAnyException();

        assertThat(sessionManager.findBySessionId(idle.getSessionId())).isEmpty();
    }

    @Test
    @DisplayName("a server that throws on getClient treats the session as a ghost")
    void cleanupStaleSessions_getClientThrows_evictsSession() {
        SocketSession session = save("u1", Instant.now());
        when(server.getClient(UUID.fromString(session.getSessionId())))
            .thenThrow(new IllegalStateException("server not started"));

        manager(false).cleanupStaleSessions();

        assertThat(sessionManager.findBySessionId(session.getSessionId())).isEmpty();
    }

    // ------------------------------------------------------------------------ scheduler

    @Test
    @DisplayName("a failing cleanup pass does not cancel the scheduled task")
    void scheduler_passThrows_keepsRunningOnLaterPasses() throws Exception {
        SessionManager exploding = mock(SessionManager.class);
        AtomicInteger passes = new AtomicInteger();
        CountDownLatch twoPasses = new CountDownLatch(2);
        when(exploding.getAll()).thenAnswer(invocation -> {
            int pass = passes.incrementAndGet();
            twoPasses.countDown();
            if (pass == 1) {
                throw new IllegalStateException("session store unavailable");
            }
            return List.of();
        });

        // cleanupIntervalSeconds is floored at 1s by the implementation, so two passes
        // arrive after roughly 2s; the latch bounds the wait instead of sleeping blindly.
        HeartbeatManager manager = new HeartbeatManager(server, exploding, STALE_SECONDS, 1, false, true);
        manager.afterPropertiesSet();
        try {
            assertThat(twoPasses.await(15, TimeUnit.SECONDS))
                .as("the scheduler must survive a failing pass").isTrue();
        } finally {
            manager.destroy();
        }
        assertThat(passes.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("destroy stops the scheduler and is safe to call twice or without start")
    void destroy_isIdempotentAndSafeWithoutStart() {
        HeartbeatManager neverStarted = manager(false);
        assertThatCode(neverStarted::destroy).doesNotThrowAnyException();

        HeartbeatManager started = manager(false);
        started.afterPropertiesSet();
        assertThatCode(started::destroy).doesNotThrowAnyException();
        assertThatCode(started::destroy).doesNotThrowAnyException();
    }
}
