package com.tutem.platform.socket.session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemorySessionManagerTest {

    private InMemorySessionManager manager;

    @BeforeEach
    void setUp() {
        manager = new InMemorySessionManager();
    }

    private static SocketSession session(String sessionId, String userId) {
        return SocketSession.builder().sessionId(sessionId).userId(userId).build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Set<String>> userIndex() {
        return (Map<String, Set<String>>)
            ReflectionTestUtils.getField(manager, "userIdToSessionIds");
    }

    @SuppressWarnings("unchecked")
    private Map<String, SocketSession> bySessionId() {
        return (Map<String, SocketSession>) ReflectionTestUtils.getField(manager, "bySessionId");
    }

    // --------------------------------------------------------------------- multi-device

    @Test
    @DisplayName("two sessions for one user are both retained (multi-device)")
    void findAllByUserId_twoSessionsForSameUser_returnsBoth() {
        manager.save(session("s1", "u1"));
        manager.save(session("s2", "u1"));

        Collection<SocketSession> sessions = manager.findAllByUserId("u1");

        assertThat(sessions).hasSize(2);
        assertThat(sessions).extracting(SocketSession::getSessionId)
            .containsExactlyInAnyOrder("s1", "s2");
        assertThat(manager.getConnectedCount()).isEqualTo(2);
        assertThat(manager.isConnected("u1")).isTrue();
    }

    @Test
    @DisplayName("removing one of a user's sessions leaves the other reachable")
    void removeBySessionId_oneOfTwo_leavesTheOtherReachable() {
        manager.save(session("s1", "u1"));
        manager.save(session("s2", "u1"));

        manager.removeBySessionId("s1");

        assertThat(manager.findBySessionId("s1")).isEmpty();
        assertThat(manager.findBySessionId("s2")).isPresent();
        assertThat(manager.findAllByUserId("u1")).extracting(SocketSession::getSessionId)
            .containsExactly("s2");
        assertThat(manager.isConnected("u1")).isTrue();
        assertThat(manager.getConnectedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("removing the last session prunes the user index and flips isConnected to false")
    void removeBySessionId_lastSession_prunesUserIndex() {
        manager.save(session("s1", "u1"));
        manager.save(session("s2", "u1"));

        manager.removeBySessionId("s1");
        manager.removeBySessionId("s2");

        assertThat(manager.findAllByUserId("u1")).isEmpty();
        assertThat(manager.isConnected("u1")).isFalse();
        assertThat(manager.getConnectedCount()).isZero();
        assertThat(userIndex()).doesNotContainKey("u1");
    }

    @Test
    @DisplayName("isConnected is never true while findAllByUserId is empty")
    void isConnected_neverDisagreesWithFindAllByUserId() {
        assertThat(manager.isConnected("u1")).isFalse();
        assertThat(manager.findAllByUserId("u1")).isEmpty();

        manager.save(session("s1", "u1"));
        assertThat(manager.isConnected("u1")).isTrue();

        // Simulate the authoritative map losing the entry without the index being pruned:
        // isConnected must still follow bySessionId, not the stale index.
        bySessionId().remove("s1");
        assertThat(manager.findAllByUserId("u1")).isEmpty();
        assertThat(manager.isConnected("u1")).isFalse();
    }

    @Test
    @DisplayName("a session with no userId is still reachable by sessionId")
    void save_anonymousSession_isReachableBySessionIdOnly() {
        manager.save(session("s1", null));

        assertThat(manager.findBySessionId("s1")).isPresent();
        assertThat(manager.getConnectedCount()).isEqualTo(1);
        assertThat(userIndex()).isEmpty();
        assertThat(manager.findAllByUserId(null)).isEmpty();
    }

    @Test
    @DisplayName("null / unknown inputs are handled without throwing")
    void nullAndUnknownInputs_areTolerated() {
        manager.save(null);
        manager.save(SocketSession.builder().sessionId(null).userId("u1").build());
        manager.removeBySessionId(null);
        manager.removeBySessionId("does-not-exist");

        assertThat(manager.getConnectedCount()).isZero();
        assertThat(manager.findBySessionId(null)).isEmpty();
        assertThat(manager.findAllByUserId("nobody")).isEmpty();
        assertThat(manager.isConnected(null)).isFalse();
    }

    @Test
    @DisplayName("deprecated findByUserId returns one of the user's sessions")
    void findByUserId_multipleSessions_returnsOneOfThem() {
        manager.save(session("s1", "u1"));
        manager.save(session("s2", "u1"));

        assertThat(manager.findByUserId("u1")).isPresent();
        assertThat(manager.findByUserId("nobody")).isEmpty();
    }

    // ------------------------------------------------------------------------ snapshots

    @Test
    @DisplayName("getAll returns an immutable snapshot that later mutations do not affect")
    void getAll_returnsImmutableSnapshot() {
        manager.save(session("s1", "u1"));
        Collection<SocketSession> snapshot = manager.getAll();
        assertThat(snapshot).hasSize(1);

        manager.save(session("s2", "u2"));
        manager.removeBySessionId("s1");

        assertThat(snapshot).as("snapshot must not track later mutations").hasSize(1);
        assertThat(manager.getAll()).hasSize(1);
        assertThatThrownBy(() -> snapshot.add(session("s3", "u3")))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("findAllByUserId returns an immutable collection")
    void findAllByUserId_returnsImmutableCollection() {
        manager.save(session("s1", "u1"));
        Collection<SocketSession> sessions = manager.findAllByUserId("u1");

        assertThatThrownBy(() -> sessions.add(session("s9", "u1")))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ---------------------------------------------------------------------- concurrency

    @Test
    @DisplayName("concurrent save/remove leaves the user index consistent with the session map")
    void saveAndRemove_underConcurrency_keepsIndexConsistent() throws Exception {
        int threads = 8;
        int opsPerThread = 200;
        int users = 4;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try {
            for (int t = 0; t < threads; t++) {
                final int threadIndex = t;
                executor.execute(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String sessionId = threadIndex + "-" + i;
                            manager.save(session(sessionId, "u" + (i % users)));
                            // Half the sessions are immediately removed again; the other half
                            // survive, so the expected end state is exact.
                            if (i % 2 == 0) {
                                manager.removeBySessionId(sessionId);
                            }
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(null, e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(30, TimeUnit.SECONDS)).as("workers finished").isTrue();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(failure.get()).isNull();

        int expectedSurvivors = threads * (opsPerThread / 2);
        assertThat(manager.getConnectedCount()).isEqualTo(expectedSurvivors);
        assertThat(manager.getAll()).hasSize(expectedSurvivors);

        Map<String, Set<String>> index = userIndex();
        Set<String> liveSessionIds = new HashSet<>(bySessionId().keySet());
        int indexedTotal = 0;
        for (Map.Entry<String, Set<String>> entry : index.entrySet()) {
            assertThat(entry.getValue())
                .as("user index must never keep an empty set for %s", entry.getKey())
                .isNotEmpty();
            assertThat(liveSessionIds)
                .as("user index must not keep dangling session ids for %s", entry.getKey())
                .containsAll(entry.getValue());
            indexedTotal += entry.getValue().size();
        }
        assertThat(indexedTotal)
            .as("every surviving session must be indexed exactly once")
            .isEqualTo(expectedSurvivors);

        int reachableByUser = 0;
        for (int u = 0; u < users; u++) {
            reachableByUser += manager.findAllByUserId("u" + u).size();
        }
        assertThat(reachableByUser).isEqualTo(expectedSurvivors);
    }

    // --------------------------------------------------------------------- re-save

    @Test
    @DisplayName("re-saving a sessionId under a new userId un-indexes it from the old user")
    void save_sameSessionIdDifferentUser_dropsStaleIndexEntry() {
        manager.save(session("s1", "userA"));
        manager.save(session("s1", "userB"));

        // Without the un-index, userA's index would still hold s1 while bySessionId points at
        // userB's session, so sendToUser("userA", ...) would deliver to userB's socket.
        assertThat(manager.findAllByUserId("userA")).isEmpty();
        assertThat(manager.isConnected("userA")).isFalse();
        assertThat(userIndex()).doesNotContainKey("userA");

        assertThat(manager.findAllByUserId("userB"))
            .singleElement()
            .satisfies(s -> assertThat(s.getUserId()).isEqualTo("userB"));
        assertThat(manager.getConnectedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("re-saving under a new userId keeps the old user's OTHER sessions")
    void save_sameSessionIdDifferentUser_keepsSiblingSessions() {
        manager.save(session("s1", "userA"));
        manager.save(session("s2", "userA"));

        manager.save(session("s1", "userB"));

        assertThat(manager.findAllByUserId("userA"))
            .singleElement()
            .satisfies(s -> assertThat(s.getSessionId()).isEqualTo("s2"));
        assertThat(manager.findAllByUserId("userB")).hasSize(1);
        assertThat(manager.getConnectedCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("re-saving the same sessionId for the same user is idempotent")
    void save_sameSessionIdSameUser_isIdempotent() {
        manager.save(session("s1", "userA"));
        manager.save(session("s1", "userA"));

        assertThat(manager.findAllByUserId("userA")).hasSize(1);
        assertThat(userIndex().get("userA")).containsExactly("s1");
        assertThat(bySessionId()).hasSize(1);
    }
}
