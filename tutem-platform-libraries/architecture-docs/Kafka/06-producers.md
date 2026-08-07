# Tutem — Kafka Producer Design (Step 8)

> **Status:** DRAFT — awaiting Orchestrator confirmation.
> **Input:** `00-architecture-baseline.md` (APPROVED), `01-kafka-architecture.md` (APPROVED, D8-amended),
> `02-kafka-topics.md` (APPROVED, D8-amended), `03-kafka-events.md` (APPROVED). Every topic, event class,
> partition key and idempotency key below is copied verbatim from those four documents.
> **Does not read or depend on** `04-event-flows.md` (concurrently authored elsewhere).

---

## 0. Producer inventory

Per baseline §0.2.3, **every event topic has exactly one producer** — the `<domain>`-owning service — and
**the command topic is the sole exception**, with multiple producers naming a single executing consumer.
There is therefore no "producer per topic" table to build beyond the 5 owning services plus the
command-topic's 3-producer set:

| Producer | Kind | Event topics owned (verbatim, count) | Also produces to the command topic? |
|---|---|---|---|
| identity-service | outbox relay | `tutem.identity.user.registered.v1`, `tutem.identity.user.profile-updated.v1`, `tutem.identity.user.deleted.v1` (3) | No (baseline §0.2.3: identity-service is explicitly not in the `send-push` producer list) |
| driver-service | outbox relay | `tutem.driver.driver.created.v1`, `tutem.driver.driver.went-online.v1`, `tutem.driver.driver.went-offline.v1`, `tutem.driver.driver.location-updated.v1`, `tutem.driver.vehicle.registered.v1`, `tutem.driver.vehicle.deactivated.v1`, `tutem.driver.document.submitted.v1`, `tutem.driver.document.verified.v1`, `tutem.driver.document.rejected.v1`, `tutem.driver.blacklist.blacklist-applied.v1`, `tutem.driver.blacklist.blacklist-expired.v1` (11) | Yes — F-03.8, F-15.3 |
| dispatch-service | outbox relay | `tutem.dispatch.offer.created.v1`, `tutem.dispatch.offer.accepted.v1`, `tutem.dispatch.offer.rejected.v1`, `tutem.dispatch.offer.expired.v1`, `tutem.dispatch.offer.withdrawn.v1`, `tutem.dispatch.request.created.v1`, `tutem.dispatch.request.matched.v1`, `tutem.dispatch.request.expired.v1`, `tutem.dispatch.request.cancelled.v1` (9) | Yes — F-07.7, F-08.6, F-10.6, F-13.5, F-14.3, F-17.1, F-17.5 |
| trip-service | outbox relay | `tutem.trip.trip.created.v1`, `tutem.trip.trip.started.v1`, `tutem.trip.trip.completed.v1`, `tutem.trip.trip.cancelled.v1`, `tutem.trip.trip.seats-exhausted.v1`, `tutem.trip.booking.confirmed.v1`, `tutem.trip.booking.onboard.v1`, `tutem.trip.booking.completed.v1`, `tutem.trip.booking.cancelled.v1`, `tutem.trip.booking.no-show.v1`, `tutem.trip.booking.paid.v1`, `tutem.trip.booking.payment-failed.v1`, `tutem.trip.booking.refunded.v1`, `tutem.trip.rating.submitted.v1` (14) | Yes — F-09.5, F-10.10, F-17.6 |
| config-service | outbox relay | `tutem.config.config.changed.v1` (1, compacted) | No |
| **(multi-producer command path)** | dispatch-service, trip-service, driver-service (3 producers, 1 consumer) | `tutem.notification.send-push.command.v1` | — see §6 |

3 + 11 + 9 + 14 + 1 = **38 event topics**, each with exactly one producer; + 1 command topic with 3
producers = **39 base topics, every one accounted for.**

### 0.1 D9 note — Redis read-model caches fed by these producers (informational, cross-reference only)

Per **D9 (user decision)**, `05-consumers.md` §4.1/§4.2/§4.4 populate three **new** Redis keys —
`tutem:trip:rider-directory:<user_id>`, `tutem:trip:vehicle-snapshot:<vehicle_id>`,
`tutem:trip:history-projection:<user_id>` — from events **this** document's producers emit: identity-service's
`AppUserRegistered`/`AppUserProfileUpdated`/`AppUserDeleted`, driver-service's `VehicleRegistered`/
`VehicleDeactivated`, and dispatch-service's `ServiceRequestCreated`/`Matched`/`Expired`/`Cancelled`
respectively. No producer in this document writes those keys directly — they are written by the
**consumers** named above, in trip-service. Recorded here only so a reader of this producer document can
trace the full path from commit to cache without cross-referencing `05-consumers.md` blindly. These are
**new keys, not in baseline §0.8's inventory** — see `05-consumers.md` §12 items 1–2.

---

## 1. identity-service

| Attribute | Detail |
|---|---|
| **Transactional boundary** | The `app_user` INSERT (F-01 step 3) / UPDATE (profile change, F-01 step 6) / `status='DELETED'` UPDATE (soft delete, F-01 step 6) **and** the corresponding `outbox_event` INSERT commit **together, in one Postgres transaction, one commit** — identity-service's own schema, per Step 3 §4.1/§4.4. **The Kafka publish is a separate, later, sequential step**, performed by the relay after this transaction has already committed; there is no atomic two-system commit (§3/§4 of Step 3). |
| **Topic(s)** | `tutem.identity.user.registered.v1`, `tutem.identity.user.profile-updated.v1`, `tutem.identity.user.deleted.v1` |
| **Event(s)** | `AppUserRegistered`, `AppUserProfileUpdated`, `AppUserDeleted` |
| **Partition key** | `user_id` — source column `app_user.id`. This is the aggregate's own id; `user` is **not** one of §0.6's 6 keying exceptions. |
| **Ordering guarantee** | Per-partition only. All of one user's lifecycle facts (`registered` → `profile-updated`* → `deleted`) land on one partition **and are guaranteed to publish in commit order under the D10 sharded relay (§9)** — `user` is not one of the two co-located-sibling keys (`offer`→`request_id`, `booking`→`trip_id`) §9 exists to protect, but the same sharding mechanism applies uniformly to every aggregate this producer owns, so `trip-service.rider-directory-cache.v1` sees a coherent state machine for that user. No ordering is claimed or needed across *different* users (baseline §5.2: order-independent by construction for this class). See §9 for the mechanism and its residual resharding caveat. |
| **Error handling** | **Broker unavailable:** the relay's publish attempt fails; `published_at` stays `NULL`; the domain write already committed and is unaffected; the next poll retries. **Publish failure mid-attempt:** the idempotent producer's own retry-within-attempt semantics prevent that attempt from duplicating a record. **Relay crash between publish and marking `published_at`:** the row is republished on the next poll — a duplicate *delivery*, never a duplicate *fact*, absorbed by consumer-side `eventId` de-dup. **Outbox backlog:** surfaced by `tutem_outbox_unpublished_records`/`tutem_outbox_publish_lag_seconds` (§14 of Step 3); low-volume topics here make backlog unlikely in practice. **Poison payload:** the payload is built once, at domain-write time, from already-validated columns — a malformed payload here is a serialization bug, caught by Avro schema validation against the Schema Registry before the record is sent, failing that publish attempt loudly (alertable) rather than corrupting the topic. **Schema Registry unavailable:** the producer cannot resolve/register a schema id, the publish attempt fails, `published_at` stays `NULL` — same backlog/retry path as a broker outage. |
| **Exactly-once strategy** | Idempotent producer (`enable.idempotence=true`) + transactional producer (`transactional.id=identity-service-outbox-relay-<instance>`) cover **only** the relay's own publish-retry de-duplication within a single publish attempt. They do **not**, and cannot, make "publish to Kafka" and "`UPDATE outbox_event SET published_at=...`" one atomic operation — no XA across Postgres and Kafka exists. The genuine end-to-end guarantee is **at-least-once delivery with idempotent consumers** (§0.7); this document never claims exactly-once beyond the relay's own retry scope. |

---

## 2. driver-service

| Attribute | Detail |
|---|---|
| **Transactional boundary** | One `outbox_event` row per domain write, in the **same transaction** as: the `driver` INSERT (F-02 step 1, → `DriverCreated`); `driver.is_online`/`active_mode`/`active_vehicle_id` UPDATE (F-04/F-05, → `DriverWentOnline`/`DriverWentOffline`); the **single indexed** `driver.current_location`/`location_updated_at` UPDATE, which per F-06 step 6 "must never share a transaction with anything else" beyond its own outbox row (→ `DriverLocationUpdated`); `vehicle` INSERT/deactivation UPDATE (→ `VehicleRegistered`/`VehicleDeactivated`); `driver_document` INSERT/status UPDATE (→ `DriverDocumentSubmitted`/`Verified`/`Rejected`); and the combined `driver_blacklist` INSERT + `driver.blacklisted_until`/`is_online=FALSE` UPDATE (→ `DriverBlacklistApplied`) or the reverse (→ `DriverBlacklistExpired`). Every one of these is driver-service's own schema; the Kafka publish for each is a separate, later, sequential relay step. |
| **Topic(s)** | All 11 listed in §0 above. |
| **Event(s)** | `DriverCreated`, `DriverWentOnline`, `DriverWentOffline`, `DriverLocationUpdated`, `VehicleRegistered`, `VehicleDeactivated`, `DriverDocumentSubmitted`, `DriverDocumentVerified`, `DriverDocumentRejected`, `DriverBlacklistApplied`, `DriverBlacklistExpired` |
| **Partition key** | `driver_id` for **every one of the 11 topics** — source column is `driver.user_id` (`driver`'s primary key is `user_id`, NewSchema §4.2 — there is no `driver.id` column) for the `driver` aggregate's own 4 events, and `driver_id` (the *owning* driver, not the sub-resource's own id) for `vehicle`, `document` and `blacklist` — §0.6's 3 of the 6 "keyed by something other than the event's own aggregate id" exceptions besides `offer`/`booking`/`config`. |
| **Ordering guarantee** | Per-`driver_id` partition only. This co-locates a driver's own state changes, vehicle changes, document outcomes and blacklist events all on one partition, **and the D10 sharded relay claim (§9) guarantees they publish in that same commit order** — which is exactly what lets `driver-service.geo-index-maintenance.v1` apply last-write-wins correctly and what lets `driver-service.blacklist-geo-sync.v1` see apply-before-expire in order. No ordering across *different* drivers. See §9 for the mechanism and its residual resharding caveat. |
| **Error handling** | Same mechanics as §1 (broker unavailable → unpublished, no data loss; publish failure → idempotent-producer retry; relay crash → duplicate delivery, not duplicate fact; poison payload → caught at Avro/Schema-Registry validation before send; Schema Registry down → publish attempt fails, retried next poll). **Backlog is the one place driver-service differs materially**: `tutem.driver.driver.location-updated.v1` is >80% of cluster traffic (§2.1 of Step 3), so its outbox is the single most backlog-sensitive queue in the system — `tutem_outbox_publish_lag_seconds` on driver-service is the leading indicator to watch, and the 24-hour retention on this one topic (§13.1 of Step 3) is the accepted bound on how long an extended relay outage can run before the oldest unpublished pings age out of business relevance (not out of the outbox table itself, which has no TTL of its own — see §4). |
| **Exactly-once strategy** | Same as §1: idempotent + transactional producer (`transactional.id=driver-service-outbox-relay-<instance>`) covers only the relay's own retry-duplication; end-to-end is at-least-once with idempotent consumers. No exactly-once claim beyond that scope. |

---

## 3. dispatch-service

| Attribute | Detail |
|---|---|
| **Transactional boundary** | One `outbox_event` row per domain write, same transaction as: the `service_request` INSERT (F-07/F-12, → `ServiceRequestCreated`); the **single conditional accept `UPDATE ride_offer SET status='ACCEPTED' ... WHERE status='SENT' AND expires_at > now()`** (NewSchema §4.6) plus the sibling `WITHDRAWN` updates plus `service_request.status='MATCHED'`, **all one transaction** (→ `RideOfferAccepted` + `RideOfferWithdrawn`(×N) + `ServiceRequestMatched`, one outbox row each, same commit); the `ride_offer.status='REJECTED'` UPDATE (→ `RideOfferRejected`); the expiry-sweep `UPDATE ... WHERE status='SENT' AND expires_at <= now()` (→ `RideOfferExpired`); the request-cancel UPDATE (→ `ServiceRequestCancelled`); and the request-expiry sweep (→ `ServiceRequestExpired`). **This is the accept-race producer**: Kafka only ever broadcasts a verdict `uq_offer_single_accept` has already settled inside this same transaction — it never adjudicates it (§1.2 of Step 3, restated as a hard constraint). |
| **Topic(s)** | All 9 listed in §0 above. |
| **Event(s)** | `RideOfferCreated`, `RideOfferAccepted`, `RideOfferRejected`, `RideOfferExpired`, `RideOfferWithdrawn`, `ServiceRequestCreated`, `ServiceRequestMatched`, `ServiceRequestExpired`, `ServiceRequestCancelled` |
| **Partition key** | `request_id` for **all 9 topics** — both the `offer` aggregate's events (keyed by the *parent* `ServiceRequest`'s id, §0.6's deliberate exception #1) and the `request` aggregate's own events (keyed by its own id, which happens to be the same column). |
| **Ordering guarantee** | Per-`request_id` partition. This is what co-locates **every `RideOffer` row for one `ServiceRequest`** — one per candidate driver, plus the accept/withdraw outcomes — on a single partition, and **under the D10 sharded relay claim (§9) this is now a genuine commit-order guarantee, not an aspiration**: `offer`→`request_id` is one of the two co-located-sibling keys §9 is specifically written to protect. This is exactly what F-08 step 4's sibling-withdraw fan-out and any consumer reconstructing "who won this request" needs, without re-sorting by timestamp (§5.3 of Step 3). No ordering across *different* requests. See §9 for the mechanism and its residual resharding caveat. |
| **Error handling** | Same mechanics as §1/§2. **The accept-race transaction is the one place a publish failure must never be confused with an arbitration failure**: if the outbox relay cannot reach the broker after the accept `UPDATE` has already committed, the winner has already been decided in Postgres — the only consequence is delayed *notification*, never a re-opened race. Poison payload on this producer would most plausibly be a serialization bug in the sibling-withdrawal fan-out (multiple outbox rows per accept transaction) — caught at Avro validation per-row, so one malformed sibling row failing to serialize does not block the others' publish attempts (each outbox row is drained and published independently by the relay). |
| **Exactly-once strategy** | Same as §1/§2: idempotent + transactional producer (`transactional.id=dispatch-service-outbox-relay-<instance>`), relay-retry scope only, at-least-once end-to-end. |

---

## 4. trip-service

| Attribute | Detail |
|---|---|
| **Transactional boundary** | One `outbox_event` row per domain write, same transaction as: the `trip` INSERT — alongside the first `booking` INSERT for RIDE/WALK, or alone for CARPOOL's trip-before-booking sequence (D6/D7) (→ `TripCreated`); `trip.status` UPDATEs for started/completed/cancelled (→ `TripStarted`/`TripCompleted`/`TripCancelled`); the **atomic seat-reservation transaction** — check offer open → check seats → increment `seats_booked` → INSERT `booking`, `ck_trip_seats` as backstop (→ `BookingConfirmed`, and in the same transaction, when the increment makes `seats_booked == seats_total`, the **derived** `TripSeatsExhausted` fact, computed at write time and never read back from a persisted column, D6); the `booking.status` UPDATEs for onboard/completed/cancelled/no-show, cancellation additionally decrementing `trip.seats_booked` **in the same transaction** ("or the offer leaks seats", F-10 step 14) (→ `BookingOnboard`/`BookingCompleted`/`BookingCancelled`/`BookingNoShow`); the `payment_status` UPDATEs (→ `BookingPaid`/`BookingPaymentFailed`/`BookingRefunded`); and the `rating` INSERT (→ `RatingSubmitted`). |
| **Topic(s)** | All 14 listed in §0 above. |
| **Event(s)** | `TripCreated`, `TripStarted`, `TripCompleted`, `TripCancelled`, `TripSeatsExhausted`, `BookingConfirmed`, `BookingOnboard`, `BookingCompleted`, `BookingCancelled`, `BookingNoShow`, `BookingPaid`, `BookingPaymentFailed`, `BookingRefunded`, `RatingSubmitted` |
| **Partition key** | `trip_id` for the 5 `trip`-token topics (own aggregate id) **and** for the 8 `booking`-token topics (the *parent* `Trip`'s id, §0.6's deliberate exception #2 — "deliberately not `booking_id`"); `booking_id` for `RatingSubmitted` (§0.6's deliberate exception — keeps both raters' submissions for one booking mutually ordered). |
| **Ordering guarantee** | Per-`trip_id` partition for 13 of the 14 topics: **every `Booking` event for one `Trip`** — every seat reservation, onboard, drop-off, cancellation on that trip — lands on one partition, and **under the D10 sharded relay claim (§9) publishes ordered with that trip's own lifecycle events as a genuine guarantee**: `booking`→`trip_id` is the second of the two co-located-sibling keys §9 protects. This is what lets a single consumer maintain a strictly ordered view of one carpool trip's evolving seat state without cross-partition joins (§5.3 of Step 3). Per-`booking_id` for `RatingSubmitted`: both directions of one booking's ratings stay ordered under the same sharded-claim mechanism; ordering across *different* bookings' ratings is not claimed (§5.2). See §9 for the mechanism and its residual resharding caveat. |
| **Error handling** | Same mechanics as §1–§3. **The seat-reservation and cancellation-seat-return transactions are the ones where a relay outage has the sharpest downstream consequence**: the DB state (`seats_booked`, `ck_trip_seats`) is always correct the instant the transaction commits — a stalled relay only delays how soon `dispatch-service.request-status-sync.v1` and `realtime-gateway.carpool-seat-fanout.v1` learn about it, never corrupts the seat count itself. Poison payload → caught at Avro validation, per outbox row, before send. Payment-related payloads (`BookingPaid`/`PaymentFailed`/`Refunded`) are produced from already-deduplicated (`payment_ref`) domain writes, so a poison payload here is again a serialization bug, not a business-logic failure. |
| **Exactly-once strategy** | Same as §1–§3: idempotent + transactional producer (`transactional.id=trip-service-outbox-relay-<instance>`), relay-retry scope only, at-least-once end-to-end with idempotent consumers (`uq_trip_one_live_per_provider`, `uq_booking_request`, `uq_rating_once` are the DB backstops on the *consuming* side that make redelivery of these particular facts safe). |

---

## 5. config-service

| Attribute | Detail |
|---|---|
| **Transactional boundary** | The `system_config` UPDATE and the `outbox_event` INSERT commit **together, one Postgres transaction** — identical shape to every other producer. **Distinct from every other producer:** the Redis snapshot refresh (`tutem:config:snapshot`) is config-service's **own separate, synchronous side effect**, performed inline in the same request that wrote the DB row, but it is **not itself part of the Kafka-mediated flow** and is not enlisted in the outbox transaction (03-kafka-events.md §5.1's transaction note, restated). |
| **Topic(s)** | `tutem.config.config.changed.v1` (**compacted**, not `delete`) |
| **Event(s)** | `SystemConfigChanged` |
| **Partition key** | `config_key` — §0.6's third deliberate exception: not a UUID at all, `SystemConfig`'s own natural key. |
| **Ordering guarantee** | Per-`config_key`, **current-value/compacted semantics** rather than an append-only event stream: a newly-joining or long-outage-recovering consumer reads the topic from offset 0 and lands on exactly today's value per key, with no separate bootstrap-from-database step. This is the one topic where "ordering" specifically means "the log-compaction thread never keeps a stale value once a newer one for the same key has been retained." |
| **Error handling** | Broker unavailable / publish failure / relay crash: identical mechanics to §1–§4 — the DB write and the (separate, synchronous) Redis snapshot refresh have already both happened by the time the relay even runs, so a stalled publish only delays how soon *other services'* snapshots refresh, never config-service's own. Outbox backlog: this topic's volume is trivially low (~10-row table, admin-driven changes only), so backlog here is a non-event in practice; the metric still applies uniformly. Poison payload: `value` is transmitted as text and type-coerced by each consumer per its own known key, so a malformed value is a business-logic concern for the admin API to validate before the DB write, not something the outbox/producer layer can catch. Schema Registry unavailable: same as §1 — publish attempt fails, retried next poll; **compacted topics are especially sensitive to a stuck relay** because a consumer bootstrapping from offset 0 during the outage would read a shorter history than intended, though never a wrong one (the DB row and Redis snapshot are unaffected). |
| **Exactly-once strategy** | Same as §1–§4: idempotent + transactional producer (`transactional.id=config-service-outbox-relay-<instance>`), relay-retry scope only. Compaction does not change the exactly-once story — it changes retention semantics, not delivery semantics (§3 of Step 3 is unaffected by cleanup policy). |

---

## 6. Multi-producer command path — `tutem.notification.send-push.command.v1`

The **sole** topic in this catalogue with more than one producer (baseline §0.2.3's deliberate exception).
Each of the 3 producing services publishes `SendPushCommand` from **its own outbox**, in the **same
transaction** as the domain fact that motivated it — there is no separate "notification outbox" and no
shared producer identity; `producerService` in the envelope always disambiguates which of the 3 actually
sent a given record (§0.2.3).

| Attribute | Detail |
|---|---|
| **Transactional boundary (per producer)** | **dispatch-service**: the `SendPushCommand` outbox row is inserted in the *same* transaction as the domain write that motivates it — e.g. F-08's accept transaction inserts both the `RideOfferAccepted` outbox row and the `SendPushCommand` (`OFFER_ACCEPTED_WINNER`) outbox row together (F-07.7, F-08.6, F-10.6, F-13.5, F-14.3, F-17.1, F-17.5). **trip-service**: same pattern — e.g. the trip-completion transaction also inserts the rating-prompt `SendPushCommand` row (F-09.5, F-10.10, F-17.6). **driver-service**: same pattern — e.g. the document-verification transaction also inserts the outcome-notice `SendPushCommand` row (F-03.8, F-15.3). |
| **Topic(s)** | `tutem.notification.send-push.command.v1` (this producer set's only topic) |
| **Event(s)** | `SendPushCommand` (imperative, `Command`-suffixed, no aggregate prefix per §0.4 — command topics carry no aggregate slot) |
| **Partition key** | `user_id` — the recipient's `app_user.id`/`driver.user_id` — per-recipient ordering, avoids an out-of-order push reaching one user (§0.6, command-topic row). |
| **Ordering guarantee** | Per-`user_id` partition. **Within one producer**, ordering is inherited "for free" from that producer's own outbox `created_at` ordering — e.g. dispatch-service's accept-transaction always inserts `RideOfferAccepted` before the `SendPushCommand` row it motivates, in the same commit, so the command is never published ahead of the fact it references. **Across producers**, there is **no coordination at all**: if driver-service and dispatch-service both need to notify the *same* `user_id` at nearly the same instant (an unlikely but not impossible overlap — e.g. a document outcome and an offer alert), which record reaches the partition first is determined purely by relative publish timing between two independent outbox relays in two independent services. This is an accepted property of the design (§0.2.3 explicitly permits multiple producers to this topic), not a defect, because each `SendPushCommand` is self-contained (carries its own `causingEventId`/`templateCode`) and rendering one push before another causes no correctness issue — only a possible display-order nuance the client's `eventId`-based de-dup does not need to resolve, since both commands are legitimately distinct. |
| **Error handling** | Identical per-producer mechanics to §2–§4 (each producer's own relay, own retry, own poison/backlog handling) — this topic introduces no new failure mode beyond "3 independent producers instead of 1." **ACL note (carried from Step 3 §11):** each of the 3 producers holds an individually-enumerated `WRITE` ACL on this topic — never a wildcard grant — so a misconfigured 4th service cannot silently start producing here. |
| **Exactly-once strategy** | Same as every other producer: idempotent + transactional producer per producing service's own relay instance (`transactional.id=<service-name>-outbox-relay-<instance>`), relay-retry scope only. The **consumer** side (`notification-service.push-delivery.v1`) is the one that absorbs at-least-once delivery here — via `tutem:notification:sent:<idempotencyKey>`, keyed off the *causing* domain event's `eventId`, per §11 of the consumer document. |

---

## 7. Producer configuration table

Applies uniformly to **every** outbox-relay producer (identity, driver, dispatch, trip, config) and to
each of the 3 command-topic producers (dispatch, trip, driver use the identical settings — same relay
mechanism, different topic set):

| Config | Value | Justification | Required for idempotence? |
|---|---|---|---|
| `acks` | `all` | Matches `min.insync.replicas=2` on RF=3 (§2 of Step 3) — a producer ack means the record survived on at least 2 of 3 replicas; anything weaker reopens the "acknowledged but lost" gap the outbox pattern exists to close. | **Required** |
| `enable.idempotence` | `true` | Deduplicates the relay's *own* retries of a single publish attempt at the broker level — the mechanism §3 of Step 3 explicitly scopes as "prevents the relay's own retries within one publish attempt from duplicating that attempt's records." | **Required** |
| `max.in.flight.requests.per.connection` | `5` | The maximum value at which Kafka's idempotent-producer sequencing guarantee still holds (broker-side dedup tracks up to 5 in-flight requests per producer session) — going higher silently breaks the exactly-once-per-attempt guarantee `enable.idempotence` is bought for. | **Required — must not exceed 5** |
| `retries` | `Integer.MAX_VALUE` (bounded in practice by `delivery.timeout.ms`) | A retryable send failure (transient broker unavailability) must not be treated as terminal by the producer itself — the *relay's* own retry-on-next-poll is the outer safety net, but a producer that gives up after too few internal retries pushes failures into the backlog metric needlessly early. | **Required — must be > 0** |
| `request.timeout.ms` | `30,000` (Kafka client default) | The per-`send()`-attempt wait for a broker response before that attempt is deemed failed and retried internally. Kept at the Kafka default deliberately — nothing in this platform's latency profile (§2.1 of Step 3: ~1,300–1,500 msg/s, no sub-second ASYNC-CANDIDATE SLA) justifies overriding it. | — |
| `delivery.timeout.ms` | `120,000` (2 minutes) | Bounds the producer's own internal retry budget for a single logical `send()` call (covering however many `request.timeout.ms`-bounded attempts fit inside it, plus `linger.ms`) before it surfaces as a failed attempt to the relay, which then leaves the outbox row `published_at IS NULL` for the next poll — this is the handoff point between "producer keeps trying" and "outbox owns the retry." **Required relationship, verified explicitly:** Kafka requires `delivery.timeout.ms >= linger.ms + request.timeout.ms`. With `linger.ms=20` (below) and `request.timeout.ms=30,000` above, the right-hand side is `20 + 30,000 = 30,020`; `120,000 >= 30,020` holds with wide headroom, comfortably accommodating several internal retry attempts within the delivery-timeout budget before it, too, expires. | **Required — the inequality above must hold** |
| `compression.type` | `lz4` | Balances CPU cost against wire/disk savings at F-06's volume (driver-service dominates cluster throughput, §2.1 of Step 3) without the higher CPU cost of `zstd`; applied uniformly across all 5 producers for one consistent operational profile rather than a per-service override. | — |
| `linger.ms` | `20` | A small deliberate batching delay — the outbox relay already batches rows per poll cycle (§8 below), so a modest producer-side linger further improves batch efficiency on the wire without meaningfully affecting the ASYNC-CANDIDATE latency budget (no flow in baseline §5 has a sub-second async SLA, §4.2 of Step 3). | — |
| `batch.size` | `32,768` bytes (32 KB) | Sized to the outbox relay's own per-poll batch (§8), large enough to amortize request overhead for the F-06-class high-volume producer without over-buffering the low-volume producers (config-service, identity-service), where batches simply flush on `linger.ms` instead. | — |
| `transactional.id` | `<service-name>-outbox-relay-<instance>` | Fixed pattern per §3 of Step 3. **Must be stable across a given instance's restarts** (so Kafka's transaction-fencing correctly recognizes the same logical producer resuming, rather than fencing itself out) **and unique per concurrently-running instance** (so two pods of the same service never fence each other's transactions) — satisfied by composing the stable `<service-name>` with a stable-per-pod instance identifier (e.g. the Kubernetes pod ordinal or a persisted instance id), never a fresh UUID per restart. | Required for the transactional-producer scope (relay-retry hardening), not for the base idempotence guarantee itself |

---

## 8. Outbox relay design — per service

Every one of the 5 owning services runs this identically-shaped relay **inside its own deployable**
(baseline D2's deployable-count discipline; Step 3 §4.2's "relay implementation detail"):

| Aspect | Decision (applies to all 5 services unless noted) |
|---|---|
| **Poll interval** | Platform-wide default **500ms** (K-Q1's recommended single default, per §16.2 of Step 3, pending real-latency-driven per-service overrides in a later tuning pass — not designed here). |
| **Batch size** | `LIMIT 200` rows per poll cycle for the 4 lower-volume services (identity, dispatch, trip, config); `LIMIT 500` for driver-service, sized to its outsized share of cluster volume (F-06, §2.1 of Step 3). |
| **Claim mechanism** | **Per D10 (approved, §9.2):** `SELECT id, aggregate_type, aggregate_id, topic, partition_key, event_type, payload, headers, occurred_at FROM outbox_event WHERE published_at IS NULL AND abs(hashtext(aggregate_id::text)) % :totalShards = :myShard ORDER BY id LIMIT :batchSize FOR UPDATE SKIP LOCKED`. `SKIP LOCKED` still guarantees no two instances ever double-claim (and therefore double-publish) the same row; the added hash-modulo shard filter additionally guarantees that **all rows for one aggregate are always claimed by the same instance** (§9.2/§9.3). |
| **Ordering within one aggregate** | Publishing within a single relay instance's own claimed shard follows `ORDER BY id` (§9.2 explains why `id`, not `created_at`, is the correct clause under sharding). **Under D10 this is now a genuine cross-instance guarantee, not merely a within-batch one — see §9, including the honestly-stated residual resharding-window exposure (§9.4).** |
| **What happens when the relay falls behind** | `tutem_outbox_unpublished_records` (row count where `published_at IS NULL`) and `tutem_outbox_publish_lag_seconds` (age of the oldest such row) are the leading indicators (§14 of Step 3). Falling behind never loses a domain fact — the row simply waits for the next poll or for backlog to be relieved by scaling relay batch size/instance count. A sustained backlog past a topic's retention window (24h for `location-updated`, 7d for most, 3d for the command topic) risks a downstream consumer never seeing that fact if it also crossed offset retention — this is why `tutem_outbox_publish_lag_seconds` is explicitly called the "single most important new metric this step introduces" in Step 3 §14. |
| **Cleanup / archival of published rows** | Published rows (`published_at IS NOT NULL`) are candidates for deletion on a short retention window (24–72 hours) per Step 3 §4.5 — a background housekeeping job, separate from the relay's publish loop, deletes rows older than the window. `outbox_event` is explicitly **not** an audit log (Step 3 §4.5 item 2): nobody reads it as history, so there is no retention requirement beyond bounding table size. |
| **`ix_outbox_unpublished` index** | **Revised under D10 (§9.2)** from Step 3 §4.1's original `CREATE INDEX ix_outbox_unpublished ON outbox_event (created_at) WHERE published_at IS NULL` to `CREATE INDEX ix_outbox_unpublished ON outbox_event (aggregate_id, id) WHERE published_at IS NULL` — still a **partial index on the unpublished subset only** (so it stays small regardless of published/archived-row volume), now clustered by `(aggregate_id, id)` so the sharded claim's per-aggregate ordering can range-scan efficiently instead of hash-filtering the full unpublished set. Infrastructure-only change (§4.5 of Step 3's "not a domain table" status); no `NewSchema.md` impact. |

---

## 9. Per-aggregate ordering under a multi-instance relay — MITIGATED by the D10 sharded claim

**Status: mitigated (user decision D10, 2026-07-29).** The earlier draft of this section identified a
genuine exposure and proposed a sharded claim as the fix; D10 approves that mechanism as the binding
design. This section now states the approved mechanism precisely, including what it does and does not
cover.

### 9.1 The exposure this closes (restated for context)

`SKIP LOCKED` alone guarantees only that two concurrently-running relay instances of the same service can
never both claim the *same* `outbox_event` row. It does **not**, by itself, guarantee that two *different*
rows for the **same `aggregate_id`** (e.g. two `Booking` events for the same `trip_id`, or two `RideOffer`
events for the same `request_id`) are claimed and published by instances in commit order — instance B could
claim a newer sibling row while instance A is still mid-flight publishing an older one, and nothing
constrains which `send()` reaches the broker first across two independent producer sessions.

### 9.2 Approved mechanism — sharded claim by `aggregate_id` hash

Every relay instance's claim query is:

```sql
SELECT id, aggregate_type, aggregate_id, topic, partition_key, event_type, payload, headers, occurred_at
FROM outbox_event
WHERE published_at IS NULL
  AND abs(hashtext(aggregate_id::text)) % :totalShards = :myShard
ORDER BY id
LIMIT :batchSize
FOR UPDATE SKIP LOCKED
```

- **`aggregate_id` is `TEXT`, not `UUID`** — corrected at DDL-derivation time (2026-07-29): `config`'s
  partition/aggregate key is `config_key` (`VARCHAR(64)`), not a UUID, so a UUID-typed column would have
  made config-service physically unable to write its own outbox rows. `hashtext(aggregate_id::text)` still
  reads correctly with a `TEXT` column — the `::text` cast is simply a no-op there now, kept for clarity —
  and **deterministically assigns every outbox row for a given `aggregate_id` to exactly one shard**, for
  the lifetime of that `totalShards` value — **all rows for one aggregate are always claimed by the same
  relay instance**, because they all hash to the same shard and each shard is claimed by exactly one
  instance at a time (§9.3). This is the mechanism that makes per-aggregate publish order a property of
  *which instance ever touches that aggregate's rows*, not of timing luck between independent instances.
- **`ORDER BY id` (not `created_at`) is sufficient, and is now the correct clause — but only because `id` is
  a monotonic `BIGINT GENERATED BY DEFAULT AS IDENTITY`, not a UUID.** This is the load-bearing detail,
  also corrected at DDL-derivation time (2026-07-29): a UUID primary key carries **no ordering information
  at all** — `ORDER BY` a random UUID would have silently defeated this entire per-aggregate ordering
  guarantee while appearing implemented, because two rows for the same aggregate inserted microseconds apart
  could sort in either direction under a UUID key. With `id` as a monotonic identity, two reasons make
  `ORDER BY id` correct: (1) `id` is the outbox row's own generated primary key, monotonically assigned at
  insert time within one service's single Postgres instance — never reordered by clock skew the way two
  `created_at` values from concurrent transactions can be, since `id` generation and the `INSERT`'s
  row-visibility order are the same event; and (2) because sharding already guarantees every row for one
  aggregate is claimed by one instance, that instance's own claim-and-publish loop only needs a **stable,
  monotonic within-shard order** — `id` gives that without depending on transaction-commit-timestamp
  granularity. `ORDER BY created_at` (the pre-D10 clause) remains correct in spirit but `id` is the tighter
  guarantee and is the one D10 specifies. **Note:** the envelope's `eventId` (a UUID) is unaffected by this
  correction — it continues to live inside `payload`, never as the primary key, exactly as `01-kafka-architecture.md`
  §4.1 states.
- **`ix_outbox_unpublished` must change shape to stay efficient under this predicate.** The original
  `CREATE INDEX ix_outbox_unpublished ON outbox_event (created_at) WHERE published_at IS NULL` (Step 3
  §4.1) does not carry `aggregate_id`, so the sharded claim's `WHERE published_at IS NULL AND
  abs(hashtext(aggregate_id::text)) % :totalShards = :myShard` would still need to hash-filter every
  unpublished row rather than seek directly to a shard's rows. The index is revised to:

  ```sql
  CREATE INDEX ix_outbox_unpublished ON outbox_event (aggregate_id, id)
      WHERE published_at IS NULL;
  ```

  This keeps the same partial-index principle (unpublished rows only, so it stays small regardless of
  archived-row volume) while ordering by `(aggregate_id, id)` so a shard's claim can range-scan efficiently
  instead of computing `hashtext()` per row during the scan; the hash-modulo filter is still evaluated per
  candidate row but now against a materially smaller, aggregate-clustered index rather than the full
  unpublished set. **This is a schema change to Kafka platform infrastructure only** (the `outbox_event`
  table, per Step 3 §4.5's "infrastructure, not a domain table" status) — it does not touch any of the 11
  NewSchema domain tables and needs no `NewSchema.md` sign-off.

### 9.3 Shard assignment — `:myShard` / `:totalShards`

- **`:totalShards`** is set to the **current target relay-instance count for that service** (not the
  partition count of any Kafka topic — a deliberately separate number, since one service's outbox feeds
  many topics with different partition counts). It is carried as a piece of runtime configuration the
  service already needs for other purposes (its own desired replica count), not a new independent value to
  keep in sync by hand.
- **`:myShard`** is derived from each instance's **stable ordinal identity** — the same Kubernetes
  StatefulSet-pod-ordinal (or equivalent stable instance id) already required for a correctly-fenced
  `transactional.id` (§7: "`<service-name>-outbox-relay-<instance>`" must be stable per instance). Reusing
  one stable identity for both purposes avoids introducing a second identity scheme.
- Each instance therefore always knows its own `:myShard` and the current `:totalShards` from its own
  runtime environment, with no coordination service or leader election required to compute them.

### 9.4 Scale-out, scale-in, and rolling deploys — the resharding window, stated precisely

Changing `:totalShards` (adding or removing relay instances) **necessarily reshuffles which shard owns
which `aggregate_id`**, exactly as growing a Kafka topic's partition count reshuffles key-to-partition
assignment (§5.1 of Step 3 states the analogous Kafka-side risk explicitly; this is the same phenomenon one
layer earlier).

- **During a rolling deploy or scale event, there is a real, bounded window in which two instances could
  briefly disagree about `:totalShards`** — e.g. instance A has already restarted with the new pod count
  and computes shards accordingly, while instance B has not yet restarted and is still computing against
  the old count. In that window, both A and B could believe they own the shard containing a given
  `aggregate_id`'s rows.
- **What `SKIP LOCKED` still guarantees even here:** neither instance can claim a row the other is currently
  holding a row-lock on — so the *no-double-publish-of-the-same-row* guarantee (§9.1) holds throughout a
  reshard, unconditionally.
- **What is NOT guaranteed during that window:** if aggregate X has two unpublished rows and, mid-reshard,
  A claims one under the old shard mapping while B claims the other under the new mapping (because each is
  filtering by a different `:totalShards` value at that instant and each finds its respective row
  unlocked), the §9.2 ordering guarantee **can briefly degrade back to the pre-D10 exposure (§9.1)** for
  that aggregate's rows straddling the resharding instant.
- **Bound on the window:** it is exactly the rollout duration of a single deploy/scale event — seconds to
  low minutes with a standard rolling-update strategy — and it only matters for an aggregate that (a) has
  more than one unpublished outbox row at that instant **and** (b) is unlucky enough to have those rows
  claimed by two instances computing different `:totalShards` values simultaneously. Given this platform's
  message rate (~1,300–1,500 msg/s cluster-wide, §2.1 of Step 3) and sub-second poll intervals (§8, 500ms
  default), the probability of a specific co-located-sibling pair (an `offer`/`request_id` or
  `booking`/`trip_id` pair) landing exactly in this window is low but **not zero, and must not be
  overclaimed as zero.**
- **Recommended operational discipline to shrink the window further (not a new mechanism, a rollout
  practice):** deploy relay-instance-count changes during low-traffic windows where practical, and prefer
  scaling by whole multiples/divisors that keep the hash-modulo mapping's disruption minimal — the same
  discipline Kafka partition growth already requires (§5.1 of Step 3: "sizing generously now... avoids ever
  needing that operation").

### 9.5 What is now a genuine guarantee vs. what remains a residual risk

- **Genuine guarantee, not an aspiration:** outside of an active resharding window, per-aggregate publish
  order is guaranteed for every aggregate this platform produces, including the two keys where it matters
  architecturally: `offer`→`request_id` (F-08 step 4's sibling-withdraw sequence) and `booking`→`trip_id`
  (F-10/F-11's seat-accounting sequence) — see the cross-references from §§1–4 above.
- **Residual risk, carried to Step 9 honestly:** the brief cross-instance shard-disagreement window during
  a scale-out/scale-in or rolling deploy (§9.4) can still theoretically interleave two rows of the same
  aggregate out of order. This is materially smaller in both probability and blast radius than the pre-D10
  exposure (which existed at all times, not just during a rollout), but it is not eliminated, and this
  document does not claim it is. It is listed alongside the two unbackstopped counters and the carpool
  `pickup_order` invariant already flagged in the consumer document (`05-consumers.md` §11) as one of the
  ordering/idempotency exposures Step 9's risk register should track together.

---

## 10. Summary matrix — producer service → topics → events → partition key

| Producer service | Topic | Event | Partition key |
|---|---|---|---|
| identity-service | `tutem.identity.user.registered.v1` | `AppUserRegistered` | `user_id` |
| identity-service | `tutem.identity.user.profile-updated.v1` | `AppUserProfileUpdated` | `user_id` |
| identity-service | `tutem.identity.user.deleted.v1` | `AppUserDeleted` | `user_id` |
| driver-service | `tutem.driver.driver.created.v1` | `DriverCreated` | `driver_id` |
| driver-service | `tutem.driver.driver.went-online.v1` | `DriverWentOnline` | `driver_id` |
| driver-service | `tutem.driver.driver.went-offline.v1` | `DriverWentOffline` | `driver_id` |
| driver-service | `tutem.driver.driver.location-updated.v1` | `DriverLocationUpdated` | `driver_id` |
| driver-service | `tutem.driver.vehicle.registered.v1` | `VehicleRegistered` | `driver_id` |
| driver-service | `tutem.driver.vehicle.deactivated.v1` | `VehicleDeactivated` | `driver_id` |
| driver-service | `tutem.driver.document.submitted.v1` | `DriverDocumentSubmitted` | `driver_id` |
| driver-service | `tutem.driver.document.verified.v1` | `DriverDocumentVerified` | `driver_id` |
| driver-service | `tutem.driver.document.rejected.v1` | `DriverDocumentRejected` | `driver_id` |
| driver-service | `tutem.driver.blacklist.blacklist-applied.v1` | `DriverBlacklistApplied` | `driver_id` |
| driver-service | `tutem.driver.blacklist.blacklist-expired.v1` | `DriverBlacklistExpired` | `driver_id` |
| dispatch-service | `tutem.dispatch.offer.created.v1` | `RideOfferCreated` | `request_id` |
| dispatch-service | `tutem.dispatch.offer.accepted.v1` | `RideOfferAccepted` | `request_id` |
| dispatch-service | `tutem.dispatch.offer.rejected.v1` | `RideOfferRejected` | `request_id` |
| dispatch-service | `tutem.dispatch.offer.expired.v1` | `RideOfferExpired` | `request_id` |
| dispatch-service | `tutem.dispatch.offer.withdrawn.v1` | `RideOfferWithdrawn` | `request_id` |
| dispatch-service | `tutem.dispatch.request.created.v1` | `ServiceRequestCreated` | `request_id` |
| dispatch-service | `tutem.dispatch.request.matched.v1` | `ServiceRequestMatched` | `request_id` |
| dispatch-service | `tutem.dispatch.request.expired.v1` | `ServiceRequestExpired` | `request_id` |
| dispatch-service | `tutem.dispatch.request.cancelled.v1` | `ServiceRequestCancelled` | `request_id` |
| trip-service | `tutem.trip.trip.created.v1` | `TripCreated` | `trip_id` |
| trip-service | `tutem.trip.trip.started.v1` | `TripStarted` | `trip_id` |
| trip-service | `tutem.trip.trip.completed.v1` | `TripCompleted` | `trip_id` |
| trip-service | `tutem.trip.trip.cancelled.v1` | `TripCancelled` | `trip_id` |
| trip-service | `tutem.trip.trip.seats-exhausted.v1` | `TripSeatsExhausted` | `trip_id` |
| trip-service | `tutem.trip.booking.confirmed.v1` | `BookingConfirmed` | `trip_id` |
| trip-service | `tutem.trip.booking.onboard.v1` | `BookingOnboard` | `trip_id` |
| trip-service | `tutem.trip.booking.completed.v1` | `BookingCompleted` | `trip_id` |
| trip-service | `tutem.trip.booking.cancelled.v1` | `BookingCancelled` | `trip_id` |
| trip-service | `tutem.trip.booking.no-show.v1` | `BookingNoShow` | `trip_id` |
| trip-service | `tutem.trip.booking.paid.v1` | `BookingPaid` | `trip_id` |
| trip-service | `tutem.trip.booking.payment-failed.v1` | `BookingPaymentFailed` | `trip_id` |
| trip-service | `tutem.trip.booking.refunded.v1` | `BookingRefunded` | `trip_id` |
| trip-service | `tutem.trip.rating.submitted.v1` | `RatingSubmitted` | `booking_id` |
| config-service | `tutem.config.config.changed.v1` | `SystemConfigChanged` | `config_key` |
| dispatch-service, trip-service, driver-service (multi-producer) | `tutem.notification.send-push.command.v1` | `SendPushCommand` | `user_id` |

**38/38 event topics, each with exactly one producer; the 1 command topic's 3-producer set is reflected
precisely, per §0.2.3 — 39/39 base topics accounted for.**

---

## 11. Self-check

| Check | Result |
|---|---|
| Every event topic has exactly one producer | ✓ — 38/38, §0/§10 |
| The command topic's producer set matches baseline §0.2.3 + the D3 extension exactly | ✓ — dispatch-service (7 call-sites incl. F-17.1/F-17.5), trip-service (3 call-sites incl. F-17.6), driver-service (2 call-sites); identity-service correctly absent |
| Every partition key matches baseline §0.6, including all documented exceptions | ✓ — `offer`/`request`→`request_id`, `booking`→`trip_id`, `rating`→`booking_id`, `config`→`config_key`, `vehicle`/`document`/`blacklist`→`driver_id`, command→`user_id`; every other topic keys on its own aggregate id |
| No exactly-once-end-to-end claim anywhere | ✓ — every producer section and §9 state at-least-once with idempotent consumers as the honest end-to-end guarantee |
| Every DB write in a transactional-boundary description is attributed to the owning service | ✓ — each producer's boundary lists only the tables that service owns per baseline §2.0 |
| No name invented, renamed, or abbreviated | ✓ — every topic/event/key copied verbatim from Steps 3–5 |
| Multi-instance relay ordering exposure addressed, not hand-waved | ✓ — §9, **mitigated by D10's approved sharded-claim mechanism**; residual resharding-window risk stated honestly, not overclaimed as zero (§9.4/§9.5) |
| D9 (Redis-only read models, no new table) reflected where this document touches them | ✓ — §0.1; `outbox_event` remains the only new table anywhere in this architecture |
| `delivery.timeout.ms >= linger.ms + request.timeout.ms` verified with real numbers, not the unstated default | ✓ — §7: `120,000 >= 20 + 30,000` |
| `ix_outbox_unpublished` re-shaped to match the sharded claim predicate | ✓ — §9.2, new shape `(aggregate_id, id) WHERE published_at IS NULL`, still infrastructure-only, no `NewSchema.md` sign-off implied |
| `outbox_event.id` is a type that actually orders by insert order | ✓ — §9.2, corrected 2026-07-29 at DDL-derivation time to `BIGINT GENERATED BY DEFAULT AS IDENTITY`; a UUID primary key would have silently voided D10's per-aggregate ordering guarantee |
| `outbox_event.aggregate_id`/`partition_key` can hold `config_key` as well as a UUID | ✓ — §9.2, corrected 2026-07-29 to `TEXT`; a `UUID`-typed column would have made config-service unable to write its own outbox rows |

---

**End of Step 8.** Awaiting Orchestrator approval.
