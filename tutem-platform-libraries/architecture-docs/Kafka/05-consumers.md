# Tutem — Kafka Consumer Design (Step 7)

> **Status:** DRAFT — awaiting Orchestrator confirmation.
> **Input:** `00-architecture-baseline.md` (APPROVED), `01-kafka-architecture.md` (APPROVED, D8-amended),
> `02-kafka-topics.md` (APPROVED, D8-amended), `03-kafka-events.md` (APPROVED) — every consumer group,
> topic, event class, partition key and idempotency key below is copied verbatim from those four documents.
> Nothing is renamed, abbreviated or invented; anything Steps 4/5 do not supply is recorded in §7
> (Unresolved) rather than assumed.
> **Does not read or depend on** `04-event-flows.md` (concurrently authored elsewhere).

---

## 0. How this catalogue was built

`03-kafka-events.md` §7.2 ("Consumer group → topics subscribed") is the authoritative, exhaustive list of
every consumer group in this system — **30 groups**, cross-checked below against §7.1 (event→topic→producer→
consumers) and the topic catalogue's Consumers column (`02-kafka-topics.md` §1). No consumer group is added,
removed or renamed here; this document adds the nine required operational attributes to each.

**By owning service:**

| Service | Consumer groups | Count |
|---|---|---|
| driver-service | geo-index-maintenance, parivahan-verification, verification-status-recompute, blacklist-geo-sync, blacklist-evaluation, total-trips-increment, rating-average-recompute, config-snapshot-refresh | 8 |
| identity-service | rating-average-recompute, config-snapshot-refresh | 2 |
| dispatch-service | user-deletion-cleanup, carpool-matching, request-status-sync, config-snapshot-refresh | 4 |
| trip-service | rider-directory-cache, vehicle-snapshot-cache, trip-provisioning, history-projection, config-snapshot-refresh | 5 |
| notification-service | driver-welcome, push-delivery, config-snapshot-refresh | 3 |
| api-gateway | config-snapshot-refresh | 1 |
| realtime-gateway | driver-presence-fanout, driver-location-fanout, offer-countdown-fanout, trip-status-fanout, carpool-seat-fanout, payment-status-fanout, config-snapshot-refresh | 7 |
| **Total** | | **30** |

**Retry-tier qualification (D8, §7.0 of `01-kafka-architecture.md`):** only 3 of these 30 groups make a
retryable call to an external system and therefore use `.retry-1/2/3` on exhaustion of an in-process
attempt before falling to `.dlq`: `driver-service.parivahan-verification.v1` (Parivahan), and the two FCM
consumers `notification-service.driver-welcome.v1` and `notification-service.push-delivery.v1`. **The
other 27 retry in-process, bounded, then go straight to `.dlq` — no retry-topic hop.**

---

## 1. driver-service consumers

### 1.1 `driver-service.geo-index-maintenance.v1`

| Attribute | Detail |
|---|---|
| Purpose | Persist each driver location ping into `driver.current_location`/`location_updated_at` and refresh the `tutem:driver:geo:<active_mode>` Redis GEO mirror that backs nearby-driver search (§4.12-A). |
| Consumer Group | `driver-service.geo-index-maintenance.v1` |
| Topics | `tutem.driver.driver.location-updated.v1` |
| Retry Strategy | **In-process bounded backoff, no retry topic** — this topic is not in D8's 3-qualifying list (consumer only touches Postgres/Redis). |
| DLQ | `tutem.driver.driver.location-updated.v1.dlq` — lands: malformed coordinate payloads, exhausted-retry writes (e.g. sustained connection-pool exhaustion). Redrive per §7.4 once the underlying DB/Redis issue is fixed; a redrive here is usually superseded by a fresher ping anyway (last-write-wins), so redrive priority is low. |
| Ordering | Per-`driver_id` partition, last-write-wins **by `occurredAt`, never publish time** — genuinely required, because an out-of-order apply would show a stale position as current. |
| Idempotency | `driver:<driverId>:location-updated:<occurredAt-iso>`. Primary backstop is a **conditional `UPDATE driver SET current_location=…, location_updated_at=… WHERE location_updated_at < :occurredAt`** — a DB-enforced last-write-wins guard, not a unique index, but it is the DB winning over Redis per §0.7. `tutem:ops:idem:driver-service.geo-index-maintenance.v1:<eventId>` is the secondary Redis dedup layer. |
| Redis usage | `tutem:driver:geo:<active_mode>` (GEO write), `tutem:driver:last-ping:<driverId>`, `tutem:ops:idem:driver-service.geo-index-maintenance.v1:<eventId>` |
| Database updates | `driver.current_location`, `driver.location_updated_at` — **driver-service owns `driver`.** |
| Failure handling | Transient DB blip → in-process retry. Poison payload → `.dlq` directly. Redis down → the Postgres write still proceeds (Redis is a derived read model, §9 of Step 3); nearby search falls back to a direct `ix_driver_available` PostGIS query. DB down → offset not committed, consumer backs off; the 24h retention on this topic is the accepted risk window (stale pings are disposable by design, §2.1.1). No external system involved. |

### 1.2 `driver-service.parivahan-verification.v1`

| Attribute | Detail |
|---|---|
| Purpose | Decouple the DL/RC upload API response from the slow, flaky Parivahan SDK call; performs the actual verification and writes the outcome. |
| Consumer Group | `driver-service.parivahan-verification.v1` |
| Topics | `tutem.driver.document.submitted.v1` |
| Retry Strategy | **Qualifies for retry tiers (D8, §7.0)** — the failure surface is the Parivahan SDK. `.retry-1` (30s) → `.retry-2` (5min) → `.retry-3` (30min) → `.dlq`. |
| DLQ | `tutem.driver.document.submitted.v1.dlq` — lands: Parivahan calls that failed on all 3 retry attempts, or a malformed submission record. Redrive re-publishes the original `documentId`/`eventId` to `tutem.driver.document.submitted.v1` once Parivahan recovers or a code fix ships. |
| Ordering | Per-`driver_id` — not required for correctness (each document is independently verified), but co-location avoids cross-partition head-of-line effects between a driver's DL and RC submissions. |
| Idempotency | `document:<documentId>:submitted`. Backstop: `driver_document.status` transitions are guarded by `ck_doc_verified`/`ck_doc_rejected` CHECK constraints — a duplicate delivery re-runs Parivahan (wasteful but harmless) and the terminal `UPDATE ... WHERE status='PENDING'` is naturally idempotent (a second attempt on an already-`VERIFIED`/`REJECTED` row is a no-op). `tutem:ops:idem:driver-service.parivahan-verification.v1:<eventId>` short-circuits the wasted re-call. |
| Redis usage | `tutem:ops:idem:driver-service.parivahan-verification.v1:<eventId>` |
| Database updates | `driver_document.status`, `verified_at`/`rejection_reason`, `parivahan_details` — **driver-service owns `driver_document`.** Also publishes `DriverDocumentVerified`/`DriverDocumentRejected` via its own outbox in the same transaction. |
| Failure handling | Parivahan timeout/5xx → retry-tier hop (never blocks the poll loop, see §3). Poison record (deserialization failure) → straight to `.dlq`. Redis down → idempotency check fails open to "not yet seen", worst case one duplicate Parivahan call — acceptable, not a correctness bug (the DB CHECK-guarded UPDATE is still the real backstop). DB down → offset withheld, consumer backs off; Parivahan is unaffected. Parivahan down for an extended outage → all 3 tiers exhaust, lands in `.dlq`, driver stays `PENDING` until redrive. |

### 1.3 `driver-service.verification-status-recompute.v1`

| Attribute | Detail |
|---|---|
| Purpose | Recompute the driver's aggregate `verification_status` once each individual document's outcome is known (a driver needs both DL and RC verified). |
| Consumer Group | `driver-service.verification-status-recompute.v1` |
| Topics | `tutem.driver.document.verified.v1`, `tutem.driver.document.rejected.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Not qualifying — this consumer never calls Parivahan again, only recomputes an aggregate from already-verified rows. |
| DLQ | `tutem.driver.document.verified.v1.dlq`, `tutem.driver.document.rejected.v1.dlq` |
| Ordering | Per-`driver_id` — matters, because the recompute reads all of that driver's `driver_document` rows and a stale read mid-transition is possible but self-correcting on the next event. |
| Idempotency | `document:<documentId>:verified` / `:rejected`. The recompute is a **full re-derivation from current `driver_document` state** (`SELECT` all docs for the driver, apply the go-online rule), not an increment — naturally idempotent, no DB unique index needed as a backstop because a duplicate delivery recomputes the same answer. |
| Redis usage | `tutem:ops:idem:driver-service.verification-status-recompute.v1:<eventId>` (belt-and-suspenders only; not load-bearing given the recompute's natural idempotency) |
| Database updates | `driver.verification_status` — **driver-service owns `driver`.** |
| Failure handling | Transient DB error → in-process retry → `.dlq` on exhaustion. Poison record → `.dlq` directly. Redis down → no impact (idempotency is structural, not Redis-dependent). DB down → offset withheld until DB recovers. |

### 1.4 `driver-service.blacklist-geo-sync.v1`

| Attribute | Detail |
|---|---|
| Purpose | Evict a newly-blacklisted driver from `tutem:driver:geo:<active_mode>` (so they stop appearing in nearby search) and re-admit them on expiry. |
| Consumer Group | `driver-service.blacklist-geo-sync.v1` |
| Topics | `tutem.driver.blacklist.blacklist-applied.v1`, `tutem.driver.blacklist.blacklist-expired.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Redis-only failure surface, non-qualifying. |
| DLQ | `tutem.driver.blacklist.blacklist-applied.v1.dlq`, `tutem.driver.blacklist.blacklist-expired.v1.dlq` |
| Ordering | Per-`driver_id` — matters (an expire arriving before its own apply, out of order, would leave the driver wrongly evicted or wrongly present). |
| Idempotency | `blacklist:<blacklistId>:blacklist-applied:<triggerDate>` / `:blacklist-expired`. GEO-set `ZREM`/re-`GEOADD` operations are naturally idempotent (removing an absent member or re-adding a present one is a no-op) — no DB write occurs here at all, so no DB backstop is needed or possible. |
| Redis usage | `tutem:driver:geo:<active_mode>` (evict/re-admit), `tutem:ops:idem:driver-service.blacklist-geo-sync.v1:<eventId>` |
| Database updates | **None.** This consumer only mutates Redis; `driver_blacklist`/`driver.blacklisted_until` were already written synchronously by the producing transaction (§2.10/§2.11 of Step 5). |
| Failure handling | Redis blip → in-process retry (idempotent op, safe to repeat) → `.dlq` on exhaustion. Poison record → `.dlq`. Redis down for the duration → the driver remains visible in geo search a little longer than the true blacklist window (bounded staleness, not a correctness break since `driver.blacklisted_until` in Postgres is still authoritative and go-online/offer-accept checks re-validate it there). No external system. |

### 1.5 `driver-service.blacklist-evaluation.v1`

| Attribute | Detail |
|---|---|
| Purpose | Apply the D4 rejection-threshold rule: count today's `RideOffer` rows with `status IN ('REJECTED','EXPIRED')` for a driver against `driver.rejection.daily_threshold`, and blacklist on breach. Triggered from **both** the driver-response path and the timer-driven expiry sweep. |
| Consumer Group | `driver-service.blacklist-evaluation.v1` |
| Topics | `tutem.dispatch.offer.rejected.v1`, `tutem.dispatch.offer.expired.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. The count query and the config-threshold read are both local (Postgres + Redis snapshot), not external. |
| DLQ | `tutem.dispatch.offer.rejected.v1.dlq`, `tutem.dispatch.offer.expired.v1.dlq` |
| Ordering | Per-`request_id` on the source topic (dispatch's partitioning) — **not required for this consumer's own correctness**: the evaluation is a same-day `COUNT(*)` over all of a driver's offers regardless of which request they belong to, so cross-request interleaving is irrelevant. |
| Idempotency | Envelope `idempotencyKey` is `offer:<offerId>:rejected`/`:expired` on the *inbound* fact; the *outbound* `DriverBlacklistApplied` is guarded by `uq_bl_one_per_day` (NewSchema) — **DB unique index is the authoritative backstop**, so a duplicate delivery of `RideOfferRejected`/`RideOfferExpired` re-runs the count and re-attempts the insert, which the unique index harmlessly rejects. |
| Redis usage | `tutem:driver:rejection-count:<driverId>:<date>` (read-through cache of the count — **not authoritative**, the `COUNT(*)` is), `tutem:config:snapshot` (read, for `driver.rejection.daily_threshold`), `tutem:ops:idem:driver-service.blacklist-evaluation.v1:<eventId>` |
| Database updates | `driver_blacklist` INSERT + `driver.blacklisted_until`/`is_online=FALSE` — **driver-service owns both tables.** The count itself reads `ride_offer`, which **dispatch-service owns exclusively** (§2.0 of the baseline: "dispatch-service only"). Per the ownership rule, this consumer obtains the count via an **internal API hop to dispatch-service** — **proposed as `GET /internal/v1/offers/rejection-count?driverId={driverId}&since={date}`, PROPOSED pending approval, see §12 item 3** — not direct cross-schema SQL. `03-kafka-events.md`'s illustrative `SELECT COUNT(*) FROM ride_offer` is read here as the *query shape*, executed inside dispatch-service (`status IN ('REJECTED','EXPIRED')` per D4) and returned over that internal API, not as literal cross-service SQL. |
| Failure handling | Transient DB/API error → in-process retry → `.dlq`. Poison record → `.dlq`. Redis down → the count falls back to the always-authoritative dispatch-service call; `system_config` falls back to a direct read if the snapshot is stale. Internal-API-to-dispatch-service down → this is a synchronous dependency introduced by the ownership rule; treat as a retryable transient failure (in-process backoff), escalating to `.dlq` only after exhaustion — **flagged**: dispatch-service unavailability now blocks blacklist evaluation, a cross-service coupling worth carrying into Step 9. |

### 1.6 `driver-service.total-trips-increment.v1`

| Attribute | Detail |
|---|---|
| Purpose | Increment `driver.total_trips` on every completed trip (RIDE, CARPOOL, or WALK — one topic, one consumer, mode-agnostic). |
| Consumer Group | `driver-service.total-trips-increment.v1` |
| Topics | `tutem.trip.trip.completed.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Postgres-only failure surface. |
| DLQ | `tutem.trip.trip.completed.v1.dlq` |
| Ordering | Per-`trip_id` on the source topic — irrelevant to this consumer's own correctness (each trip contributes exactly one increment, independent of other trips). |
| Idempotency | `trip:<tripId>:completed`. **No database unique-index backstop exists for this write** — `driver.total_trips` is a plain counter column with no idempotent-increment mechanism (unlike `verification-status-recompute`, this is a literal `+1`, not a full recompute). Sole mitigation: `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` (Redis, TTL > max retry/replay window). **Flagged risk (carried from baseline §16.4 items 1–2, F-09 step 5 and F-11 step 6 — same consumer, same mechanism, same caveat): a Redis loss concurrent with a redelivery is a genuine, narrow double-count risk.** |
| Redis usage | `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` — **this is the sole idempotency mechanism; no DB backstop.** |
| Database updates | `driver.total_trips` — **driver-service owns `driver`.** |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison record → `.dlq`. **Redis down concurrent with a redelivery → possible double increment, no automatic detection** (this is the open risk carried to Step 9 — see §6). DB down → offset withheld, consumer backs off, no data loss (Kafka retains). No external system. |

### 1.7 `driver-service.rating-average-recompute.v1`

| Attribute | Detail |
|---|---|
| Purpose | Refresh the denormalised `rating_avg`/`rating_count` on `driver` whenever the driver is the ratee. |
| Consumer Group | `driver-service.rating-average-recompute.v1` |
| Topics | `tutem.trip.rating.submitted.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Postgres-only. |
| DLQ | `tutem.trip.rating.submitted.v1.dlq` |
| Ordering | Per-`booking_id` on the source topic — **not required for this consumer's write**, because the recompute reads `AVG(score)`/`COUNT(*)` over all of the driver's ratings, which is order-independent; the partition key exists to keep *both directions* of one booking's ratings together, a property this consumer doesn't need but inherits for free. |
| Idempotency | `rating:<ratingId>:submitted`. The write is `UPDATE driver SET rating_avg = (SELECT AVG(score) ...), rating_count = (SELECT COUNT(*) ...) WHERE id = :rateeId` — a **full recompute, not an increment** — naturally idempotent; a duplicate delivery recomputes the identical answer. No DB unique index is needed as a backstop for *this* write (the source-of-truth `rating` row is itself protected by `uq_rating_once`). |
| Redis usage | `tutem:ops:idem:driver-service.rating-average-recompute.v1:<eventId>` (belt-and-suspenders; not load-bearing) |
| Database updates | `driver.rating_avg`, `driver.rating_count` — **driver-service owns `driver`.** Filters on `rateeType='DRIVER'` (payload field) so it acts only when the driver is the ratee. |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison record → `.dlq`. Redis down → no correctness impact (recompute is structurally idempotent). DB down → offset withheld. No external system. |

### 1.8 `driver-service.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Keep driver-service's in-process config view (thresholds, radii, driver-per-round counts) current within seconds of an admin change. |
| Consumer Group | `driver-service.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key` — matters for **current-value semantics** (an older value must never overwrite a newer one), guaranteed by the topic's compacted, per-key ordering. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`. Applying `{configKey, value, updatedAt}` as a keyed upsert into a local cache is naturally idempotent — overwriting with the same value twice is a no-op. |
| Redis usage | **Reads** `tutem:config:snapshot` (config-service is the **sole writer** of this key, §0.8) to warm its own in-process cache directly from the event payload; this consumer never writes that key. |
| Database updates | **None.** |
| Failure handling | Transient error → in-process retry → `.dlq`. Redis down → falls back to a direct `system_config` read via config-service's sync API until the snapshot rebuilds. DB down (config-service's `system_config`) → irrelevant to this consumer, which only reads the already-published event payload. |

---

## 2. identity-service consumers

### 2.1 `identity-service.rating-average-recompute.v1`

| Attribute | Detail |
|---|---|
| Purpose | Refresh `app_user.rating_avg`/`rating_count` whenever a rider is the ratee. |
| Consumer Group | `identity-service.rating-average-recompute.v1` |
| Topics | `tutem.trip.rating.submitted.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.trip.rating.submitted.v1.dlq` |
| Ordering | Per-`booking_id` — not required for this write (same reasoning as §1.7). |
| Idempotency | `rating:<ratingId>:submitted`. Full recompute (`AVG`/`COUNT` over `rating`), naturally idempotent — no DB unique index needed for *this* write; `uq_rating_once` protects the source row. |
| Redis usage | `tutem:ops:idem:identity-service.rating-average-recompute.v1:<eventId>` (belt-and-suspenders) |
| Database updates | `app_user.rating_avg`, `app_user.rating_count` — **identity-service owns `app_user`.** Filters on `rateeType='USER'`. |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → no correctness impact. DB down → offset withheld. |

### 2.2 `identity-service.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Same pattern as §1.8, for identity-service's own config-dependent behaviour (e.g. OTP rate-limit keys). |
| Consumer Group | `identity-service.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert, naturally idempotent. |
| Redis usage | Reads `tutem:config:snapshot` (config-service sole writer). |
| Database updates | None. |
| Failure handling | Same as §1.8. |

---

## 3. dispatch-service consumers

### 3.1 `dispatch-service.user-deletion-cleanup.v1`

| Attribute | Detail |
|---|---|
| Purpose | Cancel any live `SEARCHING` `service_request` for a rider who has just soft-deleted their account. |
| Consumer Group | `dispatch-service.user-deletion-cleanup.v1` |
| Topics | `tutem.identity.user.deleted.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.identity.user.deleted.v1.dlq` |
| Ordering | Per-`user_id` — not required for this consumer's write (one user, one idempotent conditional update). |
| Idempotency | `user:<userId>:deleted`. Write is `UPDATE service_request SET status='CANCELLED', closed_at=now() WHERE rider_id=:userId AND status='SEARCHING'` — the `WHERE status='SEARCHING'` clause makes a duplicate delivery a safe no-op (a second attempt matches zero rows). |
| Redis usage | `tutem:ops:idem:dispatch-service.user-deletion-cleanup.v1:<eventId>` (belt-and-suspenders) |
| Database updates | `service_request.status`, `closed_at` — **dispatch-service owns `service_request`.** Also produces `ServiceRequestCancelled` via its own outbox in the same transaction. |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → no impact (conditional `WHERE` is the real backstop). DB down → offset withheld. |

### 3.2 `dispatch-service.carpool-matching.v1`

| Attribute | Detail |
|---|---|
| Purpose | Run the carpool corridor-matching computation (§4.12-B) against a newly-created carpool offer (`TripCreated`), and notify matched riders directly. |
| Consumer Group | `dispatch-service.carpool-matching.v1` |
| Topics | `tutem.trip.trip.created.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Not qualifying: the Routing API call in this design is synchronous inside the API request path (F-10 step 2, `[SYNC-CRITICAL]`), not made from this Kafka consumer — this consumer only reads the already-fetched `route_line` via trip-service's internal API and runs `ST_DWithin`/`ST_LineLocatePoint` over `service_request` rows it already owns. |
| DLQ | `tutem.trip.trip.created.v1.dlq` |
| Ordering | Per-`trip_id` — not required for this consumer's own logic (one offer, one matching run), though it inherits it from the topic's own guarantee. |
| Idempotency | `trip:<tripId>:created`. This consumer performs a **read-only spatial query plus a fan-out**, not a write to any table it owns — re-running the match on redelivery recomputes the same candidate set (idempotent by construction) and re-issues `SendPushCommand`s, deduplicated downstream at notification-service via `tutem:notification:sent:<idempotencyKey>`. |
| Redis usage | `tutem:dispatch:route-cache:<coordHash>` (read, if a cached corridor lookup applies), `tutem:ops:idem:dispatch-service.carpool-matching.v1:<eventId>` |
| Database updates | **None directly** — matching is a read over `service_request` (own table) plus an internal-API read of `trip.route_line` (trip-service owns `trip`; per §2.0/Q-20, dispatch-service never reads `trip` columns directly). **This internal endpoint is not yet named/shaped — tracked as an open gap in §12 item 4**, the same class of issue as §1.5's blacklist-evaluation hop. Emits `SendPushCommand` to `tutem.notification.send-push.command.v1` via dispatch-service's own outbox — this *is* dispatch-service's Kafka write, not a DB write. |
| Failure handling | Transient DB/API error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → route-cache miss falls back to the internal API read (slower, not incorrect). Internal API to trip-service down → transient failure, in-process retry, `.dlq` on exhaustion — a real cross-service coupling worth noting for Step 9, same shape as §1.5's dispatch dependency. |

### 3.3 `dispatch-service.request-status-sync.v1`

| Attribute | Detail |
|---|---|
| Purpose | Apply `service_request.status` transitions (`ONGOING`, `COMPLETED`, `CANCELLED`, and the CARPOOL `MATCHED` hop) driven by trip/booking lifecycle facts that trip-service owns and dispatch-service must mirror. |
| Consumer Group | `dispatch-service.request-status-sync.v1` |
| Topics | `tutem.trip.trip.started.v1`, `tutem.trip.trip.completed.v1`, `tutem.trip.trip.cancelled.v1`, `tutem.trip.booking.confirmed.v1`, `tutem.trip.booking.onboard.v1`, `tutem.trip.booking.completed.v1`, `tutem.trip.booking.cancelled.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 7 topics. Postgres-only failure surface. |
| DLQ | One per source topic: `tutem.trip.trip.started.v1.dlq`, `tutem.trip.trip.completed.v1.dlq`, `tutem.trip.trip.cancelled.v1.dlq`, `tutem.trip.booking.confirmed.v1.dlq`, `tutem.trip.booking.onboard.v1.dlq`, `tutem.trip.booking.completed.v1.dlq`, `tutem.trip.booking.cancelled.v1.dlq` |
| Ordering | Per-`trip_id` on every subscribed topic — **matters**: e.g. `BookingConfirmed` must be seen and applied before a later `BookingCancelled` for the same booking's seat accounting to make sense downstream. |
| Idempotency | `trip:<tripId>:started`/`:completed`/`:cancelled`, `booking:<bookingId>:confirmed`/`:onboard`/`:completed`/`:cancelled`. Every write is a **conditional `UPDATE service_request SET status='X' WHERE status='Y'`** — e.g. `SET status='ONGOING' WHERE status='MATCHED'` — which is naturally idempotent: a redelivered fact that has already been applied matches zero rows on the second attempt. This is the same shape as `dispatch-service.user-deletion-cleanup.v1`. RIDE/WALK's `BookingConfirmed` hop is a documented no-op here (status is already `MATCHED` from the synchronous accept transaction), confirmed idempotent by the same `WHERE` clause. |
| Redis usage | `tutem:ops:idem:dispatch-service.request-status-sync.v1:<eventId>` (belt-and-suspenders) |
| Database updates | `service_request.status`, `closed_at` where terminal — **dispatch-service owns `service_request`.** |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → no impact, the conditional `WHERE` is the real guard. DB down → offset withheld across all 7 topics for this group; lag grows uniformly, alerts on `tutem_kafka_consumer_lag_records`. |

### 3.4 `dispatch-service.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Same pattern as §1.8, for dispatch-service's config-dependent behaviour (offer TTL, drivers-per-round, search radius). |
| Consumer Group | `dispatch-service.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert, naturally idempotent. |
| Redis usage | Reads `tutem:config:snapshot`. |
| Database updates | None. |
| Failure handling | Same as §1.8. |

---

## 4. trip-service consumers

### 4.1 `trip-service.rider-directory-cache.v1`

| Attribute | Detail |
|---|---|
| Purpose | Maintain a denormalised rider-display cache (name/rating-relevant fields) so trip-service's history/booking screens never need a synchronous call to identity-service per row (Q-18(b) pattern). |
| Consumer Group | `trip-service.rider-directory-cache.v1` |
| Topics | `tutem.identity.user.registered.v1`, `tutem.identity.user.profile-updated.v1`, `tutem.identity.user.deleted.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 3 topics. |
| DLQ | `tutem.identity.user.registered.v1.dlq`, `tutem.identity.user.profile-updated.v1.dlq`, `tutem.identity.user.deleted.v1.dlq` |
| Ordering | Per-`user_id` — not required per baseline §5.2 (order-independent by construction), but co-location still means a stale profile-updated can never race a newer one out of order within one user. |
| Idempotency | `user:<userId>:registered`/`:profile-updated:<date>`/`:deleted`. A keyed upsert (`HSET`) into the Redis cache row is naturally idempotent. |
| Redis usage | **`tutem:trip:rider-directory:<user_id>`** (hash, write) — **NEW key, added at Step 7 by D9 (user decision, resolves this document's earlier §12 item 1); not in the baseline §0.8 inventory, which predates it — Step 9/10 must reconcile the §0.8 table to include it.** Per D9: this is a **Kafka-fed Redis cache, not a Postgres table** — no new domain table is introduced; `outbox_event` remains the only new table in the architecture. |
| Database updates | **None.** This consumer writes only the Redis key above. **The cache is an optimisation, never a source of truth**: the authoritative rider-display data is identity-service's own `app_user` row. **Cold-start / cache-miss fallback:** any reader hitting a missing or expired `tutem:trip:rider-directory:<user_id>` key falls back to a **synchronous internal API call to identity-service** (its public-profile endpoint, §2.3 of the baseline) to fetch the same fields live, and may repopulate the cache from that response. A cold cache (fresh deploy, full Redis flush) degrades to per-request live calls until this consumer's replay/steady-state traffic repopulates it — a latency cost, never a correctness or data-loss event (consistent with baseline A-08). |
| Failure handling | Transient error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → this consumer's writes simply fail/retry; readers transparently fall back to the identity-service live call above, so a Redis outage degrades latency, not correctness. No external system. |

### 4.2 `trip-service.vehicle-snapshot-cache.v1`

| Attribute | Detail |
|---|---|
| Purpose | Maintain a denormalised vehicle-detail cache so a trip card can render vehicle category/seats without a synchronous driver-service call per view. |
| Consumer Group | `trip-service.vehicle-snapshot-cache.v1` |
| Topics | `tutem.driver.vehicle.registered.v1`, `tutem.driver.vehicle.deactivated.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.driver.vehicle.registered.v1.dlq`, `tutem.driver.vehicle.deactivated.v1.dlq` |
| Ordering | Per-`driver_id` — a driver's own vehicle changes should be applied in commit order; not correctness-critical for this specific cache (a slightly stale vehicle flag self-corrects on the next event). |
| Idempotency | `vehicle:<vehicleId>:registered`/`:deactivated`. Keyed upsert (`HSET`), naturally idempotent. |
| Redis usage | **`tutem:trip:vehicle-snapshot:<vehicle_id>`** (hash, write) — **NEW key, added at Step 7 by D9; not in the baseline §0.8 inventory, which predates it — Step 9/10 must reconcile.** Per D9 this resolves Q-21 (baseline §7) precisely as **both halves of that open question at once**: a Kafka-fed Redis cache (this consumer) *with* a live-fetch fallback on cache miss (below) — see also §12 item 2, now closed. |
| Database updates | **None.** This consumer writes only the Redis key above; the authoritative vehicle detail remains driver-service's own `vehicle` row. **Cold-start / cache-miss fallback:** a reader hitting a missing/expired `tutem:trip:vehicle-snapshot:<vehicle_id>` key falls back to a **synchronous internal API call to driver-service** (its vehicle-snapshot-for-a-trip endpoint, §2.4 of the baseline) and may repopulate the cache from the response. Cache is an optimisation, never a source of truth. |
| Failure handling | Transient error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → readers fall back to the driver-service live call above; latency cost only, never a correctness break. No external system from this consumer's own perspective (the fallback call is made by the *reader*, not by this consumer). |

### 4.3 `trip-service.trip-provisioning.v1`

| Attribute | Detail |
|---|---|
| Purpose | Create the `trip` + `booking` rows the instant an offer is won (RIDE/WALK) — the F-08 step 5 hop from dispatch's settled accept race to trip-service's own transactional creation. |
| Consumer Group | `trip-service.trip-provisioning.v1` |
| Topics | `tutem.dispatch.offer.accepted.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. Postgres-only. |
| DLQ | `tutem.dispatch.offer.accepted.v1.dlq` |
| Ordering | Per-`request_id` on the source topic — **matters**: this is the one event per request that provisions the trip; siblings' `RideOfferWithdrawn` on the same partition must not be misread as arriving "first". |
| Idempotency | `offer:<offerId>:accepted` (read from the envelope — **not derivable from the record key**, since the key is `request_id`, per §0.7). **DB unique indexes are the authoritative backstop**: `uq_trip_one_live_per_provider` and `uq_booking_request` both reject a duplicate creation attempt outright — a redelivered `RideOfferAccepted` is a safe no-op by constraint violation, caught and swallowed as "already provisioned". |
| Redis usage | `tutem:ops:idem:trip-service.trip-provisioning.v1:<eventId>` (pre-check to avoid even attempting the doomed-to-conflict insert; the DB constraint is what actually guarantees correctness) |
| Database updates | `trip` INSERT, `booking` INSERT, same transaction — **trip-service owns both `trip` and `booking`.** Also produces `TripCreated`/`BookingConfirmed` via its own outbox in that same transaction. |
| Failure handling | Transient DB error → in-process retry → `.dlq`. Poison → `.dlq`. Unique-constraint violation on a legitimate duplicate delivery → caught, logged as `skipped-duplicate`, offset committed — not routed to `.dlq` (this is the expected at-least-once path, not a failure). Redis down → the pre-check is skipped, the DB constraint alone still guarantees correctness (just one wasted insert attempt). DB down → offset withheld. |

### 4.4 `trip-service.history-projection.v1`

| Attribute | Detail |
|---|---|
| Purpose | Feed the Q-18(b) denormalised rider-history read model with `service_request` lifecycle facts (dispatch-service owns the source table; trip-service assembles the cross-context history view). |
| Consumer Group | `trip-service.history-projection.v1` |
| Topics | `tutem.dispatch.request.created.v1`, `tutem.dispatch.request.matched.v1`, `tutem.dispatch.request.expired.v1`, `tutem.dispatch.request.cancelled.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 4 topics. |
| DLQ | `tutem.dispatch.request.created.v1.dlq`, `tutem.dispatch.request.matched.v1.dlq`, `tutem.dispatch.request.expired.v1.dlq`, `tutem.dispatch.request.cancelled.v1.dlq` |
| Ordering | Per-`request_id` — **matters**: `created` must project before `matched`/`expired`/`cancelled` for the read model's status column to reflect a real state machine rather than a stale one. |
| Idempotency | `request:<requestId>:created`/`:matched`/`:expired`/`:cancelled`. Keyed upsert (`HSET`, per-`request_id` field within the rider's cache entry) is naturally idempotent. |
| Redis usage | **`tutem:trip:history-projection:<user_id>`** (hash/list of request summaries, write) — **NEW key, added at Step 7 by D9; not in the baseline §0.8 inventory, which predates it — Step 9/10 must reconcile.** Per D9: a Kafka-fed Redis cache, not a Postgres read-model table — the Q-18(b) CQRS store baseline §7 named is implemented here as a cache, not a new table. |
| Database updates | **None.** This consumer writes only the Redis key above. **Cold-start / cache-miss fallback:** a reader hitting a missing/expired `tutem:trip:history-projection:<user_id>` key falls back to a **synchronous read over trip-service's own already-authoritative tables** — `booking` by `rider_id` via `ix_booking_rider_history`, joined to `service_request` (via a sync call to dispatch-service, since dispatch-service owns `service_request`) and `trip` for detail, i.e. F-19's existing synchronous history-retrieval path (baseline §5, F-19). The cache is purely a latency optimisation over a query that is already correct and complete without it — **never the source of truth**; a full Redis flush loses no history. |
| Failure handling | Transient error → in-process retry → `.dlq`. Poison → `.dlq`. Redis down → all reads fall back to the F-19 synchronous path above; history views cost more per request but are never stale or wrong, because the fallback reads current DB state directly. |

### 4.5 `trip-service.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Same pattern as §1.8, for trip-service's config-dependent behaviour. |
| Consumer Group | `trip-service.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert. |
| Redis usage | Reads `tutem:config:snapshot`. |
| Database updates | None. |
| Failure handling | Same as §1.8. |

---

## 5. notification-service consumers

### 5.1 `notification-service.driver-welcome.v1`

| Attribute | Detail |
|---|---|
| Purpose | Send the welcome push the instant a new `driver` row is created (F-02 step 3) — the one place notification-service subscribes to a raw domain-fact topic rather than only the command topic. |
| Consumer Group | `notification-service.driver-welcome.v1` |
| Topics | `tutem.driver.driver.created.v1` |
| Retry Strategy | **Qualifies for retry tiers (D8)** — failure surface is FCM. `.retry-1` (30s) → `.retry-2` (5min) → `.retry-3` (30min) → `.dlq`. |
| DLQ | `tutem.driver.driver.created.v1.dlq` — lands: welcome pushes that failed FCM delivery on all 3 attempts. Low-priority redrive (a missed welcome push has no downstream correctness consequence). |
| Ordering | Per-`driver_id` — irrelevant here (one driver, one welcome push, no sequencing concern). |
| Idempotency | `driver:<driverId>:created`. Dedup via `tutem:notification:sent:<idempotencyKey>` (Redis, TTL) — **Redis-only, no DB backstop, but explicitly low-severity by design**: notification-service owns no tables, and a duplicate welcome push is, per baseline §3, "a UX nuisance, not a correctness bug." |
| Redis usage | `tutem:notification:tokens:<userId>` (read, FCM device tokens), `tutem:notification:sent:<idempotencyKey>` |
| Database updates | **None** — notification-service owns no tables. |
| Failure handling | FCM timeout/5xx → retry-tier hop, kept off the poll loop (§3). Poison record → `.dlq` directly. Redis down → `tutem:notification:tokens` miss means the push cannot be addressed (fails, retried); `tutem:notification:sent` miss means a possible duplicate send, accepted risk. FCM down for an extended period → all 3 tiers exhaust, `.dlq`. |

### 5.2 `notification-service.push-delivery.v1`

| Attribute | Detail |
|---|---|
| Purpose | The **sole executing consumer** of the command topic — renders and sends every FCM push in the system from the 12 producer call-sites enumerated in `03-kafka-events.md` §6.1. |
| Consumer Group | `notification-service.push-delivery.v1` |
| Topics | `tutem.notification.send-push.command.v1` |
| Retry Strategy | **Qualifies for retry tiers (D8)** — FCM is the failure surface for every message on this topic. `.retry-1` (30s) → `.retry-2` (5min) → `.retry-3` (30min) → `.dlq`. |
| DLQ | `tutem.notification.send-push.command.v1.dlq` — lands: any push that exhausted 3 FCM retry attempts, from any of the 3 producing services. `x-consumer-group` on the DLQ record is always `notification-service.push-delivery.v1` regardless of which service produced the command (§0.3's rule: failing *consumer*, not producer, names the DLQ context). |
| Ordering | Per-`user_id` — **matters in one narrow sense**: a stale "offer gone" notice must never be rendered before "offer created" for the same user. Guaranteed by the *producing* service publishing both facts from one ordered outbox sequence on its own topic first, with this command derived from that same sequence (§6.1 of Step 5). |
| Idempotency | `notify:<userId>:<templateCode>:<eventId>` — keyed off the **causing** domain event's `eventId`, not a `SendPushCommand`-specific id (this topic has no aggregate slot). Dedup via `tutem:notification:sent:<idempotencyKey>` — **Redis-only, no DB backstop; explicitly accepted per baseline §3** ("commands are naturally idempotent to execute... de-duplicated by `tutem:notification:sent:<idempotencyKey>` regardless"). |
| Redis usage | `tutem:notification:tokens:<userId>` (read), `tutem:notification:sent:<idempotencyKey>`, `tutem:ops:ratelimit:scope:subject` (push-send quota, shared inventory entry) |
| Database updates | **None** — notification-service owns no tables. |
| Failure handling | FCM timeout/5xx → retry-tier hop, offloaded to a bounded async worker pool (§3) so the poll loop is never blocked. Poison record (unrecognized `templateCode`) → `.dlq` directly. Redis down → tokens unavailable (send fails, retried) or dedup unavailable (possible duplicate, accepted). FCM outage → all 3 tiers exhaust → `.dlq`, which at this topic's volume is the single most actionable DLQ-depth metric in the system (`tutem_kafka_dlq_published_total{topic="tutem.notification.send-push.command.v1"}`). |

### 5.3 `notification-service.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Same pattern as §1.8, for notification-service's own config (e.g. push rate limits). |
| Consumer Group | `notification-service.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert. |
| Redis usage | Reads `tutem:config:snapshot`. |
| Database updates | None. |
| Failure handling | Same as §1.8. |

---

## 6. api-gateway consumer

### 6.1 `api-gateway.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Keep api-gateway's own quota/config view (rate-limit thresholds) current. |
| Consumer Group | `api-gateway.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert. |
| Redis usage | Reads `tutem:config:snapshot`; own quota keys are `tutem:ops:ratelimit:<scope>:<subject>` (unaffected by this consumer directly). |
| Database updates | **None** — api-gateway owns no tables. |
| Failure handling | Same as §1.8. |

---

## 7. realtime-gateway consumers

All seven of these consumers share one shape: **no DB writes, no external calls** — they translate a
consumed Kafka fact into a WebSocket push, routed to the right socket-holding instance via
`tutem:ops:ws-route:<userId>` and `tutem:ops:fanout:<tripId>` (§10 of Step 3). A duplicate delivery
produces a duplicate WebSocket frame, which the Flutter client discards by `eventId` (D3) — the same
tolerance FCM has, but even cheaper since there is no external system involved.

### 7.1 `realtime-gateway.driver-presence-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Push a driver's online/offline transition to any client watching that driver (rare directly; mostly feeds derived UI state). |
| Consumer Group | `realtime-gateway.driver-presence-fanout.v1` |
| Topics | `tutem.driver.driver.went-online.v1`, `tutem.driver.driver.went-offline.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. No external call — WebSocket push to an owned socket is a local operation. |
| DLQ | `tutem.driver.driver.went-online.v1.dlq`, `tutem.driver.driver.went-offline.v1.dlq` |
| Ordering | Per-`driver_id`, last-write-wins — matters for presence UI consistency. |
| Idempotency | `driver:<driverId>:went-online:<occurredAt-iso>` / `:went-offline:<occurredAt-iso>`. A duplicate push is harmless (client de-dups by `eventId`); `tutem:ops:idem:realtime-gateway.driver-presence-fanout.v1:<eventId>` is applied uniformly per A-09 but is not load-bearing for correctness. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.driver-presence-fanout.v1:<eventId>` |
| Database updates | **None** — realtime-gateway owns no tables. |
| Failure handling | Socket not found locally → hand off via `tutem:ops:fanout:<tripId>` pub/sub to the owning instance. Redis down → cross-instance handoff fails; only sockets held by the consuming instance itself still receive the push (degraded fan-out, not a crash). No DB, no external system. |

### 7.2 `realtime-gateway.driver-location-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Live-track a driver's position to any rider currently watching that trip/driver. |
| Consumer Group | `realtime-gateway.driver-location-fanout.v1` |
| Topics | `tutem.driver.driver.location-updated.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.driver.driver.location-updated.v1.dlq` |
| Ordering | Per-`driver_id`, last-write-wins by `occurredAt` — **matters**, an out-of-order push would visibly jump the driver's position backwards on the rider's map. |
| Idempotency | `driver:<driverId>:location-updated:<occurredAt-iso>`. A duplicate push renders the same position twice, harmless; a **stale** push (older `occurredAt` arriving late) is discarded client-side by comparing timestamps, not by `eventId` alone. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.driver-location-fanout.v1:<eventId>` |
| Database updates | None. |
| Failure handling | Same shape as §7.1. This is the highest-throughput consumer in realtime-gateway (F-06 dominates cluster volume, §2.1 of Step 3) — a lagging instance here is the leading indicator watched via `tutem_kafka_consumer_lag_records`. |

### 7.3 `realtime-gateway.offer-countdown-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Push the live offer-countdown/outcome UI (created, accepted, expired, withdrawn) and the request-expired notice. |
| Consumer Group | `realtime-gateway.offer-countdown-fanout.v1` |
| Topics | `tutem.dispatch.offer.created.v1`, `tutem.dispatch.offer.accepted.v1`, `tutem.dispatch.offer.expired.v1`, `tutem.dispatch.offer.withdrawn.v1`, `tutem.dispatch.request.expired.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 5 topics. |
| DLQ | `tutem.dispatch.offer.created.v1.dlq`, `tutem.dispatch.offer.accepted.v1.dlq`, `tutem.dispatch.offer.expired.v1.dlq`, `tutem.dispatch.offer.withdrawn.v1.dlq`, `tutem.dispatch.request.expired.v1.dlq` |
| Ordering | Per-`request_id` on all 5 topics — **matters**: the countdown UI must see `created` before `accepted`/`expired`/`withdrawn` for the same request, exactly the sibling-ordering property §5.3 of Step 3 exists for. |
| Idempotency | `offer:<offerId>:created`/`:accepted`/`:expired`/`:withdrawn`, `request:<requestId>:expired`. Duplicate push harmless; client `eventId` de-dup is the real guard. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.offer-countdown-fanout.v1:<eventId>` |
| Database updates | None. |
| Failure handling | Same shape as §7.1, across 5 topics; lag is tracked per-topic under one consumer group. |

### 7.4 `realtime-gateway.trip-status-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Push trip lifecycle transitions and the two OTP-verified booking events (`onboard`, `completed`) to whoever is watching that trip. |
| Consumer Group | `realtime-gateway.trip-status-fanout.v1` |
| Topics | `tutem.trip.trip.created.v1`, `tutem.trip.trip.started.v1`, `tutem.trip.trip.completed.v1`, `tutem.trip.trip.cancelled.v1`, `tutem.trip.booking.onboard.v1`, `tutem.trip.booking.completed.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 6 topics. |
| DLQ | One per source topic, matching names above with `.dlq` appended. |
| Ordering | Per-`trip_id` — matters: a trip's status must be seen to progress `created → started → completed/cancelled` in order for the UI to render a coherent journey. |
| Idempotency | `trip:<tripId>:created`/`:started`/`:completed`/`:cancelled`, `booking:<bookingId>:onboard`/`:completed`. Duplicate push harmless. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.trip-status-fanout.v1:<eventId>` |
| Database updates | None. |
| Failure handling | Same shape as §7.1, across 6 topics. |

### 7.5 `realtime-gateway.carpool-seat-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Push carpool seat-count changes (including the derived "full"/"not full" transition) to the driver and every rider watching that trip. |
| Consumer Group | `realtime-gateway.carpool-seat-fanout.v1` |
| Topics | `tutem.trip.trip.seats-exhausted.v1`, `tutem.trip.booking.confirmed.v1`, `tutem.trip.booking.cancelled.v1`, `tutem.trip.booking.no-show.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 4 topics. |
| DLQ | `tutem.trip.trip.seats-exhausted.v1.dlq`, `tutem.trip.booking.confirmed.v1.dlq`, `tutem.trip.booking.cancelled.v1.dlq`, `tutem.trip.booking.no-show.v1.dlq` |
| Ordering | Per-`trip_id` — matters: a seat freed-then-refilled sequence must render in commit order, or the pushed seat count is simply wrong at a point in time (though self-correcting on the next event). |
| Idempotency | `trip:<tripId>:seats-exhausted:<seatsBooked>`, `booking:<bookingId>:confirmed`/`:cancelled`/`:no-show`. Duplicate push harmless. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.carpool-seat-fanout.v1:<eventId>` |
| Database updates | None. |
| Failure handling | Same shape as §7.1. **Note:** `BookingConfirmed`'s `pickupOrder` field is explicitly a **hint, not a key** (baseline §16.4 item 3, §10 item 3 unapproved) — this consumer propagates that field as-is to the client and must not treat it as unique; two riders can legitimately arrive with the same value. Not a consumer-idempotency bug, but a related unbackstopped invariant this fan-out surfaces (see §6 below). |

### 7.6 `realtime-gateway.payment-status-fanout.v1`

| Attribute | Detail |
|---|---|
| Purpose | Push payment status changes (`paid`/`failed`/`refunded`) to the payer. |
| Consumer Group | `realtime-gateway.payment-status-fanout.v1` |
| Topics | `tutem.trip.booking.paid.v1`, `tutem.trip.booking.payment-failed.v1`, `tutem.trip.booking.refunded.v1` |
| Retry Strategy | In-process bounded backoff → `.dlq` for all 3 topics. |
| DLQ | `tutem.trip.booking.paid.v1.dlq`, `tutem.trip.booking.payment-failed.v1.dlq`, `tutem.trip.booking.refunded.v1.dlq` |
| Ordering | Per-`trip_id` — matters: `paid` must not be shown after a later `refunded` for the same booking due to out-of-order delivery. |
| Idempotency | `booking:<bookingId>:paid`/`:payment-failed:<occurredAt-iso>`/`:refunded`. Duplicate push harmless. |
| Redis usage | `tutem:ops:ws-route:<userId>`, `tutem:ops:fanout:<tripId>`, `tutem:ops:idem:realtime-gateway.payment-status-fanout.v1:<eventId>` |
| Database updates | None. |
| Failure handling | Same shape as §7.1. The actual payment-gateway calls this data originates from (F-16) are synchronous, inside trip-service's own request path or its own webhook handler — never made by this consumer, which only relays an already-settled fact. |

### 7.7 `realtime-gateway.config-snapshot-refresh.v1`

| Attribute | Detail |
|---|---|
| Purpose | Same pattern as §1.8, for realtime-gateway's own config (e.g. WebSocket heartbeat/session TTLs, if config-driven). |
| Consumer Group | `realtime-gateway.config-snapshot-refresh.v1` |
| Topics | `tutem.config.config.changed.v1` (compacted) |
| Retry Strategy | In-process bounded backoff → `.dlq`. |
| DLQ | `tutem.config.config.changed.v1.dlq` |
| Ordering | Per-`config_key`, current-value semantics. |
| Idempotency | `config:<configKey>:changed:<updatedAt-iso>`; keyed upsert. |
| Redis usage | Reads `tutem:config:snapshot`. |
| Database updates | None. |
| Failure handling | Same as §1.8. |

---

## 8. Concurrency table

Partition counts are the ceiling on useful concurrency (§6 of Step 3: instances ≤ partitions, or the excess
instance gets no partitions and idles). `max.poll.interval.ms` is set generously only for the 3 retry-tier
consumers, per Step 3 §6's explicit rule; every pure-DB/Redis/WebSocket consumer keeps it tight so a
genuinely stuck consumer is rebalanced away promptly.

| Consumer group | Partitions (source topic, max if multiple) | Recommended concurrency | `max.poll.records` | `max.poll.interval.ms` | Offset commit | Why |
|---|---|---|---|---|---|---|
| `driver-service.geo-index-maintenance.v1` | 24 | 12–24 instances/threads | 200 | 60,000 | Manual, after DB+Redis write | Highest-volume topic (F-06, §2.1); wide concurrency absorbs 1,250 msg/s with headroom; fast synchronous DB/Redis write keeps poll interval tight. |
| `driver-service.parivahan-verification.v1` | 6 | 6 poll threads; async Parivahan-call worker pool sized independently (e.g. 30) | 10 | 300,000 | Manual, after Parivahan outcome persisted | Low volume (one per document); small `max.poll.records` bounds in-flight async calls; 5-minute interval accommodates the offloaded external call (§9 below). |
| `driver-service.verification-status-recompute.v1` | 6 | 4–6 | 100 | 60,000 | Manual, after recompute UPDATE | DB-only, fast; tight interval. |
| `driver-service.blacklist-geo-sync.v1` | 6 | 3–6 | 200 | 60,000 | Manual, after Redis op | Redis-only, fast. |
| `driver-service.blacklist-evaluation.v1` | 12 | 6–12 | 100 | 60,000 | Manual, after insert attempt | Includes one internal-API hop to dispatch-service (§1.5) but that call is expected sub-100ms; kept in the tight tier, revisit if p99 says otherwise. |
| `driver-service.total-trips-increment.v1` | 12 | 6–12 | 200 | 60,000 | Manual, after UPDATE + Redis dedup write | DB-only, fast; flagged idempotency risk (§6) is orthogonal to concurrency tuning. |
| `driver-service.rating-average-recompute.v1` | 6 | 3–6 | 100 | 60,000 | Manual, after recompute UPDATE | DB-only, fast. |
| `driver-service.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual, after local cache upsert | Tiny, low-volume compacted topic. |
| `identity-service.rating-average-recompute.v1` | 6 | 3–6 | 100 | 60,000 | Manual | Same shape as driver-service's twin. |
| `identity-service.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |
| `dispatch-service.user-deletion-cleanup.v1` | 6 | 2–4 | 100 | 60,000 | Manual | Low volume (account deletions are rare). |
| `dispatch-service.carpool-matching.v1` | 12 | 6–12 | 50 | 60,000 | Manual, after fan-out published | Spatial query + internal API read is heavier per record than a plain UPDATE; smaller `max.poll.records` bounds per-batch latency. |
| `dispatch-service.request-status-sync.v1` | 12 | 8–12 | 200 | 60,000 | Manual | Handles 7 topics' worth of volume in one group; sized toward the top of the trip/booking class. |
| `dispatch-service.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |
| `trip-service.rider-directory-cache.v1` | 6 | 3–6 | 200 | 60,000 | Manual | Upsert-only, fast. |
| `trip-service.vehicle-snapshot-cache.v1` | 6 | 2–4 | 200 | 60,000 | Manual | Low volume (vehicle changes are rare). |
| `trip-service.trip-provisioning.v1` | 12 | 8–12 | 50 | 60,000 | Manual, after trip+booking commit | Every accepted offer becomes a trip; correctness-critical write, moderate batch size. |
| `trip-service.history-projection.v1` | 12 | 6–12 | 200 | 60,000 | Manual | Upsert-only read-model maintenance. |
| `trip-service.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |
| `notification-service.driver-welcome.v1` | 6 | 6 poll threads; async FCM-call worker pool sized independently (e.g. 20) | 10 | 300,000 | Manual, after FCM outcome known | Retry-tier consumer (§9). |
| `notification-service.push-delivery.v1` | 12 | 12 poll threads; async FCM-call worker pool sized independently (e.g. 50–100, this is the system's highest-fan-out push path) | 20 | 300,000 | Manual, after FCM outcome known | Retry-tier consumer, highest push volume in the system (§9). |
| `notification-service.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |
| `api-gateway.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |
| `realtime-gateway.driver-presence-fanout.v1` | 24 | 8–16 (bounded by realtime-gateway's own socket-count scaling, not just partitions, §10 of Step 3) | 200 | 60,000 | Manual, after push dispatched/handed off | WebSocket push is local and fast; concurrency also constrained by how many pods hold relevant sockets. |
| `realtime-gateway.driver-location-fanout.v1` | 24 | 12–24 | 300 | 60,000 | Manual | Highest-volume realtime-gateway consumer; wide concurrency. |
| `realtime-gateway.offer-countdown-fanout.v1` | 12 | 6–12 | 200 | 60,000 | Manual | Bursty per dispatch round; moderate concurrency. |
| `realtime-gateway.trip-status-fanout.v1` | 12 | 6–12 | 200 | 60,000 | Manual | Moderate volume. |
| `realtime-gateway.carpool-seat-fanout.v1` | 12 | 4–8 | 200 | 60,000 | Manual | Lower volume than ride/walk-dominated topics. |
| `realtime-gateway.payment-status-fanout.v1` | 12 | 4–8 | 200 | 60,000 | Manual | One per completed/paid booking; moderate. |
| `realtime-gateway.config-snapshot-refresh.v1` | 3 | 1–3 | 10 | 60,000 | Manual | As above. |

---

## 9. Keeping slow external calls off the poll loop

Exactly 3 consumer groups touch an external system whose latency the service does not control:
`driver-service.parivahan-verification.v1` (Parivahan SDK), `notification-service.driver-welcome.v1` and
`notification-service.push-delivery.v1` (FCM). For each:

1. **The poll loop's only job is to pull the record, deserialize it, and hand it to a bounded async
   worker-pool/executor** — it never invokes Parivahan/FCM synchronously inline. This is the mandatory rule
   from Step 3 §6, restated here as the concrete mechanism.
2. **The Kafka offset is committed (manual ack) only after the async call's outcome — success, retryable
   failure, or terminal failure — is known and persisted**: for Parivahan, once `driver_document.status`
   is written; for FCM, once the send result is known and `tutem:notification:sent:<idempotencyKey>` is
   set. A crash between dispatch-to-worker and outcome-persistence simply means the record is redelivered
   on restart — safe, because the DB/Redis write is what makes the effect idempotent, not the offset commit.
3. **`max.poll.interval.ms` is set generously (300,000ms / 5 minutes)** for these 3 groups specifically, so
   Kafka's consumer-group rebalance protocol does not treat "waiting on a slow external call" as a stuck
   consumer. `session.timeout.ms`/`heartbeat.interval.ms` are tuned independently (Kafka's separate
   heartbeat thread keeps the group membership alive even while a poll-loop thread waits on the async
   result) — a genuinely hung worker pool (not just a slow external call) is still detected and rebalanced.
4. **The worker-pool size is decoupled from partition/poll concurrency** — e.g. `push-delivery.v1` runs 12
   poll threads (matching its 12 partitions) but a 50–100-slot async FCM-call executor, because FCM calls
   are typically much faster than the poll-thread count would suggest is needed, and backpressure on the
   executor (not on Kafka) is what should absorb an FCM slowdown.
5. **No other consumer group in this catalogue makes an external call** — the payment gateway and Routing
   API are both called synchronously inside the originating REST request (F-16 steps 1–3, F-07 step 2/F-10
   step 2), never from a Kafka consumer, so no further groups need this treatment (§3.1 of `02-kafka-topics.md`).

---

## 10. Summary matrix — topic → consumer groups → services

| Topic | Consumer group(s) | Consuming service(s) |
|---|---|---|
| `tutem.identity.user.registered.v1` | `trip-service.rider-directory-cache.v1` | trip-service |
| `tutem.identity.user.profile-updated.v1` | `trip-service.rider-directory-cache.v1` | trip-service |
| `tutem.identity.user.deleted.v1` | `dispatch-service.user-deletion-cleanup.v1`, `trip-service.rider-directory-cache.v1` | dispatch-service, trip-service |
| `tutem.driver.driver.created.v1` | `notification-service.driver-welcome.v1` | notification-service |
| `tutem.driver.driver.went-online.v1` | `realtime-gateway.driver-presence-fanout.v1` | realtime-gateway |
| `tutem.driver.driver.went-offline.v1` | `realtime-gateway.driver-presence-fanout.v1` | realtime-gateway |
| `tutem.driver.driver.location-updated.v1` | `driver-service.geo-index-maintenance.v1`, `realtime-gateway.driver-location-fanout.v1` | driver-service, realtime-gateway |
| `tutem.driver.vehicle.registered.v1` | `trip-service.vehicle-snapshot-cache.v1` | trip-service |
| `tutem.driver.vehicle.deactivated.v1` | `trip-service.vehicle-snapshot-cache.v1` | trip-service |
| `tutem.driver.document.submitted.v1` | `driver-service.parivahan-verification.v1` | driver-service |
| `tutem.driver.document.verified.v1` | `driver-service.verification-status-recompute.v1` | driver-service |
| `tutem.driver.document.rejected.v1` | `driver-service.verification-status-recompute.v1` | driver-service |
| `tutem.driver.blacklist.blacklist-applied.v1` | `driver-service.blacklist-geo-sync.v1` | driver-service |
| `tutem.driver.blacklist.blacklist-expired.v1` | `driver-service.blacklist-geo-sync.v1` | driver-service |
| `tutem.dispatch.offer.created.v1` | `realtime-gateway.offer-countdown-fanout.v1` | realtime-gateway |
| `tutem.dispatch.offer.accepted.v1` | `realtime-gateway.offer-countdown-fanout.v1`, `trip-service.trip-provisioning.v1` | realtime-gateway, trip-service |
| `tutem.dispatch.offer.rejected.v1` | `driver-service.blacklist-evaluation.v1` | driver-service |
| `tutem.dispatch.offer.expired.v1` | `driver-service.blacklist-evaluation.v1`, `realtime-gateway.offer-countdown-fanout.v1` | driver-service, realtime-gateway |
| `tutem.dispatch.offer.withdrawn.v1` | `realtime-gateway.offer-countdown-fanout.v1` | realtime-gateway |
| `tutem.dispatch.request.created.v1` | `trip-service.history-projection.v1` | trip-service |
| `tutem.dispatch.request.matched.v1` | `trip-service.history-projection.v1` | trip-service |
| `tutem.dispatch.request.expired.v1` | `trip-service.history-projection.v1`, `realtime-gateway.offer-countdown-fanout.v1` | trip-service, realtime-gateway |
| `tutem.dispatch.request.cancelled.v1` | `trip-service.history-projection.v1` | trip-service |
| `tutem.trip.trip.created.v1` | `dispatch-service.carpool-matching.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.trip.started.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.trip.completed.v1` | `dispatch-service.request-status-sync.v1`, `driver-service.total-trips-increment.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, driver-service, realtime-gateway |
| `tutem.trip.trip.cancelled.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.trip.seats-exhausted.v1` | `realtime-gateway.carpool-seat-fanout.v1` | realtime-gateway |
| `tutem.trip.booking.confirmed.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.carpool-seat-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.booking.onboard.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.booking.completed.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.trip-status-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.booking.cancelled.v1` | `dispatch-service.request-status-sync.v1`, `realtime-gateway.carpool-seat-fanout.v1` | dispatch-service, realtime-gateway |
| `tutem.trip.booking.no-show.v1` | `realtime-gateway.carpool-seat-fanout.v1` | realtime-gateway |
| `tutem.trip.booking.paid.v1` | `realtime-gateway.payment-status-fanout.v1` | realtime-gateway |
| `tutem.trip.booking.payment-failed.v1` | `realtime-gateway.payment-status-fanout.v1` | realtime-gateway |
| `tutem.trip.booking.refunded.v1` | `realtime-gateway.payment-status-fanout.v1` | realtime-gateway |
| `tutem.trip.rating.submitted.v1` | `identity-service.rating-average-recompute.v1`, `driver-service.rating-average-recompute.v1` | identity-service, driver-service |
| `tutem.config.config.changed.v1` | `<service>.config-snapshot-refresh.v1` × 7 | identity-service, driver-service, dispatch-service, trip-service, notification-service, api-gateway, realtime-gateway |
| `tutem.notification.send-push.command.v1` | `notification-service.push-delivery.v1` | notification-service |

**39/39 base topics have ≥1 consumer group** (F-19 needing zero topics is consistent with `02-kafka-topics.md`
§6, which is about topics, not consumers — every topic that exists has a consumer).

---

## 11. Redis-only idempotency — flagged for Step 9

Per baseline §0.7 ("the database wins over Redis") and the explicit instruction to flag every consumer whose
idempotency rests on Redis alone:

| Consumer group | Why it has no DB unique-index backstop | Severity |
|---|---|---|
| **`driver-service.total-trips-increment.v1`** | `driver.total_trips` is a literal counter increment (`+1`), not a recompute and not guarded by any unique index. Sole mitigation: `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` (Redis, TTL). A Redis loss concurrent with a redelivery double-counts with no detection. **This single consumer group covers both flagged risk items from the baseline (F-09 step 5 and F-11 step 6) — one topic (`TripCompleted`), one mechanism, one caveat, for both RIDE/WALK and CARPOOL.** | **Genuine correctness risk — carry to Step 9 as-is.** |
| `notification-service.driver-welcome.v1` | Dedup via `tutem:notification:sent:<idempotencyKey>` only; notification-service owns no tables so no DB backstop is even possible. | **Accepted by design** — a duplicate push is a UX nuisance, not a correctness bug (baseline §3). Not the same risk class as the counter above. |
| `notification-service.push-delivery.v1` | Same mechanism, same reasoning, at higher volume (every push in the system). | **Accepted by design**, same as above. |

**Related but distinct — not a consumer-idempotency issue, still a Step 9 input:** the carpool
`pickup_order` invariant (baseline §16.4 item 3) has **no database backstop at all** (§10 item 3 is
unapproved) at the point of *booking creation* in trip-service — a database-constraint gap, not a Kafka
redelivery problem. `realtime-gateway.carpool-seat-fanout.v1` (§7.5 above) merely propagates the resulting
value downstream as a documented "hint, not a key." It is listed here for completeness because it is the
one place a consumer visibly surfaces the gap, not because a consumer caused it.

---

## 12. Unresolved

Recorded per the ABSOLUTE NAMING RULE instruction — these are gaps Steps 4/5 left implicit that this step
had to complete without inventing a name, topic, or field:

1. **CLOSED by D9 (user decision).** Storage backing for `trip-service.rider-directory-cache.v1`,
   `trip-service.vehicle-snapshot-cache.v1` and `trip-service.history-projection.v1` is **Redis, not a new
   Postgres table**: `tutem:trip:rider-directory:<user_id>`, `tutem:trip:vehicle-snapshot:<vehicle_id>`,
   `tutem:trip:history-projection:<user_id>` (§4.1, §4.2, §4.4 above). All three are **new keys added at
   Step 7 by D9** and are not present in baseline §0.8's inventory, which predates them — **Step 9/10 must
   reconcile the §0.8 table to add these three.** `outbox_event` remains the only new table anywhere in
   this architecture; the 11-table domain schema is unchanged. Each cache has a documented cold-start/
   cache-miss fallback to a synchronous read of the owning service's authoritative data (§4.1/§4.2/§4.4) —
   the cache is an optimisation, never a source of truth, consistent with baseline A-08.
2. **CLOSED by D9.** Q-21's "fetch live + Redis cache" recommendation (baseline §7) and the Kafka-fed
   `vehicle-snapshot-cache.v1` consumer are no longer in tension: D9 makes the mechanism explicitly **a
   Kafka-fed Redis cache (`tutem:trip:vehicle-snapshot:<vehicle_id>`) with a live-fetch-to-driver-service
   fallback on cache miss** — which is what both halves of Q-21's options (a) and (c) were separately
   reaching for. See §4.2 above.
3. **OPEN — sharpened per gate finding.** Internal API shape for driver-service's blacklist-evaluation
   count (§1.5). No cross-schema SQL is performed: the count over `ride_offer` (dispatch-service's
   exclusive table, §2.0 of the baseline) is obtained via an internal read call. **Proposed endpoint,
   consistent with the existing `POST /internal/v1/drivers/nearby` convention (baseline §2.4) — not yet
   approved:**
   ```
   GET /internal/v1/offers/rejection-count?driverId={driverId}&since={CURRENT_DATE}
   ```
   returning `{ "driverId": "...", "count": <int>, "windowStart": "<date>" }`, computed by dispatch-service
   as `COUNT(*) FROM ride_offer WHERE driver_id=:driverId AND status IN ('REJECTED','EXPIRED') AND
   offered_at >= :since` — **both `REJECTED` and `EXPIRED` per D4**, never `REJECTED` alone. This endpoint
   name/shape is a proposal from this step, not an approved API contract; it needs sign-off before
   implementation, exactly like `/internal/v1/drivers/nearby` did.
4. **OPEN — added per gate finding G2.** `dispatch-service.carpool-matching.v1` (§3.2) reads `trip.route_line`
   via an internal-API hop to trip-service (per §2.0/Q-20: dispatch-service never reads `trip` columns
   directly) — the same class of unnamed-internal-API gap as item 3 above, but for the carpool corridor
   match rather than blacklist evaluation. No concrete endpoint name/shape has been proposed for this hop in
   any of Steps 2–5 or in this document; it is tracked here for consistency with item 3 and needs the same
   treatment (a named, sign-off-pending internal endpoint) before implementation.

---

**End of Step 7.** Awaiting Orchestrator approval before Step 8 (producers) is finalized as a separate
deliverable.
