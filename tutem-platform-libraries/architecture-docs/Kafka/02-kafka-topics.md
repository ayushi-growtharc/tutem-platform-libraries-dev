# Tutem — Kafka Topic Catalogue (Step 4)

> **Status:** APPROVED, with a surgical D8 amendment applied in place (2026-07-28) — see §3. No topic,
> event, service or consumer-group name changed; only the retry-topic policy and the totals it feeds (§7)
> were revised. §0–§2, §4–§6, §8–§9 are unaffected by D8.
> **Input:** [00-architecture-baseline.md](00-architecture-baseline.md) (APPROVED) — §0 naming grammar,
> §0.2.1 aggregate registry, §5 the 20 flows; [01-kafka-architecture.md](01-kafka-architecture.md)
> (APPROVED) — §5 partition classes, §8 schema/compat, §11 PII rules, §13.1 retention classes.
> **Scope:** the complete topic inventory. No payload fields, no event classes beyond their topic
> identity — that is Step 5 ([03-kafka-events.md](03-kafka-events.md)).
> **Immutable:** every name below is derived mechanically from the two grammars in baseline §0.2. Nothing
> is renamed; nothing is invented beyond what a flow step in baseline §5 actually requires.

---

## 0. How this catalogue was derived

Every topic below traces to a specific flow step. Two small, declared extensions beyond the baseline's
*illustrative* event-class examples (§0.4) were necessary because a real flow step has no other event to
attach to — both follow the existing grammar exactly and are called out here, not smuggled in:

| Extension | Topic | Why | Flow evidence |
|---|---|---|---|
| `DriverCreated` | `tutem.driver.driver.created.v1` | F-02 step 3 explicitly says *"a 'driver profile created' fact is emitted"*; no existing `Driver`-prefixed class covers row creation (only online/offline/location exist in §0.4's example list, which is not exhaustive). | F-02.3 |
| `BookingPaymentFailed`, `BookingRefunded` | `tutem.trip.booking.payment-failed.v1`, `tutem.trip.booking.refunded.v1` | F-16 steps 3–4 name `FAILED` and `REFUNDED` as distinct `payment_status` outcomes with their own fan-out needs; §0.4's example list only names `BookingPaid`. One event-name per topic (§0.2.2), so a status with a distinct meaning gets its own topic, exactly like `BookingConfirmed`/`BookingCancelled`/`BookingNoShow` already do. | F-16.3, F-16.4 |

One producer-cardinality extension to §0.2.3's command-topic list, required by **D3** (WebSocket **and**
FCM both mandatory for every user-facing alert, no exceptions): the cancellation notices of **F-17.1**
(rider notified, request cancelled pre-match), **F-17.5** (provider notified, re-dispatch) and **F-17.6**
(carpool riders notified of a whole-trip cancellation) are added to `tutem.notification.send-push.command.v1`'s
producer set (dispatch-service for F-17.1/F-17.5, trip-service for F-17.6), alongside the nine producer
call-sites baseline §0.2.3 already enumerates. Without this addition, F-17's cancellation notices would
reach a foregrounded client over WebSocket (realtime-gateway consuming the domain fact) but never reach a
backgrounded client over FCM — a silent D3 violation. This is flagged, not silently assumed.

**No topic was created that no flow needs.** F-19 (history retrieval) is pure reads over already-durable
tables and needs none (see §5 coverage matrix). F-06's `DriverLocationUpdated` is the one topic explicitly
justified as carrying raw coordinates (§11.1 PII table), per the accepted §2.1.1 trade-off in the Step 3
document.

---

## 1. Primary topic table — events and commands

Grammar: `tutem.<domain>.<aggregate>.<event-name>.v<major>` (event) / `tutem.<domain>.<action>.command.v<major>`
(command). Replication factor is **3** for every topic (baseline §2), `min.insync.replicas=2`. Ordering is
always per-partition-key only (§0.6).

| # | Topic | Domain | Aggregate | Kind | Producer | Consumers (group) | Partition key | Part. | Repl. | Retention/cleanup | Ordering | Flows | Purpose |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `tutem.identity.user.registered.v1` | identity | user | event | identity-service | trip-service (`trip-service.rider-directory-cache.v1`) | `user_id` | 6 | 3 | delete, 7d | not required (order-independent) | F-01 | New `app_user` row committed; feeds trip-service's denormalised rider-display cache (Q-18 pattern). |
| 2 | `tutem.identity.user.profile-updated.v1` | identity | user | event | identity-service | trip-service (`trip-service.rider-directory-cache.v1`) | `user_id` | 6 | 3 | delete, 7d | not required | F-01 | `full_name`/`email`/`gender` change, refreshes the same cache. |
| 3 | `tutem.identity.user.deleted.v1` | identity | user | event | identity-service | dispatch-service (`dispatch-service.user-deletion-cleanup.v1`), trip-service (`trip-service.rider-directory-cache.v1`) | `user_id` | 6 | 3 | delete, 7d | not required | F-01 | Soft-delete (`status='DELETED'`) fact; dispatch-service cancels any live `SEARCHING` request for that user. |
| 4 | `tutem.driver.driver.created.v1` | driver | driver | event | driver-service | notification-service (`notification-service.driver-welcome.v1`) | `driver_id` | 6 | 3 | delete, 7d | not required | F-02 | New `driver` row (`driver_kind`, `verification_status='PENDING'`). Triggers the welcome push (F-02.3). |
| 5 | `tutem.driver.driver.went-online.v1` | driver | driver | event | driver-service | realtime-gateway (`realtime-gateway.driver-presence-fanout.v1`) | `driver_id` | 24 | 3 | delete, 7d | per-driver, last-write-wins | F-04 | `is_online=TRUE` fact. |
| 6 | `tutem.driver.driver.went-offline.v1` | driver | driver | event | driver-service | realtime-gateway (`realtime-gateway.driver-presence-fanout.v1`) | `driver_id` | 24 | 3 | delete, 7d | per-driver, last-write-wins | F-05 | `is_online=FALSE` fact (explicit or scheduled-reconciliation). |
| 7 | `tutem.driver.driver.location-updated.v1` | driver | driver | event | driver-service | driver-service (`driver-service.geo-index-maintenance.v1`), realtime-gateway (`realtime-gateway.driver-location-fanout.v1`) | `driver_id` | 24 | 3 | delete, 24h | per-driver, last-write-wins by `occurredAt` | F-06 | Highest-volume topic; drives PostGIS write + Redis geo mirror + live rider tracking. **Carries PII (raw coordinate) — see §4.** |
| 8 | `tutem.driver.vehicle.registered.v1` | driver | vehicle | event | driver-service | trip-service (`trip-service.vehicle-snapshot-cache.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-02 | New `vehicle` row; feeds trip-service's Q-21 vehicle-snapshot cache. |
| 9 | `tutem.driver.vehicle.deactivated.v1` | driver | vehicle | event | driver-service | trip-service (`trip-service.vehicle-snapshot-cache.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-02 | Vehicle taken out of service; invalidates the same cache. |
| 10 | `tutem.driver.document.submitted.v1` | driver | document | event | driver-service | driver-service (`driver-service.parivahan-verification.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-03 | Decouples the upload API response from the slow Parivahan call (self-consumed by driver-service's own async worker pool). |
| 11 | `tutem.driver.document.verified.v1` | driver | document | event | driver-service | driver-service (`driver-service.verification-status-recompute.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-03 | Parivahan outcome; recomputes aggregate `driver.verification_status` across all required documents. |
| 12 | `tutem.driver.document.rejected.v1` | driver | document | event | driver-service | driver-service (`driver-service.verification-status-recompute.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-03 | Parivahan outcome; feeds the same recompute (a rejection cannot itself verify, but is tracked). |
| 13 | `tutem.driver.blacklist.blacklist-applied.v1` | driver | blacklist | event | driver-service | driver-service (`driver-service.blacklist-geo-sync.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-13, F-14 | New `driver_blacklist` row + `blacklisted_until` set; evicts the driver from `tutem:driver:geo:<active_mode>`. |
| 14 | `tutem.driver.blacklist.blacklist-expired.v1` | driver | blacklist | event | driver-service | driver-service (`driver-service.blacklist-geo-sync.v1`) | `driver_id` | 6 | 3 | delete, 7d | per-driver | F-15 | Bar lifted; driver eligible to re-join the geo set on next go-online. |
| 15 | `tutem.dispatch.offer.created.v1` | dispatch | offer | event | dispatch-service | realtime-gateway (`realtime-gateway.offer-countdown-fanout.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** — all offers of one request co-located | F-07, F-12 | One `ride_offer` row per candidate driver (`RIDE`/`WALK` only, D7). |
| 16 | `tutem.dispatch.offer.accepted.v1` | dispatch | offer | event | dispatch-service | realtime-gateway (`realtime-gateway.offer-countdown-fanout.v1`), trip-service (`trip-service.trip-provisioning.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-08, F-12 | The settled accept race (already decided by `uq_offer_single_accept`, §4.6 — Kafka only broadcasts it). Triggers `trip`+`booking` creation. |
| 17 | `tutem.dispatch.offer.rejected.v1` | dispatch | offer | event | dispatch-service | driver-service (`driver-service.blacklist-evaluation.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-13 | Driver declined. Evidence-path 1 of 2 for D4. |
| 18 | `tutem.dispatch.offer.expired.v1` | dispatch | offer | event | dispatch-service | driver-service (`driver-service.blacklist-evaluation.v1`), realtime-gateway (`realtime-gateway.offer-countdown-fanout.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-14 | `SENT → EXPIRED` sweep. Evidence-path 2 of 2 for D4 — the timer-driven trigger. |
| 19 | `tutem.dispatch.offer.withdrawn.v1` | dispatch | offer | event | dispatch-service | realtime-gateway (`realtime-gateway.offer-countdown-fanout.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-08, F-12, F-17 | Sibling offers lost the race, or the request was cancelled pre-match. Never blacklist evidence (D4). |
| 20 | `tutem.dispatch.request.created.v1` | dispatch | request | event | dispatch-service | trip-service (`trip-service.history-projection.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-07, F-12 | Rider demand row inserted. Feeds trip-service's Q-18(b) denormalised history read model. |
| 21 | `tutem.dispatch.request.matched.v1` | dispatch | request | event | dispatch-service | trip-service (`trip-service.history-projection.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-08, F-10, F-12 | `status='MATCHED'`, applied by dispatch-service itself in the accept/booking transaction, then published. |
| 22 | `tutem.dispatch.request.expired.v1` | dispatch | request | event | dispatch-service | trip-service (`trip-service.history-projection.v1`), realtime-gateway (`realtime-gateway.offer-countdown-fanout.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-14 | `EXPIRED`/`NO_MATCH` sweep; frees `uq_req_one_live_per_rider`. |
| 23 | `tutem.dispatch.request.cancelled.v1` | dispatch | request | event | dispatch-service | trip-service (`trip-service.history-projection.v1`) | `request_id` | 12 | 3 | delete, 7d | **yes** | F-17 | Rider/system cancellation pre-match. |
| 24 | `tutem.trip.trip.created.v1` | trip | trip | event | trip-service | dispatch-service (`dispatch-service.carpool-matching.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-08, F-10, F-12 | RIDE/WALK: created at accept time. CARPOOL: **is** the offer (D6/D7), created *before* any booking — this is what dispatch-service's §4.12-B matching module consumes to start corridor matching (F-10 step 4). |
| 25 | `tutem.trip.trip.started.v1` | trip | trip | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-09, F-11 | `status='ACTIVE'`. dispatch-service applies `service_request.status='ONGOING'` on receipt. |
| 26 | `tutem.trip.trip.completed.v1` | trip | trip | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), driver-service (`driver-service.total-trips-increment.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-09, F-11 | `status='COMPLETED'`. Also the trigger for the flagged unbackstopped `driver.total_trips` counter (baseline §16.4 item 1/2). |
| 27 | `tutem.trip.trip.cancelled.v1` | trip | trip | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-17 | RIDE/WALK trip cancelled, or a whole CARPOOL trip cancelled by the driver (all live bookings cancelled in the same transaction). |
| 28 | `tutem.trip.trip.seats-exhausted.v1` | trip | trip | event | trip-service | realtime-gateway (`realtime-gateway.carpool-seat-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-10 | Derived fact (`seats_booked == seats_total`), **never persisted** (D6). Removes the offer from search-result view. Published again in reverse when a cancellation frees a seat. |
| 29 | `tutem.trip.booking.confirmed.v1` | trip | booking | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.carpool-seat-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-08, F-10, F-12 | Atomic seat reservation committed. For CARPOOL this is dispatch-service's `service_request.status='MATCHED'` hop (F-10 step 8); RIDE/WALK already set `MATCHED` synchronously in the accept transaction, so this consumer group is a no-op there (idempotent `WHERE status='SEARCHING'`). |
| 30 | `tutem.trip.booking.onboard.v1` | trip | booking | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-09, F-11 | OTP-verified pickup. dispatch-service applies `service_request.status='ONGOING'`. |
| 31 | `tutem.trip.booking.completed.v1` | trip | booking | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.trip-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-09, F-11 | Drop-off. dispatch-service applies `service_request.status='COMPLETED'`. |
| 32 | `tutem.trip.booking.cancelled.v1` | trip | booking | event | trip-service | dispatch-service (`dispatch-service.request-status-sync.v1`), realtime-gateway (`realtime-gateway.carpool-seat-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-10, F-17 | Seat returned (`seats_booked` decremented in the same transaction). dispatch-service applies `service_request.status='CANCELLED'` and, for provider-initiated RIDE/WALK cancellation, drives re-dispatch. |
| 33 | `tutem.trip.booking.no-show.v1` | trip | booking | event | trip-service | realtime-gateway (`realtime-gateway.carpool-seat-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-11 | Rider absent at pickup; seat freed for resale if the trip has not passed that point. |
| 34 | `tutem.trip.booking.paid.v1` | trip | booking | event | trip-service | realtime-gateway (`realtime-gateway.payment-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-16 | `payment_status='PAID'` — cash marked-collected or gateway callback success. Covers all three modes (D5: WALK is paid). |
| 35 | `tutem.trip.booking.payment-failed.v1` | trip | booking | event | trip-service | realtime-gateway (`realtime-gateway.payment-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-16 | Non-cash gateway callback failure. |
| 36 | `tutem.trip.booking.refunded.v1` | trip | booking | event | trip-service | realtime-gateway (`realtime-gateway.payment-status-fanout.v1`) | `trip_id` | 12 | 3 | delete, 7d | **yes** | F-16 | `payment_status='REFUNDED'`; no partial-refund/ledger semantics (NewSchema §6). |
| 37 | `tutem.trip.rating.submitted.v1` | trip | rating | event | trip-service | identity-service (`identity-service.rating-average-recompute.v1`), driver-service (`driver-service.rating-average-recompute.v1`) | `booking_id` | 6 | 3 | delete, 7d | **yes** — both directions of one booking stay ordered | F-18 | New `rating` row. Consumed by whichever aggregate owns the ratee (`app_user` or `driver`) to refresh the denormalised average. |
| 38 | `tutem.config.config.changed.v1` | config | config | event | config-service | identity-service, driver-service, dispatch-service, trip-service, notification-service, api-gateway, realtime-gateway (each `<service>.config-snapshot-refresh.v1`) | `config_key` | 3 | 3 | **compact** | **yes** — per key, current-value semantics | F-20 | `system_config` change; every service refreshes its Redis snapshot within seconds. **Compacted, not time-retained** — a new consumer bootstraps to current state by reading from offset 0. |
| 39 | `tutem.notification.send-push.command.v1` | notification (executing consumer) | — (command, no aggregate slot) | command | dispatch-service, trip-service, driver-service (multi-producer, §0.2.3 + D3 extension above) | notification-service (`notification-service.push-delivery.v1`) | `user_id` | 12 | 3 | delete, 3d | per-recipient | F-03, F-07, F-08, F-09, F-10, F-12, F-13, F-14, F-15, F-17, F-18 | The sole FCM-push command topic. Every push in the system is one of the enumerated producer call-sites; see §0 extension note above for the F-17 addition. |

---

## 2. Dead-letter topics

One `.dlq` per source topic (baseline §0.3), shared by every consumer of that source topic; the failing
consumer group is read from the `x-consumer-group` header, never from the topic name. Partitions match the
source topic exactly (§5.1 of Step 3). Retention: **30 days**, `delete`. **Unchanged by D8** — one-DLQ-per-topic
is retained deliberately (simple redrive targeting) regardless of whether that topic also has retry tiers
(§3). All 39 remain.

| # | Source topic | DLQ topic | Partitions |
|---|---|---|---|
| 1 | `tutem.identity.user.registered.v1` | `tutem.identity.user.registered.v1.dlq` | 6 |
| 2 | `tutem.identity.user.profile-updated.v1` | `tutem.identity.user.profile-updated.v1.dlq` | 6 |
| 3 | `tutem.identity.user.deleted.v1` | `tutem.identity.user.deleted.v1.dlq` | 6 |
| 4 | `tutem.driver.driver.created.v1` | `tutem.driver.driver.created.v1.dlq` | 6 |
| 5 | `tutem.driver.driver.went-online.v1` | `tutem.driver.driver.went-online.v1.dlq` | 24 |
| 6 | `tutem.driver.driver.went-offline.v1` | `tutem.driver.driver.went-offline.v1.dlq` | 24 |
| 7 | `tutem.driver.driver.location-updated.v1` | `tutem.driver.driver.location-updated.v1.dlq` | 24 |
| 8 | `tutem.driver.vehicle.registered.v1` | `tutem.driver.vehicle.registered.v1.dlq` | 6 |
| 9 | `tutem.driver.vehicle.deactivated.v1` | `tutem.driver.vehicle.deactivated.v1.dlq` | 6 |
| 10 | `tutem.driver.document.submitted.v1` | `tutem.driver.document.submitted.v1.dlq` | 6 |
| 11 | `tutem.driver.document.verified.v1` | `tutem.driver.document.verified.v1.dlq` | 6 |
| 12 | `tutem.driver.document.rejected.v1` | `tutem.driver.document.rejected.v1.dlq` | 6 |
| 13 | `tutem.driver.blacklist.blacklist-applied.v1` | `tutem.driver.blacklist.blacklist-applied.v1.dlq` | 6 |
| 14 | `tutem.driver.blacklist.blacklist-expired.v1` | `tutem.driver.blacklist.blacklist-expired.v1.dlq` | 6 |
| 15 | `tutem.dispatch.offer.created.v1` | `tutem.dispatch.offer.created.v1.dlq` | 12 |
| 16 | `tutem.dispatch.offer.accepted.v1` | `tutem.dispatch.offer.accepted.v1.dlq` | 12 |
| 17 | `tutem.dispatch.offer.rejected.v1` | `tutem.dispatch.offer.rejected.v1.dlq` | 12 |
| 18 | `tutem.dispatch.offer.expired.v1` | `tutem.dispatch.offer.expired.v1.dlq` | 12 |
| 19 | `tutem.dispatch.offer.withdrawn.v1` | `tutem.dispatch.offer.withdrawn.v1.dlq` | 12 |
| 20 | `tutem.dispatch.request.created.v1` | `tutem.dispatch.request.created.v1.dlq` | 12 |
| 21 | `tutem.dispatch.request.matched.v1` | `tutem.dispatch.request.matched.v1.dlq` | 12 |
| 22 | `tutem.dispatch.request.expired.v1` | `tutem.dispatch.request.expired.v1.dlq` | 12 |
| 23 | `tutem.dispatch.request.cancelled.v1` | `tutem.dispatch.request.cancelled.v1.dlq` | 12 |
| 24 | `tutem.trip.trip.created.v1` | `tutem.trip.trip.created.v1.dlq` | 12 |
| 25 | `tutem.trip.trip.started.v1` | `tutem.trip.trip.started.v1.dlq` | 12 |
| 26 | `tutem.trip.trip.completed.v1` | `tutem.trip.trip.completed.v1.dlq` | 12 |
| 27 | `tutem.trip.trip.cancelled.v1` | `tutem.trip.trip.cancelled.v1.dlq` | 12 |
| 28 | `tutem.trip.trip.seats-exhausted.v1` | `tutem.trip.trip.seats-exhausted.v1.dlq` | 12 |
| 29 | `tutem.trip.booking.confirmed.v1` | `tutem.trip.booking.confirmed.v1.dlq` | 12 |
| 30 | `tutem.trip.booking.onboard.v1` | `tutem.trip.booking.onboard.v1.dlq` | 12 |
| 31 | `tutem.trip.booking.completed.v1` | `tutem.trip.booking.completed.v1.dlq` | 12 |
| 32 | `tutem.trip.booking.cancelled.v1` | `tutem.trip.booking.cancelled.v1.dlq` | 12 |
| 33 | `tutem.trip.booking.no-show.v1` | `tutem.trip.booking.no-show.v1.dlq` | 12 |
| 34 | `tutem.trip.booking.paid.v1` | `tutem.trip.booking.paid.v1.dlq` | 12 |
| 35 | `tutem.trip.booking.payment-failed.v1` | `tutem.trip.booking.payment-failed.v1.dlq` | 12 |
| 36 | `tutem.trip.booking.refunded.v1` | `tutem.trip.booking.refunded.v1.dlq` | 12 |
| 37 | `tutem.trip.rating.submitted.v1` | `tutem.trip.rating.submitted.v1.dlq` | 6 |
| 38 | `tutem.config.config.changed.v1` | `tutem.config.config.changed.v1.dlq` | 3 |
| 39 | `tutem.notification.send-push.command.v1` | `tutem.notification.send-push.command.v1.dlq` | 12 |

### 2.1 Schema Registry subject naming for DLQ and retry topics — E4 decision (2026-07-29)

**Gap closed.** Neither this document nor `01-kafka-architecture.md` previously stated how the 39 DLQ
topics and the 9 qualifying retry topics (§3.1) interact with Schema Registry subject naming, even though
their Avro **value schema is byte-identical** to their source topic's (baseline §0.3: the original record's
key and value are forwarded unmodified; the 8 `x-*` headers are added alongside, never substituted in).

**Decision: keep the approved default `TopicNameStrategy` and duplicate-register the source topic's schema
under each DLQ and retry subject** (`<topic>.dlq-value`, `<topic>.retry-<n>-value`) — **48 extra subjects
total (39 DLQ + 9 retry)**, each holding a copy of its source topic's already-registered `.avsc`, no new
schema authored.

**Rationale:**
- Switching to `RecordNameStrategy` (subject = the Avro full record name) would change subject naming
  **globally**, not just for DLQ/retry topics — every one of the 39 base-topic subjects would stop being
  `<topic-name>-value` and start being `<namespace>.<RecordName>`, breaking the convention Step 3 §8 already
  fixed ("Subject naming: `<topic-name>-value`... matching the spirit of §0.2's fixed-arity design") and
  losing the per-topic compatibility isolation that convention buys (§8.1's `.v<major>` vs `schemaVersion`
  distinction is stated per-subject). That is a large blast radius for a narrow, mechanical problem.
- Duplicate registration is mechanical and belongs in CI (register every `.avsc` under its topic's subject,
  then again under that topic's `.dlq` and, where applicable, `.retry-1/2/3` subjects — a scripted loop, not
  a new schema-authoring task), not in application code or in a client-side strategy override.
- This governs the DLQ/retry side of the same coin the outbox already governs for the base topic side:
  registration is infrastructure/CI concern, not a runtime decision any service makes per-message.

**Consistency check against `Implementation/avro/README.md` §6:** that document already flagged this exact
gap and listed **two** options without prescribing between them — (1) duplicate-register under the
`.dlq`/`.retry-N` subject names, or (2) switch to `RecordNameStrategy`. **This decision selects option (1),
the one the README already described as "mechanical, scriptable... no new schema authoring, just extra
`POST` calls."** No contradiction exists: the README's option (1) and this section's decision are the same
mechanism, stated in two places for two audiences (implementation guidance there, architectural record
here). The README's option (2) is superseded for this repository — `RecordNameStrategy` remains available
as a documented alternative in general but is **not** what this architecture adopts, per the blast-radius
rationale above.

---

## 3. Retry topics — D8 revision (selective, not blanket)

> **D8 (user decision, 2026-07-28 — amends this section, previously a blanket 3-tier policy over all 39
> base topics).** Retry tiers are **over-provisioning** for a 3-developer team at this message rate: the
> original blanket policy produced 117 retry topics for no correctness gain on the 36 topics whose consumers
> never leave the database/Redis. **New rule (restated from `01-kafka-architecture.md` §7.0, the
> authoritative statement of the policy):** a topic gets `.retry-1/2/3` **if and only if** its consumer's
> failure-handling makes a retryable call to one of four external systems — **Parivahan SDK**, **Firebase
> Cloud Messaging (FCM)**, the **payment gateway**, or the **Routing API**. Every other topic's consumer
> retries **in-process** (bounded, local backoff) and on exhaustion publishes straight to `<topic>.dlq` —
> no retry-topic hop at all. DLQs are unaffected (§2): all 39 remain.

### 3.1 Qualifying retry topics — the only ones that exist

Cross-checked against the actual consumer groups fixed in `03-kafka-events.md` §7.2 — no consumer group is
invented here to reach a target count; **exactly three** of the 39 base topics have a documented consumer
that calls one of the four qualifying external systems. (Payment gateway and Routing API calls in this
design are both **synchronous, request/response, inside the originating API call itself** — F-16 steps 1–3
(fare computation, cash marking, gateway-intent creation) and F-07 step 2 / F-10 step 2 (fare estimate,
carpool polyline fetch) are all `[SYNC-CRITICAL]` per baseline §5 — so no *Kafka-consumed* topic today routes
through either dependency. This is reported as-is rather than padded to meet a target; if a future step adds
an async consumer that calls the payment gateway or Routing API — e.g. a dedicated refund-processing or
route-refresh consumer — its topic joins this table then, under the same rule.)

| # | Source topic | Consumer (§7.2 group) | External system | Partitions | `.retry-1` | `.retry-2` | `.retry-3` |
|---|---|---|---|---|---|---|---|
| 1 | `tutem.driver.document.submitted.v1` | `driver-service.parivahan-verification.v1` | **Parivahan SDK** — DL/RC verification call (F-03 step 5) | 6 | `…submitted.v1.retry-1` | `…submitted.v1.retry-2` | `…submitted.v1.retry-3` |
| 2 | `tutem.driver.driver.created.v1` | `notification-service.driver-welcome.v1` | **FCM** — welcome push (F-02 step 3) | 6 | `…created.v1.retry-1` | `…created.v1.retry-2` | `…created.v1.retry-3` |
| 3 | `tutem.notification.send-push.command.v1` | `notification-service.push-delivery.v1` | **FCM** — every other push in the system (§1 note, `02-kafka-topics.md`) | 12 | `…command.v1.retry-1` | `…command.v1.retry-2` | `…command.v1.retry-3` |

Backoff, header contract and max-attempt cap are unchanged by D8 (30s / 5min / 30min, then `.dlq`; Step 3
§7.2). Retry-topic partitions **match the source topic's partition count**, same rule as before (§5.1 of
Step 3) — 6, 6 and 12 respectively above.

**Retry topic count: 3 qualifying topics × 3 tiers = 9 retry topics** (was 117).

### 3.2 Non-qualifying topics — in-process retry, then straight to DLQ

The remaining **36 of 39** base topics have no retry-topic series at all. Grouped by domain for brevity
(full topic names are §1's primary table — none are repeated here to avoid a 36-row echo):

| Group | Topics (count) | Why no retry tier | Error path |
|---|---|---|---|
| Identity (`user.*`) | 3 | Consumers (`trip-service.rider-directory-cache.v1`, `dispatch-service.user-deletion-cleanup.v1`) only read/write Postgres. | In-process bounded retry → `.dlq` |
| Driver, non-external (`driver.went-online/went-offline/location-updated`, `vehicle.*`, `document.verified/rejected`, `blacklist.*`) | 9 | Consumers touch only Postgres/Redis (geo-index, verification-status recompute, blacklist geo-sync). `document.submitted` is the one driver-domain exception that *does* qualify (§3.1) — its sibling outcome topics (`verified`/`rejected`) do not, because their consumer only recomputes an aggregate status, it never calls Parivahan again. | In-process bounded retry → `.dlq` |
| Dispatch (`offer.*`, `request.*`) | 9 | Consumers are `realtime-gateway.*` (WebSocket fan-out, no external call), `driver-service.blacklist-evaluation.v1` (a `COUNT(*)` query), `trip-service.history-projection.v1`/`trip-provisioning.v1` (Postgres only). | In-process bounded retry → `.dlq` |
| Trip (`trip.*`, `booking.*`, `rating.*`) | 14 | Consumers are `dispatch-service.request-status-sync.v1` (Postgres), `driver-service.total-trips-increment.v1`/`rating-average-recompute.v1` (Postgres, Redis-idempotency-backstopped), `realtime-gateway.*` (WebSocket only). | In-process bounded retry → `.dlq` |
| Config (`config.changed`) | 1 | Every consumer is a `<service>.config-snapshot-refresh.v1` Redis write. | In-process bounded retry → `.dlq` |
| **Total non-qualifying** | **36** | | |

3 (qualifying) + 36 (non-qualifying) = **39 — every base topic accounted for exactly once.**

---

## 4. Compacted / state topics

Exactly **one** topic in the entire catalogue uses `compact` cleanup instead of `delete` — everything else
is a time-retained event stream, not a current-state store (§13.1 of Step 3):

| Topic | Cleanup policy | Key | Why compacted |
|---|---|---|---|
| `tutem.config.config.changed.v1` | `compact` | `config_key` | `system_config` is a ~10-row current-value table. Compaction lets a newly-joining or long-outage-recovering consumer read the topic from offset 0 and land on exactly today's snapshot per key, with no separate bootstrap-from-database step (§13.1). |

No other topic is a candidate: every domain fact is an **event** (a thing that happened), consumed once
and projected into the consumer's own store, never re-read for "current value" — including
`driver.location-updated`, which looks state-like but is deliberately `delete`/24h instead, because
`driver.current_location` (Postgres) and the Redis geo mirror are the actual current-state stores, refreshed
within milliseconds of each ping (§9 of Step 3, §2.1.1's accepted-cost argument).

---

## 5. Grammar validation — segment-by-segment

Every topic parses positionally into exactly 5 dot-separated segments, per baseline §0.2 (fixed arity).
`<domain>`/`<aggregate>` are checked against the §0.2.1 registry; `<event-name>`/`<action>` against §0.2.2's
case/tense rule; version against `.v<major>`.

| Topic | Seg 1 `tutem` | Seg 2 `<domain>` | Seg 3 `<aggregate>`/`<action>` | Seg 4 `<event-name>`/`command` | Seg 5 `v<major>` | Valid? |
|---|---|---|---|---|---|---|
| `tutem.identity.user.registered.v1` | tutem | identity ✓ registry | user ✓ registry (`AppUser`) | registered — past tense ✓ | v1 ✓ | ✓ |
| `tutem.identity.user.profile-updated.v1` | tutem | identity ✓ | user ✓ | profile-updated — past tense, kebab ✓ | v1 ✓ | ✓ |
| `tutem.identity.user.deleted.v1` | tutem | identity ✓ | user ✓ | deleted — past tense ✓ | v1 ✓ | ✓ |
| `tutem.driver.driver.created.v1` | tutem | driver ✓ | driver ✓ registry (`Driver`) | created — past tense ✓ | v1 ✓ | ✓ |
| `tutem.driver.driver.went-online.v1` | tutem | driver ✓ | driver ✓ | went-online — past tense, kebab ✓ | v1 ✓ | ✓ |
| `tutem.driver.driver.went-offline.v1` | tutem | driver ✓ | driver ✓ | went-offline ✓ | v1 ✓ | ✓ |
| `tutem.driver.driver.location-updated.v1` | tutem | driver ✓ | driver ✓ | location-updated ✓ | v1 ✓ | ✓ |
| `tutem.driver.vehicle.registered.v1` | tutem | driver ✓ | vehicle ✓ registry (`Vehicle`) | registered ✓ | v1 ✓ | ✓ |
| `tutem.driver.vehicle.deactivated.v1` | tutem | driver ✓ | vehicle ✓ | deactivated ✓ | v1 ✓ | ✓ |
| `tutem.driver.document.submitted.v1` | tutem | driver ✓ | document ✓ registry (`DriverDocument`) | submitted ✓ | v1 ✓ | ✓ |
| `tutem.driver.document.verified.v1` | tutem | driver ✓ | document ✓ | verified ✓ | v1 ✓ | ✓ |
| `tutem.driver.document.rejected.v1` | tutem | driver ✓ | document ✓ | rejected ✓ | v1 ✓ | ✓ |
| `tutem.driver.blacklist.blacklist-applied.v1` | tutem | driver ✓ | blacklist ✓ registry (`DriverBlacklist`) | blacklist-applied ✓ | v1 ✓ | ✓ |
| `tutem.driver.blacklist.blacklist-expired.v1` | tutem | driver ✓ | blacklist ✓ | blacklist-expired ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.offer.created.v1` | tutem | dispatch ✓ | offer ✓ registry (`RideOffer`) | created ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.offer.accepted.v1` | tutem | dispatch ✓ | offer ✓ | accepted ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.offer.rejected.v1` | tutem | dispatch ✓ | offer ✓ | rejected ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.offer.expired.v1` | tutem | dispatch ✓ | offer ✓ | expired ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.offer.withdrawn.v1` | tutem | dispatch ✓ | offer ✓ | withdrawn ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.request.created.v1` | tutem | dispatch ✓ | request ✓ registry (`ServiceRequest`) | created ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.request.matched.v1` | tutem | dispatch ✓ | request ✓ | matched ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.request.expired.v1` | tutem | dispatch ✓ | request ✓ | expired ✓ | v1 ✓ | ✓ |
| `tutem.dispatch.request.cancelled.v1` | tutem | dispatch ✓ | request ✓ | cancelled ✓ | v1 ✓ | ✓ |
| `tutem.trip.trip.created.v1` | tutem | trip ✓ | trip ✓ registry (`Trip`); repeated segment **approved** (§0.2.1) | created ✓ | v1 ✓ | ✓ |
| `tutem.trip.trip.started.v1` | tutem | trip ✓ | trip ✓ | started ✓ | v1 ✓ | ✓ |
| `tutem.trip.trip.completed.v1` | tutem | trip ✓ | trip ✓ | completed ✓ | v1 ✓ | ✓ |
| `tutem.trip.trip.cancelled.v1` | tutem | trip ✓ | trip ✓ | cancelled ✓ | v1 ✓ | ✓ |
| `tutem.trip.trip.seats-exhausted.v1` | tutem | trip ✓ | trip ✓ | seats-exhausted — derived state, still a valid past-participle-style fact name ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.confirmed.v1` | tutem | trip ✓ | booking ✓ registry (`Booking`, entity of `Trip`, own token per §0.2.1) | confirmed ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.onboard.v1` | tutem | trip ✓ | booking ✓ | onboard ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.completed.v1` | tutem | trip ✓ | booking ✓ | completed ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.cancelled.v1` | tutem | trip ✓ | booking ✓ | cancelled ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.no-show.v1` | tutem | trip ✓ | booking ✓ | no-show ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.paid.v1` | tutem | trip ✓ | booking ✓ | paid ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.payment-failed.v1` | tutem | trip ✓ | booking ✓ | payment-failed ✓ | v1 ✓ | ✓ |
| `tutem.trip.booking.refunded.v1` | tutem | trip ✓ | booking ✓ | refunded ✓ | v1 ✓ | ✓ |
| `tutem.trip.rating.submitted.v1` | tutem | trip ✓ | rating ✓ registry (`Rating`) | submitted ✓ | v1 ✓ | ✓ |
| `tutem.config.config.changed.v1` | tutem | config ✓ | config ✓ registry (`SystemConfig`); repeated segment **approved** | changed ✓ | v1 ✓ | ✓ |
| `tutem.notification.send-push.command.v1` | tutem | notification ✓ registry (executing consumer, no aggregate) | send-push — imperative `<action>` ✓ | **command** literal ✓ | v1 ✓ | ✓ |

39/39 valid. Every `<domain>`/`<aggregate>` pair is drawn verbatim from §0.2.1; no synonym, abbreviation or
invented token appears anywhere in the table.

---

## 6. Coverage matrix — flows → topics

| Flow | Topics used | Notes |
|---|---|---|
| F-01 Onboarding | `user.registered`, `user.profile-updated`, `user.deleted` | OTP challenge (step 2) and session issuance (step 3) are synchronous — see §7. |
| F-02 Become a carpool driver | `driver.created`, `vehicle.registered` | Step 4 (cannot go online while `PENDING`) is a synchronous service-layer check. |
| F-03 Document verification | `document.submitted`, `document.verified`, `document.rejected`, `notification.send-push.command` (F-03.8) | The Parivahan call itself is an external HTTP call inside driver-service's async worker, not a topic. |
| F-04 Driver goes online | `driver.went-online` | Eligibility checks (step 2) are synchronous. |
| F-05 Driver goes offline | `driver.went-offline` | |
| F-06 Location ping | `driver.driver.location-updated` | Dominant topic by volume (§2.1 of Step 3). |
| F-07 Ride request → match | `request.created`, `offer.created`, `notification.send-push.command` (F-07.7) | Fare estimate (step 2) and request insert (step 3) are synchronous. |
| F-08 Accept race | `offer.accepted`, `offer.withdrawn`, `request.matched`, `trip.created`, `booking.confirmed`, `notification.send-push.command` (F-08.6) | **The accept `UPDATE` itself is never a topic** — Kafka only broadcasts the already-settled outcome (§1.2 of Step 3, restated as a hard constraint here). |
| F-09 Ride execution | `trip.started`, `trip.completed`, `booking.onboard`, `booking.completed`, `notification.send-push.command` (F-09.5) | OTP verify and all status-transition writes are synchronous. |
| F-10 Carpool offer + booking | `trip.created`, `request.matched` (via `booking.confirmed` hop), `booking.confirmed`, `trip.seats-exhausted`, `booking.cancelled`, `notification.send-push.command` (F-10.6, F-10.10) | The "matches-generated" narrative fact (F-10 step 5) is **not** a separate topic: dispatch-service's `matching` module computes it and emits `notification.send-push.command` directly in the same consumer transaction that consumes `trip.created` — no intermediate topic is needed because the computation and the notify decision live in one service. The atomic seat reservation (step 8) and its cancellation counterpart (step 14) are synchronous, never Kafka-mediated. |
| F-11 Carpool execution | `trip.started`, `trip.completed`, `booking.onboard`, `booking.completed`, `booking.no-show` | Per-rider OTP verify and seat-count bookkeeping are synchronous. |
| F-12 Walk match | `offer.created`, `offer.accepted`, `offer.withdrawn`, `trip.created`, `booking.confirmed`, `booking.paid`, `notification.send-push.command` (F-08.6, reused — "identical to F-08") | |
| F-13 Rejection → blacklist | `offer.rejected`, `blacklist.blacklist-applied`, `notification.send-push.command` (F-13.5) | The reject write itself and the blacklist insert are synchronous; only the fan-out and the cross-trigger convergence are async. |
| F-14 Expiry sweeps | `offer.expired`, `request.expired`, `blacklist.blacklist-applied` (2nd trigger path), `notification.send-push.command` (F-14.3) | The sweep's `UPDATE ... WHERE status='SENT' AND expires_at <= now()` is the synchronous, idempotent core; publishing is the async tail. |
| F-15 Blacklist expiry | `blacklist.blacklist-expired`, `notification.send-push.command` (F-15.3) | The nightly clear and the lazy go-online check are synchronous. |
| F-16 Payment | `booking.paid`, `booking.payment-failed`, `booking.refunded` | Cash marking and fare computation are synchronous; only gateway callbacks and refunds are async. |
| F-17 Cancellation | `request.cancelled`, `offer.withdrawn`, `booking.cancelled`, `trip.cancelled`, `notification.send-push.command` (F-17.1, F-17.5, F-17.6 — **D3 addition**, §0 above) | |
| F-18 Rating | `rating.submitted`, `notification.send-push.command` (shares F-09.5's rating-prompt call) | The rating insert itself is synchronous (`uq_rating_once`, `ck_rating_self`); only the average-recompute propagation to identity/driver is async. |
| **F-19 History retrieval** | **none** | **Fully synchronous — no topic needed.** All three reads (`booking` by rider, `trip` by provider, `service_request` by rider) are paged queries over already-committed, already-published data; nothing here produces a new fact or needs a fan-out. History *content* arrives via the topics above (consumed into trip-service's Q-18(b) read model); the *retrieval* itself is a plain indexed read. |
| F-20 Config change | `config.config.changed` | The admin write and the Redis-snapshot write are synchronous; only the cross-service fan-out is async. |

**F-19 is the only flow needing zero topics.** Every other flow has at least one ASYNC-CANDIDATE or
SCHEDULED step and therefore at least one topic.

---

## 7. Totals

**By kind:**

> **D8 note — before/after, for audit.** The pre-D8 blanket policy (3 retry tiers on every one of the 39
> base topics) produced **195 topics / 2,115 partitions** — on a 3-broker RF=3 cluster that is ~705
> partitions per broker before any growth headroom is even applied, which is what the user flagged as
> over-provisioned for a 3-developer team at ~1,300–1,500 msg/s (Step 3 §2.1). D8 keeps every DLQ (39,
> unchanged) and reduces retry topics from 117 to **9** (§3.1). The corrected figures below are the current,
> governing totals; the pre-D8 numbers are struck through for traceability, not left ambiguous.

| Kind | Count (pre-D8) | Count (post-D8, current) |
|---|---|---|
| Event topics | 38 | 38 |
| Command topics | 1 | 1 |
| **Base topics (event + command)** | **39** | **39** |
| DLQ topics | 39 | **39 — unchanged** |
| Retry topics | ~~117~~ (3 tiers × 39) | **9** (3 tiers × 3 qualifying topics, §3.1) |
| **Grand total topics** | ~~195~~ | **87** |

**Partitions:**

| Class (from Step 3 §5.1) | Topics | Partitions each | Subtotal |
|---|---|---|---|
| Driver location/online-state (24) | went-online, went-offline, location-updated | 3 | 72 |
| Offer/request race-arbiter (12) | offer×5, request×4 | 9 | 108 |
| Trip/booking lifecycle (12) | trip×5, booking×8 | 13 | 156 |
| Low-volume driver lifecycle (6) | driver.created, vehicle×2, document×3, blacklist×2 | 8 | 48 |
| Identity/rating (6) | user×3, rating×1 | 4 | 24 |
| Config (3, compacted) | config.changed | 1 | 3 |
| Command (12) | send-push.command | 1 | 12 |
| **Base topic partitions** | | | **423 — unchanged** |
| DLQ (matches source) | 39 topics | — | **423 — unchanged** |
| Retry, 3 tiers (matches source × 3) — **qualifying topics only** | 3 topics (`document.submitted`=6, `driver.created`=6, `send-push.command`=12; §3.1) | — | ~~1,269~~ → **72** |
| **Grand total partitions across the cluster** | | | ~~2,115~~ → **918** |

**Per-broker, 3 brokers (same division convention as the D8 note above, ~705 pre-D8):**
918 ÷ 3 = **306 partitions per broker** — a ~57% reduction from the pre-D8 figure, entirely from removing
the 108 non-qualifying retry topics (36 topics × 3 tiers); base-topic and DLQ partition counts are untouched.

Base-topic partition subtotals are consistent with Step 3 §5.1's per-class counts (24 / 12 / 6 / 3) and
with §5's headroom rationale (partitions only grow, never shrink). D8 changes **which** topics get retry
tiers, never the per-class sizing rule for the ones that still do (§3.1's 6/6/12 match their source topics'
own class).

---

## 8. PII — forbidden fields per topic

Per Step 3 §11: phone numbers, raw `doc_number`, `image_url`s and precise location coordinates are never
placed in cleartext in a Kafka payload, with **one deliberate exception**.

| Topic(s) | PII that would naturally appear | Forbidden fields | What a consumer does instead |
|---|---|---|---|
| `tutem.driver.driver.location-updated.v1` | Precise coordinate | **None forbidden — the one deliberate exception.** The coordinate *is* the entire content of the fact (§11 of Step 3). Mitigated by 24h retention and ACLs restricted to driver-service + realtime-gateway only. | n/a |
| `tutem.identity.user.*` (registered, profile-updated, deleted) | phone, email, gender | `phone`, `email`, `gender` | Consumer calls identity-service's public-profile API (name + rating only) if display detail is needed. |
| `tutem.driver.document.*` (submitted, verified, rejected) | `doc_number`, `holder_name`, `image_url`, `parivahan_details` | `doc_number`, `holder_name`, `image_url`, `parivahan_details` | Consumer (driver-service's own recompute group) already has direct DB access; no external consumer of these topics exists, and none may be granted without re-justifying this row. |
| `tutem.dispatch.offer.*`, `tutem.dispatch.request.*` | pickup/drop address strings, exact pickup/drop coordinates | free-text `pickup_address`/`drop_address`; raw `pickup_point`/`drop_point` geometry beyond a rounded distance figure | Rider/driver-facing detail is fetched via the owning service's sync API at render time. |
| `tutem.trip.trip.*`, `tutem.trip.booking.*` | rider/driver identity detail, exact route geometry, `payment_ref` | full driver/rider profile fields; raw `route_line` geometry (identifiers + derived seat/status facts only); `payment_ref` (gateway token) | Vehicle/driver display detail via Q-21's fetch-with-Redis-cache pattern; payment reconciliation stays inside trip-service's `payment` module. |
| `tutem.trip.rating.submitted.v1` | rating comment text (potentially identifying) | free-text `comment` | Only `score`, not `comment`, crosses the topic; comment display (if any) is a sync read from `rating`. |

---

## 9. Where a topic exists only because of a specific decision

| Topic | Decision | Why it would not otherwise exist |
|---|---|---|
| Every FCM leg reachable only via `tutem.notification.send-push.command.v1` **for F-17.1/F-17.5/F-17.6** | **D3** | Without D3's "both paths, always" rule, these three cancellation notices would rely on realtime-gateway's WebSocket consumption of `request.cancelled`/`booking.cancelled`/`trip.cancelled` alone; D3 makes the command-topic leg mandatory even though baseline §0.2.3's original enumeration did not name these three call-sites. |
| `tutem.dispatch.offer.expired.v1` as a **blacklist-evidence** producer (consumed by `driver-service.blacklist-evaluation.v1`) | **D4** | Absent D4, `EXPIRED` would not count towards the blacklist (NewSchema §7.3's original "kinder default" was rejections only), and this topic's consumption by driver-service's blacklist-evaluation group would not exist — expiry would only need to feed `request.expired`'s rider-facing notice, not a second blacklist trigger path. |
| `tutem.trip.trip.seats-exhausted.v1` | **D6** | "Full" is derived (`seats_booked == seats_total`), never persisted as a status — this topic exists purely to announce a computed transition so search results and viewers converge, with no corresponding DB column ever changing. |
| Absence of any `RideOffer`-topic traffic for CARPOOL anywhere in this catalogue | **D7** | Carpool never creates a `ride_offer` row, so `offer.created/accepted/rejected/expired/withdrawn` carry `RIDE`/`WALK` traffic only — reflected in the coverage matrix (§6), where F-10/F-11 never appear against any `offer.*` topic. |

---

**End of Step 4.** Awaiting Orchestrator approval before Step 5 (event catalogue) proceeds.
