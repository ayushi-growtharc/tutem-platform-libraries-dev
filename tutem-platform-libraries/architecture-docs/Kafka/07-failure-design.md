# Tutem — Failure Design & Risk Register (Step 9)

> **Status:** DRAFT — this document is an ANALYSIS of the approved design (Steps 2–8). It RECOMMENDS
> mitigations; it does not apply any of them. Every recommendation below is explicitly labelled
> **RECOMMENDATION — requires Orchestrator approval** and none is to be read as an approved decision.
> Nothing here renames a topic, consumer group, table, column, index or constraint. Every table/column/
> index/constraint name cited is quoted verbatim from `NewSchema.md`; every topic/consumer-group/event name
> is quoted verbatim from Steps 4/5/7/8.
> **Input (all APPROVED unless noted):** `00-architecture-baseline.md` (Step 2), `01-kafka-architecture.md`
> (Step 3, D8-amended), `02-kafka-topics.md` (Step 4, D8-amended), `03-kafka-events.md` (Step 5, not read
> directly here — its content is fully represented in Steps 7/8's verbatim quoting of consumer groups/
> topics/idempotency keys), `04-event-flows.md` (Step 6, DRAFT, read — F-08/F-10 sequence diagrams used to
> confirm interleavings), `05-consumers.md` (Step 7, DRAFT), `06-producers.md` (Step 8, DRAFT, includes the
> D10 sharded-relay mitigation).
> **Never claimed:** end-to-end exactly-once delivery, anywhere. Kafka never settles the accept race —
> `uq_offer_single_accept` does, and this document repeats that constraint, never a Kafka mechanism, as the
> arbiter in every place the race is discussed.
> **Baseline §10 status:** UNAPPROVED. `trip.departure_time`, `service_request.departure_after`/
> `departure_before`, `uq_booking_pickup_order`, `booking.tip_amount`, `service_request.preferences` do not
> exist in the schema this document analyses. Every finding that would be closed by one of these columns
> says so explicitly and treats the column as unavailable.

---

## 0. How to read the finding IDs

`RC-` race condition, `ORD-` ordering, `DUP-` duplicate event, `LAG-` consumer lag, `SPOF-` single point of
failure, `RPL-` replay, `RTL-` retry loop, `DLK-` deadlock, `TH-` thundering herd/cold-start, `PP-` poison
pill. Each finding gives: **what breaks**, **exact trigger**, **blast radius**, **current mitigation**,
**severity**, **RECOMMENDATION (requires approval)**, and **needs §10 schema change?**.

---

## 1. Duplicate events

### 1.1 Where a duplicate can arise — mechanism catalogue

Three duplicate sources exist in this design, and they recur identically across all 39 topics:

| Source | Mechanism | Which topics are exposed |
|---|---|---|
| **Outbox republish after a crash between publish and `published_at`** | `06-producers.md` §1–§5: the relay's Kafka `send()` and the subsequent `UPDATE outbox_event SET published_at=...` are two separate Postgres/Kafka steps (`01-kafka-architecture.md` §3/§4.2). A crash between them republishes the same row (same `eventId`) on the next poll. | **Every one of the 39 base topics** — this is a property of the outbox mechanism itself, not of any one topic. |
| **Consumer reprocessing after a rebalance** | A consumer commits its DB/Redis side effect but the offset commit (`05-consumers.md`: "manual acknowledgment, committed only after the consumer's side effect... has durably completed") is lost to a rebalance or crash before the commit lands; the next owner of the partition re-reads from the last committed offset. | **Every one of the 30 consumer groups** — manual-commit-after-effect is universal, so this exposure is universal too. |
| **Retry-tier / DLQ redrive replay** | `01-kafka-architecture.md` §7.4: a redrive republishes the original key/value/`eventId` byte-for-byte. Only the 3 D8-qualifying topics (`tutem.driver.document.submitted.v1`, `tutem.driver.driver.created.v1`, `tutem.notification.send-push.command.v1`) have retry-tier hops; all 39 topics have a `.dlq` and can be redriven manually. | 3 retry-tier topics (automatic hop) + all 39 (manual redrive). |

### 1.2 Consumer-by-consumer idempotency table

Every consumer is required to be idempotent (`01-kafka-architecture.md` §6, baseline A-09). The real
question is *what backstops it* — a DB unique index/conditional `WHERE` (strong), or Redis alone (weak).
Grouped by backstop class, drawing on `05-consumers.md` §1–§7:

| Backstop class | Consumer groups | Backstop object | Duplicate-safe? |
|---|---|---|---|
| **DB unique index rejects the duplicate outright** | `trip-service.trip-provisioning.v1` | `uq_trip_one_live_per_provider`, `uq_booking_request` | Yes — insert throws, caught, logged `skipped-duplicate` |
| | `driver-service.blacklist-evaluation.v1` (outbound insert) | `uq_bl_one_per_day` | Yes |
| **DB conditional `UPDATE ... WHERE <state>` makes a re-apply match zero rows** | `dispatch-service.user-deletion-cleanup.v1`, `dispatch-service.request-status-sync.v1` | conditional `UPDATE service_request SET status=... WHERE status=<prior>` | Yes |
| | `driver-service.geo-index-maintenance.v1` | conditional `UPDATE driver ... WHERE location_updated_at < :occurredAt` | Yes (last-write-wins, not a unique index, but DB-enforced) |
| **Full recompute, not an increment — naturally idempotent** | `driver-service.verification-status-recompute.v1`, `driver-service.rating-average-recompute.v1`, `identity-service.rating-average-recompute.v1` | re-derivation from current rows | Yes — re-running produces the same answer |
| **Redis GEO op is naturally idempotent (no DB write at all)** | `driver-service.blacklist-geo-sync.v1` | `ZREM`/`GEOADD` idempotent by nature | Yes, but no DB fact is at stake here |
| **Redis-only, no DB backstop — literal `+1` increment** | `driver-service.total-trips-increment.v1` | `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` only | **No — see DUP-01** |
| **Redis-only, no DB backstop, accepted by design (no table owned)** | `notification-service.driver-welcome.v1`, `notification-service.push-delivery.v1` | `tutem:notification:sent:<idempotencyKey>` only | Accepted — duplicate push is UX noise, per `01-kafka-architecture.md` §3 |
| **Read-only, re-running recomputes the same fan-out, no write at all** | `dispatch-service.carpool-matching.v1` | none needed — idempotent by construction | Yes |
| **WebSocket push — client-side `eventId` de-dup (D3)** | all 7 `realtime-gateway.*` consumers | client dedup, not server dedup | Yes, cheaper than FCM because no external system |
| **Keyed upsert (`HSET`) — naturally idempotent** | all `*.config-snapshot-refresh.v1` (×7), `trip-service.rider-directory-cache.v1`, `trip-service.vehicle-snapshot-cache.v1`, `trip-service.history-projection.v1` | upsert semantics | Yes |

**Finding DUP-01 — `driver.total_trips` has no DB backstop, quantified.**

- **What breaks:** `driver.total_trips` (a plain integer column on `driver`, no `CHECK`, no unique index
  governing it) is incremented by `driver-service.total-trips-increment.v1` on
  `tutem.trip.trip.completed.v1`. The increment is `+1`, not a recompute.
- **Exact trigger:** (a) outbox republish of `TripCompleted` after a relay crash between publish and
  `published_at` — a normal, expected at-least-once redelivery, not an error condition; or (b) a rebalance
  after the DB `UPDATE driver SET total_trips = total_trips + 1` commits but before the offset commits.
  Either redelivers the same `eventId` to the same consumer group. If `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>`
  has already expired or been flushed, the handler re-executes the `+1`.
- **Blast radius:** one driver's `total_trips` permanently inflated by 1 per undetected duplicate. No
  cascading effect on any other table — `total_trips` is not referenced by any constraint, FK or blacklist
  rule in `NewSchema.md`. Cosmetic/reporting-grade data corruption, not a correctness break for any
  transactional flow (accept race, seat reservation, payment are all unaffected).
- **Quantified exposure:** requires the *coincidence* of (i) a redelivery of `TripCompleted` for the same
  `tripId` AND (ii) the Redis idempotency key for that specific `eventId` being absent at redelivery time.
  (ii) happens only on a Redis flush/failover or a TTL shorter than the redelivery gap — both are
  operational events, not routine. This is a genuine but narrow, low-frequency risk: it fires on the
  *intersection* of two independent low-probability events, not on ordinary at-least-once redelivery (which
  the Redis key alone already absorbs).
- **Current mitigation:** `tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` (Redis, TTL
  longer than the max retry/replay window) — the *sole* mechanism, explicitly flagged as such in
  `05-consumers.md` §1.6 and §11, carried from baseline §16.4 items 1–2.
- **Severity: MEDIUM.** Real, silent, permanent, but narrow-window and cosmetically scoped (no downstream
  constraint depends on `total_trips`).
- **RECOMMENDATION (requires Orchestrator approval) — four options, in order of preference:**
  1. **Derive the count instead of storing it.** Replace `driver.total_trips` reads with
     `SELECT COUNT(*) FROM trip WHERE provider_id = :driverId AND status = 'COMPLETED'` computed on demand
     (or cached in Redis as a *read-through cache of a query*, not as an authoritative write target). Zero
     schema change, but removes the denormalised column's purpose — trades a write-side risk for a
     read-side query cost. No §10 dependency.
  2. **A dedicated processed-events table** (`driver_trip_increment_processed(event_id UUID PRIMARY KEY,
     driver_id UUID, processed_at TIMESTAMPTZ)`), written in the *same transaction* as the `+1` UPDATE, with
     the INSERT's own PK collision as the real backstop (mirrors how `uq_bl_one_per_day` backstops blacklist
     evaluation). This is a **new table**, which baseline §6/NewSchema §6 deliberately avoided for exactly
     this class of bookkeeping — flagged as a philosophy conflict, not just an implementation detail.
  3. **A natural idempotency key on the write itself**: e.g. `UPDATE driver SET total_trips = (SELECT
     COUNT(*) FROM trip WHERE provider_id = driver.user_id AND status='COMPLETED')` — i.e. fold option 1's
     derivation into the write path so the stored column is always a recompute, not an increment, using the
     same pattern already proven safe for `rating_avg`/`rating_count` in
     `driver-service.rating-average-recompute.v1`/`identity-service.rating-average-recompute.v1`. This is the
     **cheapest fix that keeps the column** and requires no new table and no §10 change.
  4. **Accept the drift, add a reconciliation job**: a nightly sweep recomputes `driver.total_trips` from
     `COUNT(*) FROM trip WHERE provider_id=:id AND status='COMPLETED'` and corrects any drift, bounding the
     exposure to at most one day of double-counts. Cheapest to build, does not fix the root cause, only
     bounds it.
  - **Needs §10 schema change?** No — none of the four options touches baseline §10's unapproved column
    list. Option 2 needs a *new* table, but not one of the 5 proposed in §10.

---

## 2. Race conditions

### 2.1 RC-01 — The accept race itself (confirmed settled by the DB, not Kafka)

- **Confirmed:** `06-producers.md` §3 and `04-event-flows.md` §2.1 both show the accept path as **one**
  conditional statement — `UPDATE ride_offer SET status='ACCEPTED', responded_at=now() WHERE id=:offerId
  AND status='SENT' AND expires_at > now()` — backed by the partial unique index `uq_offer_single_accept ON
  ride_offer (request_id) WHERE status = 'ACCEPTED'`. Sibling `WITHDRAWN` updates and
  `service_request.status='MATCHED'` happen in the **same transaction**. No consumer path publishes to, or
  derives, the accept decision — every consumer of `tutem.dispatch.offer.accepted.v1`
  (`realtime-gateway.offer-countdown-fanout.v1`, `trip-service.trip-provisioning.v1`) only reacts to an
  already-committed fact.
- **Can any consumer path create a second booking?** No. `trip-service.trip-provisioning.v1` is the only
  consumer that inserts `trip`/`booking` rows from `RideOfferAccepted`, and it is backstopped by
  **`uq_trip_one_live_per_provider`** and **`uq_booking_request`** — both are DB unique indexes, so even a
  redelivered `RideOfferAccepted` (from outbox republish or rebalance replay) cannot create a second
  `trip`/`booking` pair; `05-consumers.md` §4.3 confirms the constraint violation is caught and logged
  `skipped-duplicate`, not routed to `.dlq`.
- **Verify `uq_booking_request` is unconditional and the Q-19 consequence:** `NewSchema.md` line 448:
  `CONSTRAINT uq_booking_request UNIQUE (request_id)` — this is a plain `UNIQUE`, not a partial index, so
  it applies regardless of the booking's or request's status, forever. Baseline (line 1115) states the
  direct consequence: **a cancelled booking's `request_id` can never be reused — a cancelling rider who
  wants to re-book needs a brand-new `service_request` row (Q-19)**, because `uq_booking_request` would
  reject a second `booking` insert against the same `request_id` even after the first booking is
  `CANCELLED`.
- **Does any flow in Steps 3–8 silently assume re-service of the same request is possible?** Checked
  against `05-consumers.md` §3.3 (`dispatch-service.request-status-sync.v1`) and `06-producers.md` §4: no
  consumer or producer attempts to re-open a `CANCELLED` `booking`'s `request_id`. F-17 (cancellation, per
  baseline) explicitly routes provider-initiated RIDE/WALK cancellation to "re-dispatch," and re-dispatch in
  this design **must mean a new `service_request`**, not a retry against the same row — but neither
  `02-kafka-topics.md` nor `05-consumers.md` names a consumer or event that *creates* the cloned
  `service_request` for re-dispatch. This is a **gap**, not a silent wrong assumption: no document
  incorrectly claims the same request can be re-served, but no document assigns which service creates the
  clone or on what trigger either.
- **Finding RC-02 — re-dispatch-after-cancellation has no owning consumer.**
  - **What breaks:** a provider-cancelled RIDE/WALK booking (`tutem.trip.booking.cancelled.v1`, consumed by
    `dispatch-service.request-status-sync.v1` per `05-consumers.md` §3.3) sets `service_request.status`
    toward `CANCELLED`, but no consumer in the catalogue is documented to *originate a fresh
    `service_request`* for the same rider so dispatch can restart the search.
  - **Trigger:** provider cancels a `CONFIRMED` RIDE/WALK booking after acceptance.
  - **Blast radius:** the rider's ride silently ends with no re-search, unless a client-side "search again"
    flow (outside Kafka, a plain API call re-submitting F-07 step 1) covers it — which is plausible but not
    documented anywhere in Steps 2–8.
  - **Current mitigation:** none documented.
  - **Severity: HIGH** — a functional gap, not merely a resiliency one; user-facing.
  - **RECOMMENDATION (requires approval):** either (a) confirm this is intentionally client-initiated (the
    rider's app re-calls "create request" — no Kafka change needed, just an explicit statement in
    `04-event-flows.md`/`02-kafka-topics.md`), or (b) add an explicit re-dispatch trigger consumed by
    dispatch-service off `tutem.trip.booking.cancelled.v1` that creates a new `service_request` cloned from
    the cancelled booking's original request fields. Option (b) is new consumer-group scope, not a schema
    change.
  - **Needs §10 schema change?** No.

### 2.2 RC-03 — Carpool atomic seat reservation

- **The interleaving:** two riders, R1 and R2, both call "book a seat" on the same carpool `trip` at
  `seats_total=4`, `seats_booked=3` (1 seat left), near-simultaneously.
  - trip-service's booking transaction (per `06-producers.md` §4 and baseline F-10 step 8) is: `check offer
    open → check seats available → increment seats_booked → insert booking → commit`, all one transaction,
    with `ck_trip_seats CHECK (seats_total BETWEEN 1 AND 8 AND seats_booked BETWEEN 0 AND seats_total)` as
    the backstop.
  - **Isolation level needed:** Postgres's default `READ COMMITTED` is **not** sufficient on its own for
    "check seats available → increment" without a row-level lock, because two concurrent transactions could
    both read `seats_booked=3, seats_total=4` (both see 1 seat free) under `READ COMMITTED` before either
    commits. The design as documented does not state an explicit `SELECT ... FOR UPDATE` on the `trip` row
    in the booking transaction — `06-producers.md` §4 describes "check → increment → insert → commit" but
    does not name a locking clause.
  - **What actually prevents oversell:** `ck_trip_seats` is a **row-level `CHECK` constraint**, re-evaluated
    at commit for every UPDATE to that row. Under `READ COMMITTED`, if R1's transaction commits first
    (`seats_booked: 3→4`), R2's transaction — having read the pre-R1 value — will have its `UPDATE trip SET
    seats_booked = seats_booked + 1 WHERE id=:tripId` **re-read the current row value at UPDATE time**
    (Postgres `UPDATE` always operates on the latest committed row, re-checking the `WHERE` and evaluating
    `CHECK` against the post-update value), so R2's `UPDATE` would set `seats_booked=5`, violating
    `ck_trip_seats (seats_booked BETWEEN 0 AND seats_total)` and the whole transaction rolls back. **This
    means oversell is prevented by `ck_trip_seats` catching the second writer at commit time, not by an
    explicit application-level lock** — correctness holds, but the failure mode for the loser is a rolled-back
    transaction/exception, not a clean "seat unavailable" business response, unless the application
    explicitly catches the `CHECK` violation and retries as "no seats left." **This exact behavior is not
    documented in `06-producers.md` or `05-consumers.md`.**
  - **Finding RC-04 — the seat-check-and-increment step is described narratively, not as an explicit
    `SELECT ... FOR UPDATE`, and the application's handling of the `ck_trip_seats` violation is unspecified.**
    - **What breaks:** nothing breaks *correctness* — `ck_trip_seats` genuinely makes oversell impossible —
      but the operational behavior (does the loser get a clean "no seats" response, or a raw constraint-violation
      exception surfaced to the API caller?) is undocumented, and if the application does a naive
      "check-then-increment" as two statements without `FOR UPDATE`, the loser wastes a round trip and gets
      an ugly failure rather than a fast, correct rejection.
    - **Trigger:** the interleaving above, at load (a carpool trip nearing full during a busy corridor).
    - **Blast radius:** narrow — one rider's booking attempt, correctly rejected but potentially with a poor
      UX (a 500-class error rather than a clean "seat taken").
    - **Current mitigation:** `ck_trip_seats` (correctness only, not UX).
    - **Severity: LOW** (correctness is fine; this is a UX/observability gap).
    - **RECOMMENDATION (requires approval):** explicitly document that the seat-reservation transaction uses
      `SELECT seats_total, seats_booked FROM trip WHERE id=:tripId FOR UPDATE` before the seats-available
      check, so the loser blocks briefly and receives a clean "seat unavailable" from an explicit
      application check rather than relying on the `CHECK` constraint as the sole backstop. This is a
      documentation/implementation clarification, not a schema change.
    - **Needs §10 schema change?** No.
- **Cancellation racing a new booking:** trip-service's cancellation transaction (F-10 step 14) decrements
  `trip.seats_booked` in the same transaction as `booking.status='CANCELLED'`. If R3 cancels while R4 is
  mid-booking on the same trip, both transactions target the same `trip` row's `seats_booked` column;
  Postgres serializes the two `UPDATE`s on that row (row-level lock acquired by the first `UPDATE` to reach
  it), so **no lost update is possible** — whichever commits second sees the first's already-applied
  change. The only externally visible effect is ordering-dependent: if the cancellation commits first, R4's
  booking may succeed where it would otherwise have failed (a freed seat becomes available); if R4's booking
  commits first, the cancellation still succeeds (decrementing from R4's new, higher `seats_booked`). Either
  order is correct; **no oversell, no lost update, no deadlock** between these two transactions because both
  acquire the lock via a plain single-row `UPDATE`, never nested lock acquisition in different orders (see
  §8 Deadlocks for why this matters).

### 2.3 RC-05 — D4 blacklist evaluation double-triggered same day

- **The scenario:** F-13 (driver rejects, `tutem.dispatch.offer.rejected.v1`) and F-14 (expiry sweep,
  `tutem.dispatch.offer.expired.v1`) both feed **the same consumer group**,
  `driver-service.blacklist-evaluation.v1` (`05-consumers.md` §1.5), which counts `ride_offer` rows with
  `status IN ('REJECTED','EXPIRED')` for the driver, same day, against `driver.rejection.daily_threshold`.
- **Can the same day's evidence trigger twice?** Yes, by design — that is the entire point of D4 (both
  `REJECTED` and `EXPIRED` count). The question is whether the resulting `driver_blacklist` INSERT is safe
  when both paths fire close together for the same driver on the same day.
- **Does `uq_bl_one_per_day` hold for both trigger paths?** `NewSchema.md` lines 521–525 define **two**
  distinct partial unique indexes on `driver_blacklist`, and they must not be conflated:
  `CREATE UNIQUE INDEX uq_bl_one_active ON driver_blacklist (driver_id) WHERE is_active;` (at most one
  *live* bar per driver) and, separately, `CREATE UNIQUE INDEX uq_bl_one_per_day ON driver_blacklist
  (driver_id, trigger_date) WHERE reason = 'EXCESSIVE_REJECTION';` (one day's rejections can only trip the
  rule once). The predicate that matters for this finding is **`uq_bl_one_per_day`'s own** — `WHERE reason
  = 'EXCESSIVE_REJECTION'`, not `WHERE is_active`. This index has no knowledge of *which* trigger path
  fired — it keys purely on `(driver_id, trigger_date)`, filtered only by `reason`. **Confirmed: it holds
  identically for both paths**, because both the F-13 path and the F-14 path execute the identical "count →
  compare to threshold → INSERT driver_blacklist" logic (always with `reason='EXCESSIVE_REJECTION'`) inside
  the same consumer group, and the second INSERT attempt (whichever path loses the race) collides on
  `uq_bl_one_per_day` and is rejected — exactly the behavior `05-consumers.md` §1.5 documents ("a duplicate
  delivery... re-attempts the insert, which the unique index harmlessly rejects").
  **This predicate is actually stronger than a `WHERE is_active` filter would have been**: because
  `uq_bl_one_per_day` carries no `is_active` condition, the one-trigger-per-day guarantee for a given
  `(driver_id, trigger_date)` pair holds **permanently**, even after that row's `is_active` flag is later
  flipped `FALSE` (e.g. by the blacklist-expiry path, F-15). Had the predicate instead been `WHERE
  is_active` — which it is not — a second same-day `EXCESSIVE_REJECTION` trigger arriving *after* the first
  row had already been deactivated would **not** collide with it (a deactivated row would fall outside a
  `WHERE is_active` partial index and the second INSERT would succeed), silently reopening the same-day
  double-trigger this rule exists to prevent. The actual `WHERE reason = 'EXCESSIVE_REJECTION'` predicate
  has no such hole.
- **What happens if the count is read at different times by the two paths?** This is the genuine subtlety.
  Say a driver has 4 rejections/expiries and the threshold is 5. A `RideOfferRejected` event and a
  `RideOfferExpired` event for two *different* offers both land in the consumer group's poll batch close
  together. Both handler invocations independently run `COUNT(*) FROM ride_offer WHERE driver_id=:id AND
  status IN ('REJECTED','EXPIRED') AND offered_at >= :today` (via the proposed internal API,
  `05-consumers.md` §12 item 3). If both counts are taken *after* both offers' status writes have already
  committed (which they have, since dispatch-service's `UPDATE ride_offer` commits synchronously before the
  Kafka fact is even published, per `06-producers.md` §3's transactional-boundary description), **both
  handler invocations will independently see count=5 and both will attempt the INSERT** — this is not a
  stale-read race, because the count is always read from dispatch-service's already-committed state, never
  from the event payload's own snapshot. The second INSERT attempt collides on `uq_bl_one_per_day` and is
  rejected. **No double-blacklist, no missed blacklist, and the "trigger_date/rejection_count/threshold
  snapshot" (`driver_blacklist.trigger_date`, `rejection_count`, `threshold` columns per `NewSchema.md`
  line ~505–520) reflects whichever path's INSERT won** — the loser's slightly different count (if the two
  reads happened to straddle a moment where the count changed) is simply discarded, which is the correct,
  intended behavior of "first valid trigger wins, idempotently."
- **Severity: this is NOT a defect** — `uq_bl_one_per_day`, under its verified `WHERE reason =
  'EXCESSIVE_REJECTION'` predicate (not the earlier misquoted `WHERE is_active`), genuinely settles it, and
  does so permanently per `(driver_id, trigger_date)` rather than only while the row stays active. Recorded
  here as a confirmed-safe finding, not a remediation item.

---

## 3. Message ordering issues

### 3.1 ORD-01 — What is and is not ordered, restated with consequences

Per `01-kafka-architecture.md` §5.2/§5.3 and `06-producers.md` §9, ordering is per-partition-key only, and
is now a **genuine, D10-mitigated guarantee** for the co-located keys (`offer`→`request_id`,
`booking`→`trip_id`) outside of a resharding window (§9.4 of Step 8). Concretely:

| Ordering need | Mechanism | Confirmed by |
|---|---|---|
| Sibling offer withdraw must not be seen "before" the winning accept | `request_id` partition key + D10 sharded claim | `06-producers.md` §3, §9.5 |
| Carpool seat-count sequence on one trip must stay in commit order | `trip_id` partition key + D10 sharded claim | `06-producers.md` §4, §9.5 |
| A driver's location pings must apply last-write-wins, not arrival-order | `driver_id` partition key, `occurredAt`-based conditional `UPDATE` in `driver-service.geo-index-maintenance.v1` | `05-consumers.md` §1.1 |
| `BookingConfirmed` must be seen before a later `BookingCancelled` for `dispatch-service.request-status-sync.v1`'s status-sync logic to make sense | `trip_id` partition key | `05-consumers.md` §3.3 |

### 3.2 Finding ORD-02 — the D10 resharding window degrades exactly the two ordering guarantees that matter most

- **What breaks:** during a rolling deploy or scale event on **dispatch-service's** or **trip-service's**
  outbox relay, two relay instances can briefly disagree about `:totalShards` (`06-producers.md` §9.4).
  If a single aggregate (one `request_id`'s `RideOffer` rows, or one `trip_id`'s `Booking` rows) has more
  than one unpublished outbox row at that exact instant, and the two rows are claimed by two instances each
  computing a different `:totalShards`, publish order for that aggregate's siblings can invert.
- **Exact trigger:** a rolling deploy of dispatch-service or trip-service, or a horizontal scale event on
  either, landing at the same moment a `service_request` is mid-dispatch-round (multiple `RideOffer` siblings
  still unpublished) or a carpool `trip` is mid-multi-booking (multiple `Booking` rows still unpublished).
- **Blast radius:** for dispatch-service — a consumer (`realtime-gateway.offer-countdown-fanout.v1`) could
  render a sibling's `RideOfferWithdrawn` before the winning `RideOfferAccepted` for the same `request_id`,
  producing a momentarily wrong countdown UI (self-correcting on the next event, since the UI state machine
  re-syncs). For trip-service — `dispatch-service.request-status-sync.v1` or
  `realtime-gateway.carpool-seat-fanout.v1` could see a `BookingCancelled` before the `BookingConfirmed` it
  logically follows, producing a transient inconsistent seat-count display. **No DB write is at risk** in
  either case — the underlying Postgres state (`ride_offer.status`, `trip.seats_booked`) is always correct
  the instant its own transaction commits; this is purely a Kafka-consumer-side transient display-ordering
  defect.
- **Current mitigation:** `06-producers.md` §9.4's stated bound — "the rollout duration of a single
  deploy/scale event — seconds to low minutes" — and the recommendation to deploy relay-instance-count
  changes during low-traffic windows.
- **Severity: LOW** — bounded window, self-correcting, no data-loss/data-corruption consequence, explicitly
  and honestly not overclaimed as zero by Step 8 itself.
- **RECOMMENDATION (requires approval):** none needed beyond what Step 8 already documents; optionally,
  add a metric `tutem_outbox_shard_reassignment_total{service}` incremented whenever `:totalShards` changes,
  so an operator can correlate a brief spate of "sibling out of order" client reports with a recent
  deploy/scale event rather than treating it as a mystery. This is an observability addition (§7 below), no
  schema change.
- **Needs §10 schema change?** No.

### 3.3 Finding ORD-03 — carpool `pickup_order` has no ordering guarantee at all, by design gap (restated, deepened)

- **What breaks:** `booking.pickup_order` (per baseline F-11 step 2 and `05-consumers.md` §7.5) is a
  **hint, not a key** — there is no database object enforcing it. Under D7, riders book a carpool `trip`
  directly and concurrently; each rider's booking transaction computes its own `pickup_order` (via
  `ST_LineLocatePoint` ranking, per baseline §4.12-B) independently of any other in-flight booking on the
  same trip.
- **Exact trigger:** two riders, R1 and R2, book seats on the same carpool trip within the same
  transaction window; both compute a `pickup_order` value based on their position along `trip.route_line`
  at the moment their own transaction runs. If R1 and R2's pickup points fall close enough along the
  corridor that the ranking computation (independent, not coordinated) resolves to the same integer rank,
  both bookings commit with **the same `pickup_order` value** — nothing in `NewSchema.md` prevents this,
  because (per `NewSchema.md` line 466) the only unique index on `booking` scoped to a trip is
  `uq_booking_exclusive_trip ON booking (trip_id) WHERE mode IN ('RIDE','WALK')`, which explicitly excludes
  `CARPOOL`.
- **Blast radius:** the multi-pickup sequencing UI/driver instructions (F-11 step 2: "driver drives to
  riders in `(pickup_order, confirmed_at)`") falls back to the `confirmed_at` tiebreak, which baseline
  itself calls "mandatory until §10 item 3's `uq_booking_pickup_order` exists." The consequence is degraded
  UX (a driver's suggested route order may not match true geographic proximity ordering) never a lost seat,
  never an oversold trip, never a lost booking. `05-consumers.md` §11 confirms
  `realtime-gateway.carpool-seat-fanout.v1` "propagates that field as-is to the client and must not treat it
  as unique."
- **Current mitigation:** the `confirmed_at` tiebreak (application-level convention, not a database
  constraint).
- **Severity: MEDIUM** — a real, live, unbackstopped correctness-of-*sequencing* (not of seats) gap that
  fires under realistic concurrent-booking load on any popular carpool corridor, not an edge case.
- **RECOMMENDATION (requires approval):** approve baseline §10 item 3 — `uq_booking_pickup_order` +
  `ck_booking_order_mode` (index/CHECK only, no new column, per baseline's own §10.1 summary: "the only
  *correctness* hole here," "cheapest item"). Until approved, this remains an accepted, explicitly-flagged
  gap, not silently assumed solved.
- **Needs §10 schema change? YES — baseline §10 item 3, currently UNAPPROVED.**

---

## 4. Consumer lag

### 4.1 LAG-01 — `tutem.driver.driver.location-updated.v1` is the highest-risk consumer group class

- **Why:** at 24 partitions and >80% of cluster traffic (1,250 of ~1,300–1,500 msg/s, `01-kafka-architecture.md`
  §2.1), this topic feeds two consumer groups: `driver-service.geo-index-maintenance.v1` and
  `realtime-gateway.driver-location-fanout.v1`. Both are recommended at 12–24 instances/threads
  (`05-consumers.md` §8) — already near the partition ceiling, leaving little headroom to add concurrency
  as a lag-mitigation lever without a partition-count increase (which, per `01-kafka-architecture.md` §5,
  "can only be increased, never decreased," and reshuffles co-located keys — though `driver_id` keying has
  no co-location requirement across drivers, so growing this topic's partitions is comparatively low-risk
  compared to `offer`/`booking`).
- **Effect of lag on the 20-second offer TTL:** **`driver-service.geo-index-maintenance.v1`'s lag has no
  direct effect on `dispatch.offer.ttl_seconds`** — the offer TTL is enforced entirely by
  `ride_offer.expires_at` in Postgres and the F-14 expiry sweep, neither of which reads the location topic.
  **However, lag on `driver-service.geo-index-maintenance.v1` directly stales `tutem:driver:geo:<active_mode>`**,
  which is what `POST /internal/v1/drivers/nearby` (§4.12-A) uses as its Redis-backed hot path — if this
  consumer group lags by, say, 30 seconds, a driver who went offline or moved out of radius 30 seconds ago
  may still be surfaced as a dispatch candidate, and a newly-online driver may not be surfaced for 30
  seconds. This degrades *candidate quality*, not the TTL mechanism itself, and PostGIS's `ix_driver_available`
  remains the synchronous fallback source of truth if Redis is skipped or flushed.
  **`realtime-gateway.driver-location-fanout.v1`'s lag directly degrades the rider's live-tracking freshness**
  — a lagging fan-out consumer means the rider's map shows a driver position that is stale by the lag
  amount, a direct, user-visible quality regression, though never a correctness break (no booking/payment
  data is at stake).
- **Concrete lag budgets per consumer class:**

  | Consumer class | Example groups | Lag budget | Rationale |
  |---|---|---|---|
  | **Latency-critical — user perceives it live** | `realtime-gateway.driver-location-fanout.v1`, `realtime-gateway.offer-countdown-fanout.v1` | **< 2 seconds** sustained; alert at >5s | Directly visible on-screen; offer countdown UI must track the real `dispatch.offer.ttl_seconds` (default 20s) closely enough that a rider/driver doesn't see a countdown that disagrees with the actual accept window |
  | **Correctness-adjacent, but DB is still authoritative** | `driver-service.geo-index-maintenance.v1`, `driver-service.blacklist-evaluation.v1`, `trip-service.trip-provisioning.v1` | **< 10 seconds** sustained; alert at >30s | Degrades candidate-pool freshness or delays trip creation notice, but the DB write (or the DB's own authoritative check) is unaffected — a rider still gets a ride, just possibly slower |
  | **Safe to lag materially** | `*.config-snapshot-refresh.v1` (×7), `trip-service.rider-directory-cache.v1`, `trip-service.vehicle-snapshot-cache.v1`, `trip-service.history-projection.v1` | **< 60 seconds** typical; alert at >5 minutes | All have a synchronous cold-fallback to the owning service's authoritative store (D9's cache-miss fallback pattern) or, for config, `tutem:config:snapshot`'s only consequence is "the tunable takes a bit longer to apply" |
  | **Push delivery — user annoyance, not data risk** | `notification-service.push-delivery.v1`, `notification-service.driver-welcome.v1` | **< 30 seconds** typical; alert at >2 minutes (tighter because retry-tier hops already add up to 35 min worst-case before DLQ) | A late push is a UX defect; the WebSocket path (D3) already carries the same fact in parallel, so FCM lag is not the sole delivery path |

- **Severity: MEDIUM** overall for LAG-01 — no data-loss risk (Kafka retains per §13.1's retention windows),
  but the F-06 topic's sheer volume share means it is the first place any broker/consumer capacity problem
  will surface, and its 24-hour retention (shortest in the system) means a sustained multi-hour lag risks
  **losing pings to retention before they're ever consumed** — see RPL-01 below for the interaction.
- **RECOMMENDATION (requires approval):** adopt the lag-budget table above as the SLO basis for §7's
  alerting plan (already partially proposed there); no design change is required beyond what
  `01-kafka-architecture.md` §5.1's "5–10× headroom" already provides. If sustained lag is observed in
  practice, the documented scale-out path (`01-kafka-architecture.md` §2.2) is to add brokers/partitions,
  not to change the partition-key/ordering model.
- **Needs §10 schema change?** No.

---

## 5. Single points of failure

| SPOF | What happens | Current mitigation | Severity | RECOMMENDATION (requires approval) |
|---|---|---|---|---|
| **Kafka — 2 of 3 brokers die** | Per `01-kafka-architecture.md` §12: any partition whose remaining replica set drops below `min.insync.replicas=2` **stops accepting writes**. The outbox row simply stays `published_at IS NULL`; the domain write already committed and is unaffected. **Every one of the 5 outbox relays across all services silently accumulates backlog** — this is a full-platform event-delivery stall, not a partial one, because every service's relay hits the same unavailable cluster. | RF=3, `min.insync.replicas=2`, rack-aware across 3 AZs; outbox pattern absorbs the stall without data loss. | **HIGH** for availability of *all* async fan-out (pushes, live tracking, history projection) simultaneously; **not a correctness risk** (no domain write is lost). | Accept as-is for v1 — this is the documented, deliberate trade of `min.insync.replicas=2` (`01-kafka-architecture.md` §2). No change recommended; flag in the monitoring plan (§7) as a platform-wide-outage-class alert, not a per-service one. |
| **Schema Registry unavailable** | **Producers:** cannot register/resolve a schema id for a *new* schema version, but a producer using an already-cached schema id for an unchanged schema can often continue (Avro client-side caching) — `01-kafka-architecture.md` §8/§4.2's error-handling rows (`06-producers.md` §1–§5) state the failure mode as "producer cannot resolve/register a schema id, the publish attempt fails, `published_at` stays `NULL`" for the case where no cached id exists. **Consumers:** cannot deserialize a payload for a schema id not already in their local cache — an uncached deserialization attempt fails, and per `01-kafka-architecture.md` §7.1 a deserialization failure is a **terminal (poison)** error, routed straight to `.dlq`, not retried. | Per-environment Schema Registry (not shared with other environments); Avro client-side schema-id caching reduces registry dependency for steady-state traffic. | **HIGH** — a sustained Schema Registry outage during a deploy that introduces a new schema version could route a burst of otherwise-valid records to `.dlq` as false poison-pills, and can stall every producer's *new*-schema publishes platform-wide. | **RECOMMENDATION:** document an explicit Schema Registry HA topology (the current documents state "one registry per environment" but not its own replication/backup story) and confirm client-side schema caching is enabled with a generous local cache to reduce registry read dependency at steady state. This is an operational/infra recommendation, not a schema or naming change. |
| **Redis — full flush or instance failure** | Per `01-kafka-architecture.md` §12: geo lookups fall back to direct PostGIS (`ix_driver_available`); config reads fall back to direct `system_config`; WebSocket routing rebuilds as clients reconnect; **idempotency de-dup keys are the one case with a real, bounded consequence** — this is exactly DUP-01's exposure. The 3 D9 read-model caches (`tutem:trip:rider-directory:<user_id>`, `tutem:trip:vehicle-snapshot:<vehicle_id>`, `tutem:trip:history-projection:<user_id>`) all degrade to synchronous fallback calls — see TH-02 below for the thundering-herd consequence of *all three* falling back simultaneously. | A-08: Redis is never the source of truth for any of the above. | **HIGH** for the thundering-herd interaction (TH-02); **MEDIUM** standalone (latency, not correctness). | See TH-02's recommendation. |
| **PostgreSQL** | Each of the 5 owning services' own schema becomes unavailable; the outbox pattern does not protect against this — a domain write and its outbox row are **the same transaction**, so a Postgres outage stalls the domain write itself (the caller sees a 5xx), not just the publish. This is a **synchronous-path SPOF**, not an async one — no Kafka mechanism mitigates it. | "One PostgreSQL instance, one schema per owning service" (baseline §2.0) — a single point of failure for **all 5** owning services simultaneously if the instance-level infrastructure fails, though each service's schema is logically isolated for blast-radius-of-a-bad-migration purposes. | **CRITICAL** for the whole platform if the single physical instance fails — every synchronous write (accept, seat reservation, OTP, payment) stops. | **RECOMMENDATION:** confirm (outside this document's scope, but flagged per baseline A-06/Q-12) whether the single-physical-instance decision has an HA replica/failover story (streaming replication + automated failover) — this is infrastructure, not a Kafka/schema decision, but it is the single largest correctness-path SPOF in the entire architecture and deserves explicit sign-off alongside this risk register. |
| **The outbox relay itself** | If a specific service's relay process dies (not the whole Postgres instance), that service's `outbox_event` backlog grows but its domain writes are unaffected — covered by `tutem_outbox_unpublished_records`/`tutem_outbox_publish_lag_seconds` (§14 of Step 3). Because the relay runs **inside the same deployable** (D2), a relay-only failure without a full pod crash is unlikely in practice (it is not a separately-deployed process that can fail independently of the service). | In-process `@Scheduled` task inside each of the 5 services; `SELECT ... FOR UPDATE SKIP LOCKED` lets multiple pod instances share the load. | **LOW** given multi-instance deployment. | None beyond existing monitoring (§7). |
| **api-gateway** | If api-gateway is down, no synchronous request reaches any service — this is the front door for every `[SYNC-CRITICAL]` step (accept, seat reservation, OTP, payment). Kafka/async flows already in flight (dispatch rounds, expiry sweeps) continue independently since they don't route through api-gateway. | Stateless, horizontally scaled, no tables owned (baseline §2.1). | **CRITICAL** for new requests; **no effect** on in-flight async processing. | Standard multi-instance/load-balanced deployment; no Kafka-specific recommendation. |
| **realtime-gateway WebSocket routing** | If `tutem:ops:ws-route:<userId>` or `tutem:ops:fanout:<tripId>` (Redis) is unavailable, cross-instance push handoff fails — "only sockets held by the consuming instance itself still receive the push" (`05-consumers.md` §7.1). Combined with a full Redis flush, this is also TH-01/TH-02 territory (reconnect storm). | FCM (D3) is the parallel, independent delivery path for every user-facing alert — a WebSocket routing failure degrades to FCM-only delivery, never zero delivery. | **MEDIUM** — degrades to FCM-only, not a total outage of user-facing alerts. | None beyond §7's monitoring; D3's dual-path design is already the mitigation. |
| **Parivahan SDK down** | `driver-service.parivahan-verification.v1` exhausts its 3 retry tiers (30s/5min/30min) and lands in `.dlq`; the driver stays `PENDING` indefinitely until redrive. | Retry tiers (D8-qualifying topic) + `.dlq` + manual redrive (`01-kafka-architecture.md` §7.4). | **MEDIUM** — blocks driver onboarding, not any live-trip flow. | None beyond existing DLQ/redrive; this is the documented, intended degradation path. |
| **FCM down** | Both `notification-service.driver-welcome.v1` and `notification-service.push-delivery.v1` exhaust retry tiers and DLQ. **Every push in the system routes through the single command topic `tutem.notification.send-push.command.v1`**, so an FCM outage affects all 12 producer call-sites at once. | WebSocket (D3) carries the same fact for foregrounded clients — FCM-only users (backgrounded) miss the alert until FCM recovers or a redrive fires. Retry tiers + `.dlq`. | **MEDIUM** — backgrounded-client alerts delayed platform-wide; foregrounded clients unaffected (WebSocket independent). | None beyond existing DLQ/redrive/D3 dual-path. |
| **Routing API down** | Synchronous calls only (F-07 step 2, F-10 step 2), never from a Kafka consumer (`02-kafka-topics.md` §3.1). Both call-sites have a documented straight-line (`ST_MakeLine`) fallback per `NewSchema.md`. | Fallback fare/route estimate, no Kafka involvement. | **LOW** — degrades estimate accuracy, never blocks the request. | None. |
| **Payment gateway down** | Synchronous calls only (F-16 steps 1–3), never from a Kafka consumer. `CASH` path is entirely unaffected. Non-cash bookings would see `BookingPaymentFailed` published from a synchronous callback failure, or the initiating call itself fails and the client retries. | `CASH` fallback always available; `booking.payment_status` CHECK-governed transitions (`ck_booking_paystat`) make a stuck `PENDING` state visible and queryable. | **MEDIUM** — blocks non-cash payment completion, not the ride itself (ride already happened). | None beyond existing design; outside Kafka's scope entirely. |
| **Object storage (DL/RC images) down** | F-03 step 2 (image upload) fails at the client-to-storage hop, before any service or topic is involved. `driver_document` registration (step 3) requires `image_url`, so document submission is blocked entirely during an outage. | None documented — this is a pure infrastructure dependency with no fallback named anywhere in Steps 2–8. | **MEDIUM** — blocks driver onboarding, no live-trip impact. | Outside Kafka's scope; flagged for completeness only. |

---

## 6. Replay problems

### 6.1 RPL-01 — Which topics are replay-safe, and the retention interaction

Per `01-kafka-architecture.md` §12 ("Replay/reprocessing safety... any topic can be replayed... with the
same correctness guarantee as first delivery... because every consumer is required to be idempotent"), the
**design's own claim is that replay is uniformly safe.** This section verifies that claim per consumer
class and states where retention makes replay **impossible in practice**, not merely risky.

| Consumer class | Replay-safe? | Why / why not |
|---|---|---|
| DB-unique-index-backstopped (`trip-service.trip-provisioning.v1`, blacklist INSERT path) | **Yes, genuinely** | Constraint violation on replay is caught and is a no-op |
| Conditional-`UPDATE`-backstopped (`dispatch-service.request-status-sync.v1`, `dispatch-service.user-deletion-cleanup.v1`, `driver-service.geo-index-maintenance.v1`) | **Yes, genuinely** | Re-applying to an already-transitioned row matches zero rows |
| Full-recompute consumers (rating averages, verification-status recompute) | **Yes, genuinely** | Recompute from current state, order-independent |
| **`driver-service.total-trips-increment.v1`** | **NO — would double-increment** | Replaying `tutem.trip.trip.completed.v1` from an earlier offset re-runs every `+1` a second time for every trip in the replayed range, because the Redis idempotency keys for old `eventId`s have almost certainly expired by the time anyone deliberately replays (TTL is "longer than the max retry/replay window," not infinite) — **a deliberate historical replay is exactly the scenario the Redis TTL was not designed to survive.** This directly contradicts §12's blanket claim for this one consumer and must be called out as the exception. |
| **`notification-service.push-delivery.v1`, `notification-service.driver-welcome.v1`** | **Would double-notify, but accepted** | A replay resends every push in the replayed range — annoying, not incorrect, per the design's own "UX nuisance, not correctness bug" framing. Technically "not replay-safe" in the naive sense, but explicitly and correctly accepted as low-severity. |
| Redis-cache-only consumers (`*.config-snapshot-refresh.v1`, `trip-service.rider-directory-cache.v1`, `.vehicle-snapshot-cache.v1`, `.history-projection.v1`) | **Yes, genuinely** | Keyed upsert is idempotent regardless of how many times or in what order it is replayed |
| `realtime-gateway.*` fan-out consumers | **Yes, functionally** | A replay re-sends already-seen WebSocket pushes; client-side `eventId` de-dup absorbs it. Replaying old location pings, however, would visibly move a driver's pin backward on a rider's map if `occurredAt` comparison is bypassed — the design's own `occurredAt`-based staleness check (not `eventId` de-dup) is what actually protects this, and it does hold under replay. |

**Retention interaction — replay is impossible, not merely risky, for some windows:**

- `tutem.driver.driver.location-updated.v1`: **24-hour retention**. Any replay attempt beyond 24 hours in
  the past is **physically impossible** — the records are gone. This is by design (baseline explicitly
  excludes a GPS breadcrumb table), but it means "replay this topic from offset 0" is only ever a same-day
  operation.
- All other domain-fact `delete`-policy topics: **7-day retention**. A replay beyond 7 days is impossible.
- `tutem.notification.send-push.command.v1`: **3-day retention**. Impossible beyond 3 days.
- `tutem.config.config.changed.v1`: **compacted**, so "replay from offset 0" always works and always lands
  on the current per-key snapshot — this is the one topic where replay-from-beginning is not time-bounded.
- DLQ topics: **30-day retention** — the longest window in the system, deliberately, for investigation.

**Finding RPL-01.** 
- **Severity: MEDIUM.** The one genuine correctness exception (`total-trips-increment`) is the same
  underlying issue as DUP-01, just triggered by a deliberate operational replay instead of an accidental
  redelivery — same root cause, same remediation options.
- **RECOMMENDATION (requires approval):** (1) State explicitly, as a correction to `01-kafka-architecture.md`
  §12's blanket claim, that `driver-service.total-trips-increment.v1` is **not** safe to replay past its
  Redis idempotency TTL, and that a deliberate replay of `tutem.trip.trip.completed.v1` requires either (a)
  fixing the increment per DUP-01's recommendations first, or (b) a documented manual reconciliation
  (`driver.total_trips` recompute from `COUNT(*) FROM trip WHERE ... status='COMPLETED'`) immediately after
  any deliberate replay of this topic. (2) Publish the **safe replay procedure per consumer class** as: DB-
  backstopped and cache-upsert consumers → replay freely; notification consumers → replay only with
  awareness of duplicate pushes; `total-trips-increment` → do not replay without the DUP-01 fix or a
  post-replay reconciliation.
- **Needs §10 schema change?** No.

---

## 7. Retry loops

### 7.1 RTL-01 — Can a record ping-pong forever?

- **D8's policy** (`01-kafka-architecture.md` §7.0–§7.2, `02-kafka-topics.md` §3): only 3 of 39 topics
  qualify for `.retry-1/2/3` (Parivahan/FCM failure surface); the other 36 retry in-process, bounded, then
  go straight to `.dlq`. **Max attempts: 3, then `.dlq`** — this is a hard, stated cap for both classes.
- **Verified: does it actually terminate in all cases?**
  - **Qualifying topics:** each hop (`.retry-1` at 30s, `.retry-2` at 5min, `.retry-3` at 30min) is a
    **distinct topic**, consumed by a **separate delayed consumer**; `x-retry-count` (a DLQ-family header
    per §0.3) increments on each hop. After `.retry-3` fails, the record goes to `.dlq` — there is no
    documented path that routes a `.dlq`-bound record back to `.retry-1` or to the original topic
    automatically. **Confirmed: terminates in 3 attempts, always, for the qualifying topics.**
  - **Non-qualifying topics:** the in-process bounded retry has "the same backoff shape, no separate hop" —
    i.e., 3 attempts within the same consumer invocation (or a small number of poll cycles), then straight
    to `.dlq`. **Confirmed: terminates**, and moreover terminates *faster* than the qualifying-topic path
    because there is no cross-topic hop latency.
- **What about a DLQ redrive that fails again?** `01-kafka-architecture.md` §7.4 states redrive republishes
  to the **original topic** with the original `eventId`. If the underlying cause was not actually fixed,
  the record fails again through the **same** retry path (in-process or retry-tier) and lands back in the
  **same** `.dlq` a second time. **This is not an infinite loop** — each redrive is a manually-triggered,
  finite event, and each pass through retries is capped at 3 attempts — but it is an **operational loop**
  if an operator redrives without actually fixing the cause, and nothing in the design detects "this
  `eventId` has already been redriven N times." 
  - **Finding RTL-02 — no redrive-count tracking.**
    - **What breaks:** an operator could redrive the same poison record repeatedly (e.g. a scripted,
      unattended redrive tool naively re-running on a schedule) without the record ever surfacing as
      "chronically failing," because `x-retry-count` is reset to 0 on a fresh publish to the original topic
      (redrive publishes the original key/value, and `x-` headers are DLQ-transport metadata added only at
      DLQ time by the *failing* consumer, not carried forward as a persistent redrive counter).
    - **Trigger:** repeated automated or careless manual redrive of a record whose root cause was not fixed.
    - **Blast radius:** wasted consumer/broker cycles, delayed operator attention to the real cause; no
      data corruption (idempotent consumers still apply).
    - **Current mitigation:** `01-kafka-architecture.md` §7.4 item 5 — "Redrive is always **manual-trigger,
      batch-scoped**... never an automatic 'replay everything in the DLQ on a timer.'" This is a **process**
      mitigation, not a **technical** one.
    - **Severity: LOW.**
    - **RECOMMENDATION (requires approval):** add a `redriveCount`/`lastRedrivenAt` annotation persisted
      alongside the DLQ tooling's own state (not a Kafka header, since redrive must forward the original
      record unmodified per §0.3) so a redrive tool can refuse or warn on redriving the same `eventId` more
      than N times without a human override. Tooling-level, not a topic/schema change.
    - **Needs §10 schema change?** No.
- **Poison record that fails deserialization before any handler runs:** per `01-kafka-architecture.md` §7.1,
  this is explicitly categorized as **terminal**, routed **directly to `.dlq`**, never through retry tiers
  or in-process retry — "retrying a deterministic failure... only wastes capacity and delays operator
  visibility." **Confirmed: this terminates in exactly one attempt**, the fastest-terminating case in the
  whole retry design. See §9 (Poison Pill) below for the detailed scenario.

---

## 8. Deadlocks

### 8.1 DLK-01 — Seat-reservation transaction vs. outbox insert vs. relay's `FOR UPDATE SKIP LOCKED` claim

- **Lock-ordering analysis:**
  - **Seat-reservation transaction** (trip-service, F-10 step 8): acquires a row lock on `trip` (via the
    `UPDATE trip SET seats_booked = ...` or an explicit `SELECT ... FOR UPDATE`, per RC-04's recommendation)
    and inserts into `booking` and `outbox_event`. `outbox_event` is an **INSERT**, which never blocks on
    an existing row lock (only `UPDATE`/`DELETE`/explicit-lock statements contend for row locks; a fresh
    `INSERT` only needs a new row, contending at most on unique-index insertion, not on another
    transaction's already-locked row). **No lock-ordering conflict is possible between the `trip` UPDATE and
    the `outbox_event` INSERT within the same transaction**, because they lock disjoint rows in disjoint
    tables and the INSERT never needs to wait on the UPDATE's lock.
  - **The relay's claim query** (`06-producers.md` §8/§9.2): `SELECT ... FROM outbox_event WHERE
    published_at IS NULL AND ... FOR UPDATE SKIP LOCKED`. This locks **only `outbox_event` rows already
    committed** (the domain transaction has already committed by the time the relay's next poll runs, per
    the outbox pattern's sequential design) — the relay never contends with an in-flight domain transaction
    for the same row, because it only ever selects `published_at IS NULL` rows that already exist (i.e.,
    already committed by a prior, finished transaction). **`SKIP LOCKED` further guarantees the relay never
    blocks waiting on another relay instance's claim** — it simply skips a row another instance is
    currently holding.
  - **Conclusion: no deadlock is possible between the seat-reservation transaction and the relay's claim**,
    because they never lock the same row at overlapping times — the domain transaction's `outbox_event`
    INSERT and the relay's later `SELECT ... FOR UPDATE SKIP LOCKED` on that same (now-committed) row are
    strictly sequential (insert-then-commit, then, only afterward, select-and-claim), not concurrent.
- **Any consumer that writes multiple tables in one transaction?** Checked against `05-consumers.md` §1–§7:
  - `trip-service.trip-provisioning.v1`: writes `trip` INSERT + `booking` INSERT + `outbox_event` INSERT(s)
    — all in trip-service's own schema, all INSERTs (a fresh `trip_id`/`booking_id` per invocation), no
    `UPDATE` against a pre-existing row shared with any other transaction. **No deadlock risk**: two
    concurrent invocations of this consumer (different partitions, different offers) target disjoint new
    rows.
  - `driver-service.blacklist-evaluation.v1`: writes `driver_blacklist` INSERT + `driver.blacklisted_until`/
    `is_online=FALSE` UPDATE, **both against the same driver's data**, in one transaction. Two concurrent
    invocations of this same consumer group for the **same** `driverId` (one from the F-13 rejection path,
    one from the F-14 expiry path, both landing in the same consumer group per RC-05) would both attempt to
    `UPDATE driver ... WHERE id=:driverId` — Postgres serializes these via the row lock on `driver`, one
    transaction waits for the other's commit, **no deadlock** (a wait is not a deadlock; a deadlock requires
    a *cycle* of waits, and here both transactions want the same single row in the same order — `driver`
    first is not even required, since `driver_blacklist` is a fresh INSERT with no shared lock target).
  - **No consumer in the catalogue acquires locks on two or more pre-existing rows in an order that could
    invert between two concurrent transactions** — every multi-table write documented in `05-consumers.md`
    is either (a) all-INSERT (no lock ordering issue) or (b) a single `UPDATE` on one pre-existing row plus
    INSERTs of fresh rows (b's INSERT never contends for a lock the UPDATE holds, since they're different
    tables/rows). **No deadlock scenario is identified across any of the 30 consumer groups as documented.**
- **Explicit lock-ordering rule to state as a RECOMMENDATION for future extensions (requires approval,
  documentation-only):** any *future* consumer or flow that needs to lock two or more pre-existing rows in
  one transaction (e.g., a hypothetical future consumer touching both `trip` and `driver` in one
  transaction) must acquire locks in a **fixed, globally-agreed order** — e.g., always by ascending table
  name, then by primary key — to prevent a genuine deadlock between two such consumers running concurrently
  in opposite orders. No current consumer needs this rule, but it should be written down before one does.
- **Severity: LOW — this is a confirmed-safe finding for the current design, with a forward-looking
  documentation recommendation.**
- **Needs §10 schema change?** No.

---

## 9. Thundering herd / cold-start

### 9.1 TH-01 — 50k users reconnecting after an outage: WebSocket re-establishment

- **What breaks:** `01-kafka-architecture.md` §10 describes realtime-gateway's routing as
  `tutem:ops:ws-route:<userId>` lookups plus `tutem:ops:fanout:<tripId>` pub/sub, both **ephemeral by
  design** (§0.8: "Rebuilt as clients reconnect"). If realtime-gateway itself, or the underlying Redis
  instance backing these two keys, goes down and recovers, **all 50,000 concurrent users' sockets drop
  simultaneously and every client's reconnect logic fires at once** — a classic thundering herd against
  api-gateway's auth path and realtime-gateway's own connection-accept path.
- **Exact trigger:** realtime-gateway fleet restart (deploy, or recovery from an outage), or a Redis
  failover that invalidates `tutem:ops:ws-route:*`/`tutem:ops:presence:*` en masse.
- **Blast radius:** every connected client (up to the full 50k concurrent target) attempts to
  re-authenticate (api-gateway) and re-establish a socket (realtime-gateway) within a short window — a
  spike in both services' request rate, and a spike in `tutem:ops:ws-route:<userId>` writes as each
  reconnecting socket re-registers.
- **Current mitigation:** none explicitly documented in Steps 2–8 beyond realtime-gateway's own horizontal
  scaling on socket count (§10 of Step 3).
- **Severity: HIGH** at the stated 50k-concurrent-user target — this is exactly the scenario baseline's own
  scale reading flags ("a connection-count and fan-out problem for realtime-gateway... not a Kafka
  throughput problem," `01-kafka-architecture.md` §2.1) but no explicit reconnect-storm mitigation (jittered
  backoff, staggered reconnect) is named anywhere in Steps 2–8.
- **RECOMMENDATION (requires approval):** specify client-side reconnect jitter/exponential backoff (a
  Flutter-client behavior, not a backend one) and confirm api-gateway's rate-limiting
  (`tutem:ops:ratelimit:<scope>:<subject>`) is tuned to tolerate a reconnect burst without throttling
  legitimate reconnects as abuse. This is a client-behavior and infra-tuning recommendation, not a
  schema/topic change.
- **Needs §10 schema change?** No.

### 9.2 TH-02 — Redis flush cascades across the D9 caches AND the geo index simultaneously

- **What breaks:** a single full Redis flush or failover invalidates, all at once: `tutem:driver:geo:<active_mode>`
  (falls back to `ix_driver_available` PostGIS scan), `tutem:trip:rider-directory:<user_id>` (falls back to
  identity-service's public-profile sync API), `tutem:trip:vehicle-snapshot:<vehicle_id>` (falls back to
  driver-service's vehicle-snapshot endpoint), `tutem:trip:history-projection:<user_id>` (falls back to a
  sync read across `booking`/`service_request`/`trip`, including a **cross-service call to dispatch-service**
  since trip-service does not own `service_request`), `tutem:notification:tokens:<userId>` (push sends fail
  until repopulated), and `tutem:ops:idem:*` (DUP-01's exposure window opens for every consumer
  simultaneously, not just `total-trips-increment`).
- **Exact trigger:** Redis instance failure/failover, or an operational full flush.
- **Blast radius:** every one of these fallbacks is a **synchronous call to an owning service**
  (identity-service, driver-service, dispatch-service) — at 50k concurrent users, a simultaneous cache-miss
  storm across rider-directory, vehicle-snapshot, and history-projection lookups could produce a genuine
  **synchronous-load spike on identity-service and driver-service** that neither of those services'
  documented sizing (`01-kafka-architecture.md` §2.1, which sizes *Kafka* throughput, not synchronous
  API load) accounts for. This is the sharpest thundering-herd risk in the design: **three independently
  small "degrades to a live call" decisions (D9) compose into one large synchronous-load spike when their
  common dependency (Redis) fails as a single event.**
- **Current mitigation:** each fallback individually documented as "latency cost, not correctness break"
  (`05-consumers.md` §4.1/§4.2/§4.4) — but no document addresses the **combined** load when all three (plus
  the geo index, plus notification tokens, plus every consumer's idempotency check) fail over at once.
- **Severity: HIGH.** Individually LOW-severity fallbacks compose into a genuine capacity risk at scale;
  this is exactly the kind of cross-cutting risk a per-consumer document cannot catch, which is why it is
  surfaced here.
- **RECOMMENDATION (requires approval):** (1) Add rate-limiting or request-coalescing on the fallback path
  itself (e.g., a short-lived local in-process cache or a single-flight pattern so N simultaneous cache
  misses for the same key collapse into 1 upstream call, not N) at identity-service's and driver-service's
  public-profile/vehicle-snapshot endpoints specifically, since those are the two most likely to receive a
  correlated miss storm. (2) Stagger cache-repopulation via the normal Kafka-consumer replay path (which
  already exists — `trip-service.rider-directory-cache.v1` etc. will naturally repopulate as new events
  flow) rather than attempting a bulk cache-warm on Redis recovery, to avoid a second self-inflicted spike.
  (3) Size identity-service/driver-service's synchronous API capacity with this compound cache-flush
  scenario explicitly in mind, not just steady-state traffic. All three are capacity/implementation
  recommendations, not schema or naming changes.
- **Needs §10 schema change?** No.

### 9.3 TH-03 — Offer TTL expiry storms

- **What breaks:** `dispatch.offer.ttl_seconds` (default 20s, per `system_config`) means every dispatch
  round's offers expire in a tight cluster ~20 seconds after creation. At peak (baseline's own F-07 sizing:
  3,750 `ride_offer` rows/peak minute, `01-kafka-architecture.md` §2.1), the F-14 expiry sweep processing
  a batch of near-simultaneously-expiring offers produces a **burst** of `RideOfferExpired` facts (and,
  since D4, blacklist-evaluation triggers) concentrated in a narrow time window following each dispatch
  round, rather than being smoothly distributed.
- **Exact trigger:** any dispatch round with a low accept rate (e.g., during a driver shortage), producing
  many simultaneous expiries ~20s later.
- **Blast radius:** a burst on `tutem.dispatch.offer.expired.v1` → `driver-service.blacklist-evaluation.v1`
  (the count/insert path) and `realtime-gateway.offer-countdown-fanout.v1`. Bounded by
  `dispatch.max_drivers_per_round` (default 5) × the concurrent-search estimate (500 at peak,
  `01-kafka-architecture.md` §2.1) — i.e., at most ~2,500 offers could theoretically expire within the same
  few-second window, well within the 12-partition topic's documented headroom.
- **Current mitigation:** partition count (12) sized for burstiness explicitly (`01-kafka-architecture.md`
  §5.1: "Bursty, not sustained-high; 12 gives enough parallelism").
- **Severity: LOW** — already accounted for in the partition-sizing rationale; no new exposure identified
  beyond what §5.1 already budgets for.
- **RECOMMENDATION:** none beyond confirming the existing headroom remains valid if driver-shortage
  scenarios (which maximize simultaneous expiry) become materially more frequent than modeled.
- **Needs §10 schema change?** No.

---

## 10. Poison pill

### 10.1 PP-01 — Deserialization failure before the handler runs

- **Scenario:** a record's Avro payload cannot be deserialized against any schema id the consumer's client
  has or can fetch (e.g., a corrupted record, or a schema id referencing a deleted/incompatible schema).
- **Treatment (confirmed from `01-kafka-architecture.md` §7.1):** categorized **terminal**, routed
  **directly to `.dlq`**, bypassing both in-process retry and retry tiers entirely — this is correct,
  because a deserialization failure is deterministic (the same bytes will always fail the same way) and
  retrying wastes capacity per §7.0's own reasoning.
- **Consequence for the poll loop:** because manual offset commit only happens after a successful handler
  invocation (or an explicit DLQ-routing decision), a deserialization failure must be caught **before**
  handler dispatch and routed to `.dlq` **without advancing past it silently** — the record's offset is
  still committed once the `.dlq` publish succeeds (this is the correct behavior: the poison record is
  "handled" by being quarantined, so the consumer must move on to the next offset, or the entire partition
  stalls behind one unprocessable record forever). **This mechanic is implied by the design but not
  explicitly spelled out in `05-consumers.md`'s per-consumer failure-handling rows**, which mostly describe
  "poison record → `.dlq` directly" without stating explicitly that offset commit still proceeds.
- **Finding PP-02 — offset-commit-after-DLQ-publish is implied, not stated.**
  - **Severity: LOW** (this is almost certainly the intended behavior given the whole architecture depends
    on partitions never stalling behind one bad record, per `01-kafka-architecture.md` §12's "non-blocking
    retry topics... mean a poison record never stalls its siblings").
  - **RECOMMENDATION (requires approval):** add one explicit sentence to the consumer design confirming: "a
    record routed to `.dlq` has its offset committed immediately after the successful `.dlq` publish,
    exactly as if it had been successfully processed — a `.dlq` publish failure itself is treated as a
    transient infrastructure error (retried) rather than leaving the offset uncommitted indefinitely."
    Documentation-only.
  - **Needs §10 schema change?** No.

### 10.2 PP-03 — Avro schema incompatibility at runtime

- **Scenario:** a producer publishes with a schema version a given consumer's cached reader schema cannot
  read under `BACKWARD` compatibility (should be prevented at registration time per `01-kafka-architecture.md`
  §8, but a registry misconfiguration or a manual bypass could still produce this).
- **Treatment:** same as PP-01 — a deserialization/compatibility failure is a **terminal** error, routed to
  `.dlq`. The Schema Registry's `BACKWARD` enforcement at registration time is the **preventive** control;
  `.dlq` is the **detective/containment** control for the case prevention fails.
- **Severity: LOW** given the registry-side enforcement; the residual risk is a registry
  misconfiguration, which is an operational/access-control concern, not a design gap.
- **RECOMMENDATION:** none beyond confirming registry write-access (who can register a schema) is
  restricted to CI/CD, not ad-hoc — an ACL/process concern, not a schema change.
- **Needs §10 schema change?** No.

### 10.3 PP-04 — An event payload references a row that no longer exists

- **Scenario:** a consumer's handler dereferences an `aggregateId`/foreign identifier (e.g. a `driverId` in
  `RideOfferRejected`) against a row that has since been hard-deleted — except **baseline's soft-delete-only
  policy with `ON DELETE RESTRICT` on history FKs means no row referenced by any event payload in this
  system can ever be hard-deleted** (`app_user.status='DELETED'`, never a `DELETE FROM app_user`; the same
  for `driver`, `trip`, `booking`, `service_request`). **This class of poison record should not be able to
  occur in Tutem's design**, given the "full history per user" invariant.
- **Where it could still occur despite that:** a **race between event order and read timing** — e.g., a
  consumer processing a stale/very-delayed `RideOfferCreated` fact (after retention-adjacent lag) for a
  `driverId` that, while never hard-deleted, has had its `driver` row's relevant fields changed
  incompatibly with the event's assumptions (not a missing row, a *stale* one). This is better classified
  as a **stale-read** issue than a true poison pill, and is exactly what per-aggregate ordering (§3 above)
  and idempotent, current-state-reading consumers (verification-status-recompute, rating-average-recompute)
  are designed to tolerate — a re-derivation from current state self-corrects.
- **Genuine gap:** `01-kafka-architecture.md` §7.1 classifies "a business-rule violation that will never
  resolve by retrying (e.g. a referenced `driver_id` that genuinely does not exist)" as **terminal**, routed
  to `.dlq` — this is the correct treatment *if* it ever happens, even though the soft-delete policy makes
  it structurally rare. No change needed; the categorization already covers this correctly as a defensive
  measure even though the schema design should prevent the underlying cause.
- **Severity: LOW.** Structurally prevented by the soft-delete + `ON DELETE RESTRICT` design; already
  correctly categorized as terminal/DLQ if it somehow occurs anyway.
- **Needs §10 schema change?** No.

---

## 11. Two open internal-API hops — synchronous coupling on otherwise event-driven paths

Both are carried forward from `05-consumers.md` §12 items 3–4 and deepened here per the failure-behavior
question this step must answer.

### 11.1 Blacklist-evaluation → dispatch-service rejection-count

- **Coupling:** `driver-service.blacklist-evaluation.v1` (`05-consumers.md` §1.5) must obtain the
  `COUNT(*) FROM ride_offer WHERE ... status IN ('REJECTED','EXPIRED')` figure from dispatch-service (which
  exclusively owns `ride_offer`, per baseline §2.0) via a **proposed, not-yet-approved** internal endpoint
  `GET /internal/v1/offers/rejection-count`.
- **Failure behavior when dispatch-service is down:** `05-consumers.md` §1.5 states this is treated as "a
  retryable transient failure (in-process backoff), escalating to `.dlq` only after exhaustion." **Concrete
  consequence: while dispatch-service is unreachable, blacklist evaluation for every driver stalls** — the
  consumer group's lag grows uniformly across both source topics (`tutem.dispatch.offer.rejected.v1`,
  `tutem.dispatch.offer.expired.v1`) until dispatch-service recovers or the in-process retry exhausts and
  the record lands in `.dlq` (at which point that specific evidence event is **not lost** — it sits in
  `.dlq` for redrive — but the driver in question is not blacklisted promptly, a **fail-open** consequence
  for a compliance-adjacent rule). **This is the more concerning of the two hops**, because a driver who
  should be blacklisted continues receiving offers during the outage window.
- **Severity: MEDIUM** — fail-open on a compliance rule during a dependency outage, bounded by the outage
  duration and by `.dlq`'s eventual redrive catching up.
- **RECOMMENDATION (requires approval):** approve the proposed `GET /internal/v1/offers/rejection-count`
  endpoint shape (already specified in `05-consumers.md` §12 item 3) so this ceases to be an unnamed gap;
  separately, consider whether a **fail-closed** posture (temporarily suspend the driver's eligibility for
  new offers whenever this dependency is unreachable, rather than fail-open) is the correct default —
  this is a product/policy decision, not a Kafka mechanism, and is flagged here for Orchestrator judgment
  rather than decided.
- **Needs §10 schema change?** No — this is an API-contract approval, not a schema change.

### 11.2 Carpool-matching → trip-service `route_line` read

- **Coupling:** `dispatch-service.carpool-matching.v1` (`05-consumers.md` §3.2) reads `trip.route_line` via
  an internal-API hop to trip-service (trip-service owns `trip`, per baseline §2.0/Q-20); **no concrete
  endpoint name/shape has been proposed anywhere in Steps 2–8** (`05-consumers.md` §12 item 4 — the more
  open of the two gaps).
- **Failure behavior when trip-service is down:** same in-process-retry-then-`.dlq` treatment as §11.1.
  **Concrete consequence: while trip-service is unreachable, no new carpool trip can begin its
  corridor-matching computation** — `TripCreated` events accumulate lag in this specific consumer group,
  delaying rider notification for legitimately matching carpool requests. Once trip-service recovers, the
  backlog is processed normally (Kafka retains the `TripCreated` facts per the 7-day retention window), so
  this is a **latency** failure, not a **lost-match** failure — no carpool trip's matching window is
  time-boxed in a way that a delay would silently drop a match (there is no `expires_at` on the carpool
  `trip` row equivalent to `ride_offer.expires_at`/`service_request.expires_at`).
- **Severity: MEDIUM** — delays carpool matching platform-wide during a trip-service outage, no data loss.
- **RECOMMENDATION (requires approval):** name and approve a concrete internal endpoint (e.g.
  `GET /internal/v1/trips/{tripId}/route` returning `{routeLine, originPoint, destinationPoint}`),
  consistent with the existing `POST /internal/v1/drivers/nearby` convention — this closes the "unnamed
  gap" `05-consumers.md` §12 item 4 flags, and is purely an API-naming/approval action, not a schema change.
- **Needs §10 schema change?** No.

---

## 12. Monitoring & alerting plan (REQUIRED deliverable of this step)

Per `01-kafka-architecture.md` §14, thresholds/severities/SLOs/dashboards/runbooks were explicitly deferred
to Step 9. This section is that deliverable. All metric names follow §0.11's `tutem_<subject>_<unit>`
grammar with the bounded label set (`service`, `env`, `instance`, plus `topic`/`consumer_group`/
`event_type`/`outcome`/`mode` where applicable) — no new label is introduced beyond that set.

### 12.1 Metrics that matter

| Metric (already named in `01-kafka-architecture.md` §14 unless marked NEW) | What it tells an operator |
|---|---|
| `tutem_kafka_consumer_lag_records` | Per-group, per-partition lag — the primary lag signal (§4 above) |
| `tutem_kafka_dlq_published_total` | DLQ arrival rate — poison-record or systemic-outage indicator |
| `tutem_outbox_unpublished_records` / `tutem_outbox_publish_lag_seconds` | Outbox relay health — "the single most important new metric" per Step 3 |
| `tutem_kafka_e2e_latency_seconds` | `occurredAt` → consumer-completion latency — the realtime-promise metric |
| `tutem_kafka_consumed_total` (label `outcome`) | Throughput and, via `skipped-duplicate`, how often idempotent de-dup actually fires |
| `tutem_kafka_broker_under_replicated_partitions` / `tutem_kafka_controller_active_count` | Cluster-health basics |
| **NEW** `tutem_dlq_redrive_total` (labels: `topic`, `outcome`=success/failed-again) | Redrive rate and success — feeds RTL-02's "chronic redrive" detection |
| **NEW** `tutem_outbox_shard_reassignment_total` (label: `service`) | Correlates ORD-02's transient sibling-ordering reports with a recent relay scale/deploy event |
| **NEW** `tutem_idempotency_cache_miss_total` (labels: `consumer_group`, per §0.8's `tutem:ops:idem:*` keyspace) | Direct measurement of how often the Redis-only backstop (DUP-01, RPL-01) is actually being relied upon vs. a DB backstop catching the duplicate first |
| **NEW** `tutem_redis_fallback_total` (labels: `service`, `cache`=`rider-directory`\|`vehicle-snapshot`\|`history-projection`\|`geo`) | Direct measurement of TH-02's compound cache-miss load on identity-service/driver-service |

### 12.2 Concrete alert thresholds and severities

| Alert | Threshold | Severity |
|---|---|---|
| Consumer lag — latency-critical class (§4 table) | `tutem_kafka_consumer_lag_records` > 5s sustained for 1 min, for `realtime-gateway.driver-location-fanout.v1` or `.offer-countdown-fanout.v1` | **P1 — page** |
| Consumer lag — correctness-adjacent class | > 30s sustained for 2 min, for `driver-service.geo-index-maintenance.v1`, `driver-service.blacklist-evaluation.v1`, `trip-service.trip-provisioning.v1` | **P2 — page during business hours, ticket otherwise** |
| Consumer lag — safe-to-lag class | > 5 min sustained for 10 min | **P3 — ticket** |
| DLQ depth spike | `tutem_kafka_dlq_published_total` rate > 10/min sustained for 5 min on any topic | **P2** |
| DLQ depth spike, retry-tier topics specifically | any sustained rate on `tutem.notification.send-push.command.v1.dlq` for > 15 min | **P1** (this is the highest-blast-radius DLQ per `05-consumers.md` §5.2's own note) |
| Outbox publish lag | `tutem_outbox_publish_lag_seconds` > 60s for any service | **P2** |
| Outbox publish lag, driver-service specifically | > 30s (tighter, given F-06's 24h retention and >80% volume share) | **P1** |
| Broker under-replication | `tutem_kafka_broker_under_replicated_partitions` > 0 for > 2 min | **P1** |
| Idempotency-cache-miss rate spike (DUP-01/TH-02 early-warning) | `tutem_idempotency_cache_miss_total` for `driver-service.total-trips-increment.v1` rate suddenly > baseline by 10x | **P2 — investigate Redis health, correlates with a possible flush/failover** |
| Redis fallback rate spike (TH-02 early-warning) | `tutem_redis_fallback_total` summed across the 3 D9 caches + geo > 10x baseline for > 1 min | **P1 — likely Redis outage, watch identity-service/driver-service synchronous load** |
| Redrive without effect (RTL-02) | `tutem_dlq_redrive_total{outcome="failed-again"}` for the same topic > 3 times in 24h | **P3 — ticket, root cause likely not actually fixed** |

### 12.3 SLOs per critical path

| Critical path | SLO |
|---|---|
| **Offer delivery within the 20s TTL** (`dispatch.offer.ttl_seconds`) | 99% of `RideOfferCreated` → client-visible push (either WebSocket or FCM) within 3 seconds of `occurredAt`, measured via `tutem_kafka_e2e_latency_seconds{topic="tutem.dispatch.offer.created.v1"}` — chosen so the full 20s countdown is materially usable, not consumed by delivery latency |
| **Location freshness** | 99% of `tutem.driver.driver.location-updated.v1` facts visible on a watching rider's map within 2 seconds of `occurredAt`, measured via `tutem_kafka_e2e_latency_seconds{topic="tutem.driver.driver.location-updated.v1", event_type="DriverLocationUpdated"}` on the `realtime-gateway.driver-location-fanout.v1` consumer |
| **Payment settlement** | 99.5% of `BookingPaid`/`BookingPaymentFailed`/`BookingRefunded` facts reach `realtime-gateway.payment-status-fanout.v1` within 10 seconds of the underlying gateway callback's commit — looser than the two above because payment status is not a live-countdown UI element |
| **Blacklist evaluation freshness** | 95% of `RideOfferRejected`/`RideOfferExpired` facts result in a `driver_blacklist` INSERT decision (blacklist or no-op) within 30 seconds — looser, since this is a compliance control, not a live UX element, but bounded because of §11.1's fail-open consequence |

### 12.4 Dashboards

1. **Platform health** — per-consumer-group lag heatmap (all 30 groups), DLQ depth per topic, outbox
   publish lag per service, broker under-replication.
2. **F-06 dominance dashboard** — dedicated view for `tutem.driver.driver.location-updated.v1` given its
   >80% traffic share: throughput, both consumer groups' lag side by side, Redis geo-write success rate.
3. **Accept-race / dispatch dashboard** — `RideOfferCreated`/`Accepted`/`Withdrawn`/`Rejected`/`Expired`
   rates, blacklist-evaluation lag, the two internal-API hops' (§11) latency/error rate.
4. **Carpool dashboard** — `TripCreated`/`BookingConfirmed`/`TripSeatsExhausted` rates, carpool-matching
   consumer lag, `pickup_order` collision rate (a derivable metric: count of bookings per trip sharing a
   `pickup_order` value — direct observability into ORD-03 until §10 item 3 is approved).
5. **Redis/cache health dashboard** — hit/miss rate per D9 cache, geo-index staleness, idempotency-cache-miss
   rate per consumer group (direct DUP-01/RPL-01 visibility).
6. **Redrive/DLQ dashboard** — DLQ depth trend, redrive rate/outcome, `x-retry-count` distribution.

### 12.5 Runbook triggers

- **P1 broker under-replication** → check AZ health, confirm `min.insync.replicas` is still met on affected
  partitions, do not force a leader election manually unless the controller quorum is confirmed healthy.
- **P1 driver-service outbox lag** → check driver-service pod health and DB connection-pool saturation
  first (the write path, not Kafka, since the outbox row already committed); confirm relay threads are not
  starved.
- **P1 Redis fallback spike** → confirm whether this is a planned flush/maintenance or an unplanned failure;
  if unplanned, watch identity-service/driver-service CPU/connection-pool metrics for TH-02's compound-load
  risk and be ready to manually rate-limit or scale those two services before Redis fully recovers.
- **P2 blacklist-evaluation lag or the rejection-count internal-API hop failing** → check dispatch-service
  health; if the outage is prolonged, manually flag known-high-rejection drivers for review rather than
  relying on the automated path (a documented manual fallback for §11.1's fail-open exposure).
- **P3 chronic redrive** → escalate to the owning service's on-call for root-cause, do not redrive again
  without a code/config change since the last attempt.

---

## 13. Prioritised remediation table

Ordered by severity, then by effort (cheapest first within a severity tier).

| ID | Severity | Recommendation | Effort | Needs §10 schema change? | Needs user approval? | v1 or later? |
|---|---|---|---|---|---|---|
| RC-02 | HIGH | Confirm/assign an explicit re-dispatch-after-cancellation owner (client-initiated, or a new dispatch-service consumer off `tutem.trip.booking.cancelled.v1`) | Low (decision) / Medium (if new consumer) | No | **Yes** | **v1 — functional gap** |
| SPOF (PostgreSQL) | CRITICAL (infra) | Confirm HA/failover story for the single physical Postgres instance | Medium (infra, outside this doc) | No | **Yes** | **v1** |
| TH-02 | HIGH | Request-coalescing/single-flight on identity-service & driver-service fallback endpoints; staggered cache repopulation; capacity sizing for compound cache-flush | Medium | No | **Yes** | **v1** |
| TH-01 | HIGH | Client reconnect jitter/backoff; api-gateway rate-limit tuning for reconnect bursts | Low–Medium | No | **Yes** | **v1** |
| SPOF (Schema Registry) | HIGH | Document Schema Registry HA topology; confirm client-side schema caching | Low (documentation) / Medium (infra) | No | Yes | v1 |
| ORD-03 (carpool `pickup_order`) | MEDIUM | Approve baseline §10 item 3 (`uq_booking_pickup_order` + `ck_booking_order_mode`) | Low (index/CHECK only, no backfill) | **YES — §10 item 3** | **Yes** | **v1 — cheapest correctness fix in this entire register** |
| DUP-01 / RPL-01 (`total_trips`) | MEDIUM | Convert to a recompute (`COUNT(*)` pattern) matching `rating_avg`/`rating_count`'s already-proven shape | Low–Medium | No | Yes | v1 |
| §11.1 (blacklist rejection-count hop) | MEDIUM | Approve `GET /internal/v1/offers/rejection-count`; decide fail-open vs fail-closed | Low (API approval) / policy decision | No | **Yes** | v1 |
| §11.2 (carpool route-line hop) | MEDIUM | Name and approve `GET /internal/v1/trips/{tripId}/route` | Low (API approval) | No | **Yes** | v1 |
| LAG-01 | MEDIUM | Adopt the lag-budget table as the alerting SLO basis (§12) | Low (already designed for) | No | No (implementation detail) | v1 |
| RC-04 (seat-check locking) | LOW | Document explicit `SELECT ... FOR UPDATE` + clean-rejection handling for the seat-reservation transaction | Low (documentation) | No | No | v1 |
| ORD-02 (resharding window) | LOW | Add `tutem_outbox_shard_reassignment_total`; no design change | Low | No | No | v1 (monitoring only) |
| RTL-02 (redrive tracking) | LOW | Add `redriveCount`/`lastRedrivenAt` tooling-side tracking | Low | No | No | Later — tooling maturity item |
| PP-02 (offset-commit-after-DLQ) | LOW | One clarifying sentence in consumer design | Trivial | No | No | v1 (documentation) |
| TH-03 | LOW | None — already correctly sized | — | No | No | — |
| DLK-01 | LOW (confirmed-safe) | Forward-looking lock-ordering rule for future multi-row consumers | Trivial (documentation) | No | No | Later |
| PP-03, PP-04 | LOW | None beyond existing controls | — | No | No | — |

---

## 14. Accepted risks for v1 vs. must-fix-before-launch

### 14.1 ACCEPTED RISKS for v1 (with justification)

- **DUP-01 / RPL-01 residual, if the recompute fix (recommended above) is not taken before launch.**
  Justification: narrow-window (requires the intersection of a redelivery/replay AND a Redis-key loss),
  cosmetically scoped (no constraint or downstream flow reads `driver.total_trips`), and a reconciliation
  job is a cheap safety net even if the root-cause fix slips. **Conditionally accepted** — the
  Orchestrator should still decide whether to ship the recompute fix in v1 or accept-and-reconcile.
- **ORD-02 (D10 resharding window).** Justification: bounded to rollout duration, self-correcting, no
  data-loss consequence, and Step 8 itself already states this honestly rather than overclaiming zero risk.
- **TH-03 (offer TTL expiry storms).** Justification: already correctly budgeted for in partition sizing;
  no new exposure found.
- **SPOF — Parivahan/FCM/payment gateway/Routing API outages.** Justification: these are the documented,
  intended degradation paths (retry tiers, DLQ, straight-line fallback, CASH fallback) — the design already
  treats external-dependency unavailability as an expected operating condition, not an exceptional one.
- **DLK-01, PP-03, PP-04.** Justification: confirmed-safe or structurally-prevented by existing design
  choices (soft delete + `ON DELETE RESTRICT`, disjoint lock targets); no action needed.

### 14.2 MUST BE FIXED (or explicitly, consciously accepted with sign-off) BEFORE LAUNCH

- **RC-02 — re-dispatch-after-cancellation ownership.** This is a *functional* gap (a cancelled RIDE/WALK
  booking may leave the rider stranded with no re-search path), not merely a resiliency one. Must be
  resolved — either documented as client-initiated or given an owning consumer — before launch.
- **ORD-03 — carpool `pickup_order` collision.** Fires under realistic concurrent-booking load on any
  popular corridor, not an edge case, and baseline itself already calls this "the only correctness hole" in
  §10. The fix (§10 item 3) is the cheapest schema change in this entire register (index/CHECK only, no
  column, no backfill) — there is no good reason to defer it to a later release.
- **SPOF — PostgreSQL single-instance HA.** Every synchronous write in the product (accept, seat
  reservation, OTP, payment) depends on this one instance. This is the single largest correctness-path SPOF
  identified in this entire document and must have an explicit HA/failover answer, even if the answer is
  "accepted for v1 with a documented RTO," before launch.
- **TH-02 — compound Redis-flush load on identity-service/driver-service.** Must at minimum be sized and
  load-tested before the 50k-concurrent-user target is claimed as met, since this is precisely the scenario
  where three individually-low-severity design decisions (D9's caches) compose into a platform-wide capacity
  risk under the exact failure condition (Redis loss) the rest of the design otherwise treats as routine.

---

## 15. Summary of new findings not in the pre-supplied known-risks list

The prompt's 5 known risks (total_trips counter, pickup_order, resharding window, the two internal-API
hops, the three D9 caches) are all addressed above (DUP-01/RPL-01, ORD-03, ORD-02, §11.1/§11.2, TH-02
respectively) and deepened with quantified triggers, exact objects, and options. **New findings surfaced by
this analysis, not present in the known-risks list:**

- **RC-02** — no documented owner for re-dispatch after a provider-initiated RIDE/WALK cancellation, despite
  `uq_booking_request`'s unconditional uniqueness making this a real, not hypothetical, gap (Q-19).
- **RC-04** — the carpool seat-reservation transaction's locking clause (`SELECT ... FOR UPDATE`) and the
  application's handling of a `ck_trip_seats` violation are not explicitly documented, even though
  correctness itself is confirmed sound.
- **RTL-02** — no redrive-count tracking exists to detect a chronically-redriven, never-actually-fixed
  poison record.
- **PP-02** — offset-commit-after-DLQ-publish is implied by the whole architecture's non-blocking-partition
  design but never explicitly stated as a rule.
- **TH-01** — the 50k-concurrent-user WebSocket reconnect storm has no documented client-side jitter/backoff
  or gateway-side burst tolerance, despite being explicitly named as "the highest-connection-count component"
  in the baseline.
- **TH-02** — the compound thundering-herd effect of all three D9 caches plus the geo index plus every
  consumer's idempotency check degrading simultaneously on one Redis failure, which no single per-cache
  document addresses because each was evaluated independently.
- **SPOF (Schema Registry)** — the "one registry per environment" decision has no stated HA/replication
  story of its own, and a registry outage during a schema-version rollout can produce false poison-pill
  routing to `.dlq`.
- **SPOF (PostgreSQL single instance)** — flagged as the single largest correctness-path SPOF in the
  architecture, distinct from and more severe than any Kafka-side SPOF, since it blocks every
  `[SYNC-CRITICAL]` step in the product, not just async fan-out.

---

**End of Step 9.** Every recommendation above requires explicit Orchestrator approval before any
implementation, naming, topic, consumer-group, or schema change is made. Nothing in this document alters
`00-architecture-baseline.md` through `06-producers.md`; §10 of the baseline remains UNAPPROVED and every
capability gated on it remains UNAVAILABLE.
