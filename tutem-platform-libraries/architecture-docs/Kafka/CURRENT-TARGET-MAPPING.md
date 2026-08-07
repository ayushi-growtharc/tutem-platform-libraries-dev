# Current → Target Architecture Mapping

> **Purpose:** this repository describes **three** architectures that do not agree with each
> other (see `CLAUDE.md`'s "three-layer reality gap") — the live monolith, the actual Java
> code, and two separate greenfield doc streams (HLD/`KafkaNWebSocketAPIs.md` vs. the
> `00`–`07` baseline/Mongo-schema series). Every API-contract document produced under
> `Kafka/kafka-APIs/api-contracts/` has had to independently reconcile that gap, which means
> re-deriving the same facts and re-litigating the same discrepancies every time.
>
> This document exists so that stops happening. It is not a new design — it asserts nothing
> that isn't already stated somewhere in the code or the existing docs. It is a **lookup
> table**: for each capability, what actually exists today (grounded in real file paths,
> checked against the code in this session), what the approved target is, and — the part
> that turns "these disagree" into something actionable — what concretely triggers the move
> from one to the other.
>
> **How to use this from a contract document:** state which row(s) apply, build the contract
> against the **Current** column (it's what a developer can pick up and implement this
> sprint), and note the **Target**/**Trigger** columns as forward-looking context, not as
> something to build now. Don't silently build against an aspirational target and call it
> done — that is exactly the pattern that produced the conflicts this table exists to stop.

---

## 1. Service topology

| Aspect | Current (code) | Target | Trigger to move |
|---|---|---|---|
| Deployables | **2**: `tutem-user-service-dev`, `tutem-rider-service-dev` | **8** (baseline stream): api-gateway, realtime-gateway, identity-service, driver-service, dispatch-service, trip-service, config-service, notification-service — **or** 6 (HLD stream): identity, driver, ride, dispatch, geo, notification + 2 gateways. **These two target streams disagree with each other**, not just with the code — see §7. | Neither target has a stated trigger anywhere in the docs. Treat the 2-service code as authoritative for anything shipping now. |
| Auth/identity/org | `tutem-user-service-dev` (`modules/auth`, `modules/user`, `modules/organization`, `modules/admin`) | `identity-service` in both target streams — same responsibility, different name only | Rename-only when the split happens; no functional change implied |
| Ride/pairing/carpool/walk/driver/fare/station | `tutem-rider-service-dev` (`modules/ride`, `pairing`, `carpooling`, `walkmode`, `driver`, `fare`, `station`, `vehicletype`) | Split across `driver-service`, `dispatch-service`, `trip-service` (baseline) or `driver-service`, `ride-service`, `dispatch-service` (HLD) | Extraction trigger for each sub-split: not specified anywhere. Not recommended to attempt until geo extraction (below) proves the pattern. |
| Live location / geo | `tutem-rider-service-dev/modules/driver` (`DriverLocationStatus`, `DriverLocationTrack`) | A dedicated `geo-service` (HLD stream) *or* location-as-state-on-`Driver` inside `driver-service` (baseline stream — no separate geo-service at all) | **This is the one extraction worth doing next, independent of the other 7.** See §2 — it has its own, concrete trigger. |
| `api-gateway` / `realtime-gateway` | Does not exist as a deployable — Flutter apps hit `tutem.in/api5/*` directly today; the Java services have no gateway in front of them yet | Both target streams agree an edge gateway is needed | Trigger: whenever client traffic is cut over from the monolith to the Java services — this is a hard prerequisite for that cutover, not an optional step |

**Recommendation stated once, referenced from contracts rather than re-argued:** treat the
2-service code topology as current for everything except location/geo (§2), where enough
independent load and dependency pressure exists to justify going first.

---

## 2. Location / geo-service extraction

This is the one row with an actual, numeric trigger — worth its own table.

| Aspect | Current (code) | Target | Trigger |
|---|---|---|---|
| Owning module | `tutem-rider-service-dev/modules/driver` (co-located with driver profile/vehicle logic) | Standalone `modules/location` inside rider-service first (clean seams), lifted to a real `geo-service` deployable later | **Location write-rate exceeds ~500/sec sustained**, or Redis/socket-sdk dependencies added for this flow start destabilizing deploys of unrelated rider-service features (carpool, walk mode) — whichever comes first |
| Live position store | Mongo `driver_location_statuses` (`@GeoSpatialIndexed`, no TTL, single doc per driver — already a "live cache" pattern, just in the wrong store) | Redis (`drivers:live` geo-set + `driver:{id}` hash, `EXPIRE 30s`) as the sole live-position source; Mongo document **retired** once Redis is live, per the earlier discussion — one live-position store, not two | Redis dependency landing in rider-service (§2 of the location contract) — do this in the same change that adds Redis, not as a later cleanup |
| History store | `driver_location_tracks` (Mongo, **no TTL**, all fixes, unbounded growth) | Same collection, **retained only for fixes taken during an active ride, 90-day TTL** | **Decided** (this document) — see §3 |
| Ingest transport | `POST /api/driver/location-track`, one fix per REST call, `driverId` from request body (spoofable) | WebSocket-in (socket-sdk, Socket.IO) → Kafka `driver.location` → geo consumer, per `CatD-tracking.md` and its contract | Land alongside the Redis migration above — the REST endpoint should be retired in the same change, not left running in parallel with two write paths |
| Kafka topic | Does not exist — no consumer anywhere in rider-service today (`grep` for `@KafkaListener` returns nothing) | `driver.location`, HLD-defined, keyed by `driverId` | First Kafka consumer in rider-service; see §4 for what else that unblocks |

---

## 3. Location history retention (decided)

**Product decision:** location history is retained **only for fixes taken during an active
ride**, with a **90-day TTL** on top.

| Aspect | Current (code) | Target | Trigger |
|---|---|---|---|
| What gets written to `driver_location_tracks` | **Every** fix from every driver, on duty or not, ride or no ride — the existing `createLocationTrack` has no filter | **Only** fixes where the driver has an active-ride association at consume time (the same active-ride lookup the fan-out hop already does for the Redis `PUBLISH ride:{rideId}` step, per the location contract's §6) | Implement in the same consumer change that adds the Kafka ingest path (§2) — this is a filter condition in `GeoService.applyBatch`, not a separate project |
| Retention | **None** — no TTL index exists on this collection today | `expireAfterSeconds` TTL index on `createdAt`, 90 days | Same change — add the TTL index when the collection's write pattern changes, not as a follow-up migration |
| Estimated volume impact | ~1,250 writes/sec sustained (every driver, all the time) at target load | Only-during-ride filtering cuts this to roughly the fraction of drivers who are actually on a trip at any moment — plausibly a 70–80% reduction before the TTL even applies, since most of a driver's on-duty time is idle/searching, not on-trip | N/A — this is the expected effect of the decision above, not a separate trigger |
| Schema gap | No `sampledAt` field (only `@CreatedDate` ingest time) | Add `sampledAt` (nullable, additive) — see the location contract §4.3 | Same change |

**What this replaces:** the open question in `driver-location-ingest-contract.md` §4.5 /
Open Item 2 is now closed. Update that contract's checklist item "Decide the retention
policy" to reference this row instead of leaving it open.

---

## 4. Kafka producer/consumer pattern

| Aspect | Current (code) | Target | Trigger |
|---|---|---|---|
| Producer library | Hand-rolled: `shared/events/RideEventPublisher` (`KafkaTemplate<String,Object>`, `.whenComplete` error log), topic constants in `RideEventTopics` | `event-sdk` (`EventPublisher`, `EventEnvelope`, `TopicResolver`) from `tutem-platform-libraries-dev` | **Version compatibility**: `event-sdk` pins Spring Boot 3.5.4 / spring-kafka 3.3.7; both services are on 3.3.3. Trigger is a coordinated Spring Boot upgrade across both services, not a per-flow decision. |
| Consumers | **None** — no `@KafkaListener` exists anywhere in rider-service or user-service today | Per-flow, as needed | The `driver.location` consumer (§2) would be the **first** Kafka consumer in either service — treat its error-handling/batch-listener wiring as new ground, not an established pattern to copy |
| Event DTO shape | Flat `record`s, no envelope, no `eventId`/`correlationId` (e.g. `RideCreatedEvent`) | `EventEnvelope<T>` wrapping a payload, with `eventId`/`traceId`/`source` (event-sdk) *or* the 00-series baseline's own richer envelope (`eventId`, `correlationId`, `causationId`, `aggregateType`/`aggregateId`, `idempotencyKey`) — **these two target envelopes also don't match each other** | Same as producer library — don't adopt either envelope piecemeal per-flow; it's a whole-codebase migration once event-sdk is actually depended on |
| Serialization | `JsonSerializer`/`JsonDeserializer` (Spring Kafka support), configured in `application.yml` | Avro + Schema Registry (baseline stream only — the HLD stream doesn't specify a serializer) | Same Spring Boot upgrade trigger, plus standing up a Schema Registry, which doesn't exist in any environment today |

**Recommendation:** every contract written against the current codebase should keep using
the hand-rolled `RideEventPublisher` pattern (as both existing contracts in this folder do).
Do not introduce `event-sdk` piecemeal, one flow at a time — it isn't buildable against the
current Spring Boot version, and a partial migration would leave two envelope shapes live
simultaneously.

---

## 5. WebSocket protocol

| Aspect | Current (code) | Target | Trigger |
|---|---|---|---|
| Protocol | **Socket.IO** (`socket-sdk` wraps `netty-socketio`; shipped Flutter apps use `socket_io_client`) | **Docs say STOMP** (`/topic/ride.{id}`, `/topic/driver.{id}`) — this is a documentation defect, not a real target, since a Socket.IO client cannot speak STOMP and the shipped apps cannot be changed | **None needed — this should just be corrected in the docs**, not treated as a migration. See the reasoning in the prior discussion: Socket.IO is the only option that doesn't break every shipped build. |
| Destination naming | Socket.IO rooms, e.g. `ride.{rideId}`, joined via `sendToRoom` | Docs' `/topic/ride.{id}` maps 1:1 onto room `ride.{rideId}` — same identifier, different transport syntax | Find/replace across the docs: `/topic/X.{id}` → Socket.IO room `X.{id}`, stated once as a conversion rule so it isn't re-derived per contract |
| Cross-pod fan-out | Not yet implemented — `socket-sdk`'s `sendToRoom`/`broadcast` are pod-local by the library's own design (see `SessionManager` javadoc) | Redis pub/sub (`PUBLISH ride:{rideId}`, subscribed by every gateway pod) — **already designed**, not blocked on anything | Implement alongside the Redis dependency added for §2 — same Redis client, same PR |
| Session store | `InMemorySessionManager` (socket-sdk default) — single-pod only | A distributed `SessionManager` bean (Redis-backed), with `app.socket.heartbeat.evict-ghost-sessions=false` | Whenever rider-service is deployed with more than one pod — currently unclear if it is |

---

## 6. Module naming (rename, no architecture change)

| Aspect | Current (code) | Target | Trigger |
|---|---|---|---|
| `modules/locationhistory` | Holds `UserRecentSearchLocationHistory`, `NamedLocation` — user's saved/recent search locations, unrelated to GPS tracking | Rename to `modules/savedplaces` (or similar) — content unchanged, name corrected | No blocker — this is a same-PR rename whenever convenient. Recommended to do it **before** the new `modules/location` package (§2) is created, so the two don't sit side by side with confusingly similar names. |

---

## 7. The two greenfield doc streams (neither is "current")

Both of these are targets, not current state, and they disagree with each other. Listed here
so a contract can point at this row instead of re-explaining the conflict every time.

| Aspect | HLD / `KafkaNWebSocketAPIs.md` + `APIsToCreate.md` | Baseline / `00`–`07` + `Mongo/01-mongodb-schema.md` |
|---|---|---|
| Services | 6 + 2 gateways, includes `geo-service` | 8, no `geo-service` — location is state on `Driver` |
| Topic naming | Short HLD names (`driver.location`, `ride.requested`) | 5-segment (`tutem.<domain>.<aggregate>.<event>.v<major>`) |
| Registration endpoint | Dedicated `POST /v1/users`, supports OTP/PASSWORD/GOOGLE | Folded into `POST /auth/otp/verify`, OTP only |
| Error envelope | RFC 7807 `application/problem+json` | Custom `{code, message, correlationId, details?}` |
| Schema format | Unspecified | Avro + Schema Registry |

**No resolution recommended here.** Pick one stream per capability as contracts come up
(as the two existing contracts in this folder each had to), and add the decision as a new
row in this table so the next contract doesn't re-derive it.

---

## Maintenance rule for this table

When a contract document resolves a current/target discrepancy that isn't listed above, add
a row here rather than leaving the resolution buried in that contract only. This table is
only useful if it stays the single place these decisions accumulate.
