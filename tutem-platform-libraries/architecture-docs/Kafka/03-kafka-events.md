# Tutem — Kafka Event Catalogue (Step 5)

> **Status:** APPROVED. A D8 amendment (2026-07-28) revised `02-kafka-topics.md`'s retry-topic policy; it
> does not change any topic, event, producer, consumer, or partition key in this document (verified — this
> file contains no retry-topic references). The only change here is E2: `DriverCreated`'s trigger, corrected
> to F-02 step 3 to match `02-kafka-topics.md`.
> **Input:** [02-kafka-topics.md](02-kafka-topics.md) (topic names used verbatim, not modified),
> [00-architecture-baseline.md](00-architecture-baseline.md) §0.10 (envelope), §0.6 (partition keys), §0.7
> (idempotency), [01-kafka-architecture.md](01-kafka-architecture.md) §4 (outbox), §8 (Avro/BACKWARD).
> **Every field below traces to a real `NewSchema.md` column, is labelled `derived`, or is an envelope
> field.** No invented persisted field appears anywhere in this document.

---

## 0. Conventions used in every event below

**Envelope (§0.10 of baseline) — identical shape for all 39 events, shown once:**

| Field | Type | Header too? | Notes |
|---|---|---|---|
| `eventId` | UUID string | yes | = `outbox_event.id` |
| `eventType` | string | yes | PascalCase class name below |
| `schemaVersion` | string | yes | `<major>.<minor>` within the topic's `.v<major>` |
| `correlationId` | UUID string | yes | propagated from the originating user action |
| `causationId` | UUID string, nullable | no | `eventId` of the record that caused this one; `null` at the edge |
| `occurredAt` | string (ISO-8601 UTC) | no | when the fact became true in the producer's transaction |
| `producerService` | string | no | one of the 8 `<service-name>`s |
| `aggregateType` | string | no | the §0.2.1 registry root name |
| `aggregateId` | UUID string | no | the event's own aggregate root id |
| `idempotencyKey` | string | no | `<aggregate-token>:<aggregateId>:<event-name>[:<sequenceOrDate>]` |

Every example JSON below shows the envelope fields **flattened alongside** the payload fields in one Avro
record — this is the style used consistently throughout this document (Step 3 §8 chose Avro; the field
tables below are the Avro-style `field | type | nullable | source column | notes` schema per event, one
concrete example instance per event).

**Compatibility rule for every event (Step 3 §8, restated per-event only where a concrete example is
useful):** Avro `BACKWARD` compatibility, subject `<topic-name>-value`. A **BACKWARD-compatible** change is
always: adding a new **optional** field with a default (bumps `schemaVersion`'s minor digit, e.g. `1.0` →
`1.1`, same topic). A **breaking** change is always: removing/renaming a required field, or changing a
field's type incompatibly — this **cuts a new `.v2` topic**, never mutates the existing one (baseline
§0.2.2, Step 3 §8.1). Each event section gives one concrete instance of each.

**Outbox/transaction convention:** "Outbox" below always means *the owning service's own* `outbox_event`
row, inserted in the **same transaction and commit** as the domain write named (Step 3 §4.1).

**PII convention:** fields Step 3 §11 forbids (phone, email, `doc_number`, `holder_name`, `image_url`,
`parivahan_details`, free-text addresses/comments, `payment_ref`, raw route geometry beyond identifiers) are
never included; each event states what is deliberately excluded and how a consumer obtains it instead.

---

## 1. Identity domain (identity-service)

### 1.1 `AppUserRegistered`

| Aspect | Value |
|---|---|
| Topic | `tutem.identity.user.registered.v1` |
| Producer | identity-service |
| Consumers | trip-service — `trip-service.rider-directory-cache.v1` |
| Partition key | `user_id` (= `app_user.id`, the aggregate's own id — no exception) |
| Idempotency key | `user:3f1a2b4c-...:registered` |
| Ordering | Not required — order-independent by construction (Step 3 §5.2) |
| `schemaVersion` | `1.0` |
| Trigger | F-01 step 4 |
| Transaction/outbox | identity-service's `app_user` INSERT (F-01 step 3) + outbox row, same commit |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `userId` | UUID | no | `app_user.id` | |
| `phoneVerified` | boolean | no | `app_user.phone_verified` | always `true` at this point |
| `status` | string | no | `app_user.status` | `ACTIVE` |
| `createdAt` | string (ts) | no | `app_user.created_at` | |

Excluded (§11 PII): `phone`, `email`, `full_name`, `gender`. A consumer needing display detail calls
identity-service's public-profile API.

**Example**
```json
{
  "eventId": "8a5e...", "eventType": "AppUserRegistered", "schemaVersion": "1.0",
  "correlationId": "c1a1...", "causationId": null, "occurredAt": "2026-07-28T09:12:03Z",
  "producerService": "identity-service", "aggregateType": "AppUser", "aggregateId": "3f1a2b4c-...",
  "idempotencyKey": "user:3f1a2b4c-...:registered",
  "userId": "3f1a2b4c-...", "phoneVerified": true, "status": "ACTIVE", "createdAt": "2026-07-28T09:12:03Z"
}
```

**Versioning:** BACKWARD example — add optional `signupChannel` (nullable string, default `null`) → `1.1`.
Breaking example — removing `status` (a consumer that gates on it would silently break) forces `v2`.

---

### 1.2 `AppUserProfileUpdated`

| Aspect | Value |
|---|---|
| Topic | `tutem.identity.user.profile-updated.v1` |
| Producer | identity-service |
| Consumers | trip-service — `trip-service.rider-directory-cache.v1` |
| Partition key | `user_id` |
| Idempotency key | `user:<userId>:profile-updated:<occurredAt-date>` |
| Ordering | Not required |
| `schemaVersion` | `1.0` |
| Trigger | F-01 step 6 (profile update) |
| Transaction/outbox | identity-service's `app_user` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `userId` | UUID | no | `app_user.id` | |
| `ratingAvg` | decimal | yes | `app_user.rating_avg` | included because it is non-PII and display-relevant |
| `ratingCount` | int | yes | `app_user.rating_count` | |

Excluded: `full_name`, `email`, `gender` (§11). A display consumer resolves the current name via the
public-profile API rather than trusting a stale event copy.

**Example**
```json
{
  "eventId": "9b3f...", "eventType": "AppUserProfileUpdated", "schemaVersion": "1.0",
  "correlationId": "c2b2...", "causationId": null, "occurredAt": "2026-07-28T10:00:11Z",
  "producerService": "identity-service", "aggregateType": "AppUser", "aggregateId": "3f1a2b4c-...",
  "idempotencyKey": "user:3f1a2b4c-...:profile-updated:2026-07-28",
  "userId": "3f1a2b4c-...", "ratingAvg": 4.8, "ratingCount": 12
}
```

**Versioning:** BACKWARD — add optional `preferredLanguage` → `1.1`. Breaking — changing `ratingAvg` from
decimal to a nested `{avg, count}` object forces `v2`.

---

### 1.3 `AppUserDeleted`

| Aspect | Value |
|---|---|
| Topic | `tutem.identity.user.deleted.v1` |
| Producer | identity-service |
| Consumers | dispatch-service — `dispatch-service.user-deletion-cleanup.v1`; trip-service — `trip-service.rider-directory-cache.v1` |
| Partition key | `user_id` |
| Idempotency key | `user:<userId>:deleted` |
| Ordering | Not required |
| `schemaVersion` | `1.0` |
| Trigger | F-01 step 6 (soft delete) |
| Transaction/outbox | identity-service's `app_user.status='DELETED'` UPDATE + outbox row (never a hard delete — `ON DELETE RESTRICT`) |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `userId` | UUID | no | `app_user.id` | |
| `deletedAt` | string (ts) | no | `derived` — `occurredAt` of this event | no dedicated column; soft delete is the `status` transition itself |

**Example**
```json
{
  "eventId": "1c44...", "eventType": "AppUserDeleted", "schemaVersion": "1.0",
  "correlationId": "c3c3...", "causationId": null, "occurredAt": "2026-07-28T11:20:00Z",
  "producerService": "identity-service", "aggregateType": "AppUser", "aggregateId": "3f1a2b4c-...",
  "idempotencyKey": "user:3f1a2b4c-...:deleted",
  "userId": "3f1a2b4c-...", "deletedAt": "2026-07-28T11:20:00Z"
}
```

**Versioning:** BACKWARD — add optional `deletionReason` enum-as-string → `1.1`. Breaking — none envisioned
without changing the meaning of soft delete itself; a hypothetical hard-delete signal would require `v2`
because it changes the field's semantics, not just its shape.

**Consumer effect (dispatch-service):** cancels any `SEARCHING` `service_request` owned by `userId`
(idempotent `WHERE status='SEARCHING'`), publishing `ServiceRequestCancelled` in turn.

---

## 2. Driver domain (driver-service)

### 2.1 `DriverCreated`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.driver.created.v1` |
| Producer | driver-service |
| Consumers | notification-service — `notification-service.driver-welcome.v1` |
| Partition key | `driver_id` (= `driver.user_id` — `driver`'s primary key is `user_id`, NewSchema §4.2; there is no `driver.id` column) |
| Idempotency key | `driver:<driverId>:created` |
| Ordering | Not required |
| `schemaVersion` | `1.0` |
| Trigger | F-02 step 3 (the "driver profile created" fact emission — matches `02-kafka-topics.md`'s F-02.3 citation) |
| Transaction/outbox | driver-service's `driver` INSERT (F-02 step 1) + outbox row, same commit |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `driverId` | UUID | no | `driver.user_id` | `driver`'s primary key is `user_id` (NewSchema §4.2) — no separate `driver.id` column exists |
| `userId` | UUID | no | `driver.user_id` | links back to the `AppUser`; same column as `driverId` above, carried separately for payload clarity |
| `driverKind` | string | no | `driver.driver_kind` | `FULL_TIME` \| `CARPOOL` |
| `verificationStatus` | string | no | `driver.verification_status` | `PENDING` at creation |

**Example**
```json
{
  "eventId": "2d55...", "eventType": "DriverCreated", "schemaVersion": "1.0",
  "correlationId": "c4d4...", "causationId": null, "occurredAt": "2026-07-28T08:00:00Z",
  "producerService": "driver-service", "aggregateType": "Driver", "aggregateId": "7e2b...",
  "idempotencyKey": "driver:7e2b...:created",
  "driverId": "7e2b...", "userId": "3f1a2b4c-...", "driverKind": "CARPOOL", "verificationStatus": "PENDING"
}
```

**Versioning:** BACKWARD — add optional `referralSource` → `1.1`. Breaking — splitting `driverKind` into a
list (multi-kind drivers) would change cardinality and forces `v2`.

**Note — notification-service as a direct domain-event consumer.** This is the one place notification-service
subscribes to a raw fact topic rather than only `tutem.notification.send-push.command.v1`; F-02 step 3
explicitly evidences it ("a 'driver profile created' fact is emitted... notification-service greets them").
Every other notification touchpoint in this catalogue goes through the command topic.

---

### 2.2 `DriverWentOnline`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.driver.went-online.v1` |
| Producer | driver-service |
| Consumers | realtime-gateway — `realtime-gateway.driver-presence-fanout.v1` |
| Partition key | `driver_id` |
| Idempotency key | `driver:<driverId>:went-online:<occurredAt-iso>` |
| Ordering | Per-driver, last-write-wins — matters for presence UI consistency |
| `schemaVersion` | `1.0` |
| Trigger | F-04 step 4 |
| Transaction/outbox | driver-service's `driver.is_online=TRUE` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `driverId` | UUID | no | `driver.user_id` | `driver`'s primary key is `user_id` (NewSchema §4.2) — no separate `driver.id` column exists |
| `activeMode` | string | no | `driver.active_mode` | `RIDE`\|`CARPOOL`\|`WALK` |
| `activeVehicleId` | UUID | yes | `driver.active_vehicle_id` | `null` for `WALK` |

**Example**
```json
{
  "eventId": "3e66...", "eventType": "DriverWentOnline", "schemaVersion": "1.0",
  "correlationId": "c5e5...", "causationId": null, "occurredAt": "2026-07-28T07:00:00Z",
  "producerService": "driver-service", "aggregateType": "Driver", "aggregateId": "7e2b...",
  "idempotencyKey": "driver:7e2b...:went-online:2026-07-28T07:00:00Z",
  "driverId": "7e2b...", "activeMode": "RIDE", "activeVehicleId": "9a11..."
}
```

**Versioning:** BACKWARD — add optional `clientAppVersion` → `1.1`. Breaking — none envisioned; a mode-set
(multi-mode online) change would force `v2`.

---

### 2.3 `DriverWentOffline`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.driver.went-offline.v1` |
| Producer | driver-service |
| Consumers | realtime-gateway — `realtime-gateway.driver-presence-fanout.v1` |
| Partition key | `driver_id` |
| Idempotency key | `driver:<driverId>:went-offline:<occurredAt-iso>` |
| Ordering | Per-driver, last-write-wins |
| `schemaVersion` | `1.0` |
| Trigger | F-05 steps 1–2 (explicit) or F-05 step 4 (reconciliation sweep) |
| Transaction/outbox | driver-service's `is_online=FALSE, active_mode=NULL, active_vehicle_id=NULL` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `driverId` | UUID | no | `driver.user_id` | `driver`'s primary key is `user_id` (NewSchema §4.2) — no separate `driver.id` column exists |
| `reason` | string | no | `derived` | `EXPLICIT` \| `STALE_PING_RECONCILIATION` — not a stored column, computed by the writing code path |

**Example**
```json
{
  "eventId": "4f77...", "eventType": "DriverWentOffline", "schemaVersion": "1.0",
  "correlationId": "c6f6...", "causationId": null, "occurredAt": "2026-07-28T18:30:00Z",
  "producerService": "driver-service", "aggregateType": "Driver", "aggregateId": "7e2b...",
  "idempotencyKey": "driver:7e2b...:went-offline:2026-07-28T18:30:00Z",
  "driverId": "7e2b...", "reason": "EXPLICIT"
}
```

**Versioning:** BACKWARD — add optional `lastActiveMode` (snapshot before clearing) → `1.1`. Breaking — none
envisioned.

---

### 2.4 `DriverLocationUpdated`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.driver.location-updated.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.geo-index-maintenance.v1`; realtime-gateway — `realtime-gateway.driver-location-fanout.v1` |
| Partition key | `driver_id` |
| Idempotency key | `driver:<driverId>:location-updated:<occurredAt-iso>` |
| Ordering | Per-driver, last-write-wins by `occurredAt` (never publish timestamp — baseline §0.10) |
| `schemaVersion` | `1.0` |
| Trigger | F-06 step 2 |
| Transaction/outbox | driver-service's `driver.current_location`/`location_updated_at` UPDATE + outbox row — **the single indexed `UPDATE` that must never share a transaction with anything else** (F-06 step 6) |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `driverId` | UUID | no | `driver.user_id` | `driver`'s primary key is `user_id` (NewSchema §4.2) — no separate `driver.id` column exists |
| `latitude` | double | no | `driver.current_location` (X/lon, Y/lat decomposed) | **PII exception — see §8 of the topic catalogue** |
| `longitude` | double | no | `driver.current_location` | |
| `activeMode` | string | no | `driver.active_mode` | needed by realtime-gateway to route to the right channel |
| `locationUpdatedAt` | string (ts) | no | `driver.location_updated_at` | |

**Example**
```json
{
  "eventId": "5061...", "eventType": "DriverLocationUpdated", "schemaVersion": "1.0",
  "correlationId": "c707...", "causationId": null, "occurredAt": "2026-07-28T12:00:04Z",
  "producerService": "driver-service", "aggregateType": "Driver", "aggregateId": "7e2b...",
  "idempotencyKey": "driver:7e2b...:location-updated:2026-07-28T12:00:04Z",
  "driverId": "7e2b...", "latitude": 19.0760, "longitude": 72.8777, "activeMode": "RIDE",
  "locationUpdatedAt": "2026-07-28T12:00:04Z"
}
```

**Versioning:** BACKWARD — add optional `speedKmh`/`headingDegrees` (derived from consecutive pings, not
stored) → `1.1`. Breaking — switching from separate `latitude`/`longitude` scalars to a nested GeoJSON point
forces `v2`, because every existing consumer's flat-field read breaks.

---

### 2.5 `VehicleRegistered`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.vehicle.registered.v1` |
| Producer | driver-service |
| Consumers | trip-service — `trip-service.vehicle-snapshot-cache.v1` |
| Partition key | `driver_id` (§0.6: `vehicle` keys by the owning driver, not `vehicle_id`) |
| Idempotency key | `vehicle:<vehicleId>:registered` (envelope `aggregateId` = `vehicle.id`, not the partition key — §0.7) |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-02 step 2 |
| Transaction/outbox | driver-service's `vehicle` INSERT + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `vehicleId` | UUID | no | `vehicle.id` | |
| `driverId` | UUID | no | `vehicle.driver_id` | |
| `category` | string | no | `vehicle.category` | `BIKE`\|`AUTO`\|`CAB` |
| `seatCapacity` | int | no | `vehicle.seat_capacity` | bikes forced to 1 |

Excluded: `registration_no` — not classified PII by §11 but is a real-world identifying plate number with
no stated consumer need; omitted on minimal-payload grounds. A consumer needing it calls driver-service's
vehicle-snapshot API.

**Example**
```json
{
  "eventId": "6172...", "eventType": "VehicleRegistered", "schemaVersion": "1.0",
  "correlationId": "c818...", "causationId": null, "occurredAt": "2026-07-28T08:05:00Z",
  "producerService": "driver-service", "aggregateType": "Vehicle", "aggregateId": "9a11...",
  "idempotencyKey": "vehicle:9a11...:registered",
  "vehicleId": "9a11...", "driverId": "7e2b...", "category": "CAB", "seatCapacity": 4
}
```

**Versioning:** BACKWARD — add optional `fuelType` → `1.1`. Breaking — none envisioned.

---

### 2.6 `VehicleDeactivated`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.vehicle.deactivated.v1` |
| Producer | driver-service |
| Consumers | trip-service — `trip-service.vehicle-snapshot-cache.v1` |
| Partition key | `driver_id` |
| Idempotency key | `vehicle:<vehicleId>:deactivated` |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | Vehicle-management API call (part of F-02's vehicle lifecycle; no separately numbered flow step) |
| Transaction/outbox | driver-service's `vehicle` deactivation UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `vehicleId` | UUID | no | `vehicle.id` | |
| `driverId` | UUID | no | `vehicle.driver_id` | |

**Example**
```json
{
  "eventId": "7283...", "eventType": "VehicleDeactivated", "schemaVersion": "1.0",
  "correlationId": "c929...", "causationId": null, "occurredAt": "2026-08-01T09:00:00Z",
  "producerService": "driver-service", "aggregateType": "Vehicle", "aggregateId": "9a11...",
  "idempotencyKey": "vehicle:9a11...:deactivated",
  "vehicleId": "9a11...", "driverId": "7e2b..."
}
```

**Versioning:** BACKWARD — add optional `deactivationReason` → `1.1`. Breaking — none envisioned.

---

### 2.7 `DriverDocumentSubmitted`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.document.submitted.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.parivahan-verification.v1` (self-consumed: decouples the upload API from the slow Parivahan call, per Step 3 §6's "keep slow external calls off the poll loop" rule) |
| Partition key | `driver_id` |
| Idempotency key | `document:<documentId>:submitted` |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-03 step 3 |
| Transaction/outbox | driver-service's `driver_document` INSERT (`status='PENDING'`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `documentId` | UUID | no | `driver_document.id` | |
| `driverId` | UUID | no | `driver_document.driver_id` | |
| `docType` | string | no | `driver_document.doc_type` | `DRIVING_LICENSE`\|`VEHICLE_RC` — American spelling, matching `ck_doc_type`'s `CHECK (doc_type IN ('DRIVING_LICENSE','VEHICLE_RC'))` (NewSchema §4.4) exactly |
| `vehicleId` | UUID | yes | `driver_document.vehicle_id` | required for `VEHICLE_RC` (`ck_doc_rc_vehicle`) |

Excluded (§11): `doc_number`, `holder_name`, `expiry_date`, `image_url`. The Parivahan-verification consumer
reads these directly from Postgres by `documentId` (same service, same DB) — no cross-service PII exposure
occurs, and the topic never carries them.

**Example**
```json
{
  "eventId": "8394...", "eventType": "DriverDocumentSubmitted", "schemaVersion": "1.0",
  "correlationId": "ca3a...", "causationId": null, "occurredAt": "2026-07-28T08:10:00Z",
  "producerService": "driver-service", "aggregateType": "DriverDocument", "aggregateId": "b1c2...",
  "idempotencyKey": "document:b1c2...:submitted",
  "documentId": "b1c2...", "driverId": "7e2b...", "docType": "VEHICLE_RC", "vehicleId": "9a11..."
}
```

**Versioning:** BACKWARD — add optional `resubmission` boolean flag → `1.1`. Breaking — none envisioned.

---

### 2.8 `DriverDocumentVerified`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.document.verified.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.verification-status-recompute.v1` |
| Partition key | `driver_id` |
| Idempotency key | `document:<documentId>:verified` |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-03 step 6 (VERIFIED branch) |
| Transaction/outbox | driver-service's `driver_document.status='VERIFIED'` UPDATE (`ck_doc_verified`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `documentId` | UUID | no | `driver_document.id` | |
| `driverId` | UUID | no | `driver_document.driver_id` | |
| `docType` | string | no | `driver_document.doc_type` | |
| `verifiedAt` | string (ts) | no | `driver_document.verified_at` | |

Excluded: `parivahan_details` (§11 — full raw verification payload never crosses the bus).

**Example**
```json
{
  "eventId": "94a5...", "eventType": "DriverDocumentVerified", "schemaVersion": "1.0",
  "correlationId": "cb4b...", "causationId": "8394...", "occurredAt": "2026-07-28T08:40:00Z",
  "producerService": "driver-service", "aggregateType": "DriverDocument", "aggregateId": "b1c2...",
  "idempotencyKey": "document:b1c2...:verified",
  "documentId": "b1c2...", "driverId": "7e2b...", "docType": "VEHICLE_RC", "verifiedAt": "2026-07-28T08:40:00Z"
}
```

**Versioning:** BACKWARD — add optional `verificationLatencyMs` (derived, submitted→verified delta) → `1.1`.
Breaking — none envisioned.

---

### 2.9 `DriverDocumentRejected`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.document.rejected.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.verification-status-recompute.v1` |
| Partition key | `driver_id` |
| Idempotency key | `document:<documentId>:rejected` |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-03 step 6 (REJECTED branch) |
| Transaction/outbox | driver-service's `driver_document.status='REJECTED'` UPDATE (`ck_doc_rejected`, mandatory `rejection_reason`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `documentId` | UUID | no | `driver_document.id` | |
| `driverId` | UUID | no | `driver_document.driver_id` | |
| `docType` | string | no | `driver_document.doc_type` | |
| `rejectionReason` | string | no | `driver_document.rejection_reason` | not PII — a compliance code/short reason, kept because F-03 step 8 must render it to the driver |

**Example**
```json
{
  "eventId": "a5b6...", "eventType": "DriverDocumentRejected", "schemaVersion": "1.0",
  "correlationId": "cc5c...", "causationId": "8394...", "occurredAt": "2026-07-28T08:40:00Z",
  "producerService": "driver-service", "aggregateType": "DriverDocument", "aggregateId": "b1c2...",
  "idempotencyKey": "document:b1c2...:rejected",
  "documentId": "b1c2...", "driverId": "7e2b...", "docType": "VEHICLE_RC", "rejectionReason": "IMAGE_ILLEGIBLE"
}
```

**Versioning:** BACKWARD — add optional `retryAllowed` boolean → `1.1`. Breaking — none envisioned.

---

### 2.10 `DriverBlacklistApplied`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.blacklist.blacklist-applied.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.blacklist-geo-sync.v1` |
| Partition key | `driver_id` |
| Idempotency key | `blacklist:<blacklistId>:blacklist-applied:<triggerDate>` — the one class of idempotency key that carries the `:<sequenceOrDate>` suffix (§0.7), because `uq_bl_one_per_day` is itself date-scoped |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-13 step 4 (driver-response path) or F-14 step 1a (expiry-sweep path) |
| Transaction/outbox | driver-service's `driver_blacklist` INSERT + `driver.blacklisted_until`/`is_online=FALSE` UPDATE + outbox row, one transaction |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `blacklistId` | UUID | no | `driver_blacklist.id` | |
| `driverId` | UUID | no | `driver_blacklist.driver_id` | |
| `reason` | string | no | `driver_blacklist.reason` | `EXCESSIVE_REJECTION` for this rule |
| `triggerDate` | string (date) | no | `driver_blacklist.trigger_date` | |
| `rejectionCount` | int | no | `driver_blacklist.rejection_count` | **snapshotted**, per D4 counts `REJECTED`+`EXPIRED` |
| `threshold` | int | no | `driver_blacklist.threshold` | snapshotted so history explains itself under the rule in force at the time (F-20 step 4) |
| `blockedUntil` | string (ts) | no | `driver_blacklist.blocked_until` | capped at 30 days (`ck_bl_temporary`) |

**Example**
```json
{
  "eventId": "b6c7...", "eventType": "DriverBlacklistApplied", "schemaVersion": "1.0",
  "correlationId": "cd6d...", "causationId": "d7e8...", "occurredAt": "2026-07-28T14:00:00Z",
  "producerService": "driver-service", "aggregateType": "DriverBlacklist", "aggregateId": "c3d4...",
  "idempotencyKey": "blacklist:c3d4...:blacklist-applied:2026-07-28",
  "blacklistId": "c3d4...", "driverId": "7e2b...", "reason": "EXCESSIVE_REJECTION",
  "triggerDate": "2026-07-28", "rejectionCount": 5, "threshold": 5, "blockedUntil": "2026-07-29T14:00:00Z"
}
```

**Versioning:** BACKWARD — add optional `evidenceOfferIds` array (which offers counted) → `1.1`. Breaking —
none envisioned; changing `rejectionCount`/`threshold` from snapshotted ints to a formula reference would
force `v2` because it changes what old consumers can trust as historically accurate.

---

### 2.11 `DriverBlacklistExpired`

| Aspect | Value |
|---|---|
| Topic | `tutem.driver.blacklist.blacklist-expired.v1` |
| Producer | driver-service |
| Consumers | driver-service — `driver-service.blacklist-geo-sync.v1` |
| Partition key | `driver_id` |
| Idempotency key | `blacklist:<blacklistId>:blacklist-expired` |
| Ordering | Per-driver |
| `schemaVersion` | `1.0` |
| Trigger | F-15 step 1 (nightly job) or step 2 (lazy go-online check) |
| Transaction/outbox | driver-service's `driver_blacklist.is_active=FALSE` + `driver.blacklisted_until=NULL` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `blacklistId` | UUID | no | `driver_blacklist.id` | |
| `driverId` | UUID | no | `driver_blacklist.driver_id` | |

**Example**
```json
{
  "eventId": "c7d8...", "eventType": "DriverBlacklistExpired", "schemaVersion": "1.0",
  "correlationId": "ce7e...", "causationId": null, "occurredAt": "2026-07-29T14:00:05Z",
  "producerService": "driver-service", "aggregateType": "DriverBlacklist", "aggregateId": "c3d4...",
  "idempotencyKey": "blacklist:c3d4...:blacklist-expired",
  "blacklistId": "c3d4...", "driverId": "7e2b..."
}
```

**Versioning:** BACKWARD — add optional `expiredVia` (`SCHEDULED`\|`LAZY_CHECK`) → `1.1`. Breaking — none
envisioned.

---

## 3. Dispatch domain (dispatch-service)

### 3.1 `RideOfferCreated`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.offer.created.v1` |
| Producer | dispatch-service |
| Consumers | realtime-gateway — `realtime-gateway.offer-countdown-fanout.v1` |
| Partition key | `request_id` (§0.6 exception 1 — co-locates all offers of one request) |
| Idempotency key | `offer:<offerId>:created` — **not derivable from topic+record key**, since the key is `request_id`, not `offer_id` (§0.7); must be read from the envelope |
| Ordering | **Yes** — all offers for one request stay mutually ordered |
| `schemaVersion` | `1.0` |
| Trigger | F-07 step 6 (RIDE) / F-12 step 4 (WALK) |
| Transaction/outbox | dispatch-service's `ride_offer` INSERT (`status='SENT'`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `offerId` | UUID | no | `ride_offer.id` | |
| `requestId` | UUID | no | `ride_offer.request_id` | = the record key |
| `driverId` | UUID | no | `ride_offer.driver_id` | |
| `distanceKm` | decimal | no | `ride_offer.distance_km` | driver→pickup at offer time |
| `expiresAt` | string (ts) | no | `ride_offer.expires_at` | drives the client countdown |

**Example**
```json
{
  "eventId": "d8e9...", "eventType": "RideOfferCreated", "schemaVersion": "1.0",
  "correlationId": "cf8f...", "causationId": null, "occurredAt": "2026-07-28T09:30:02Z",
  "producerService": "dispatch-service", "aggregateType": "RideOffer", "aggregateId": "e5f6...",
  "idempotencyKey": "offer:e5f6...:created",
  "offerId": "e5f6...", "requestId": "d4e5...", "driverId": "7e2b...", "distanceKm": 1.8,
  "expiresAt": "2026-07-28T09:30:22Z"
}
```

**Versioning:** BACKWARD — add optional `vehicleCategory` (echoed from the request) → `1.1`. Breaking —
none envisioned.

---

### 3.2 `RideOfferAccepted`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.offer.accepted.v1` |
| Producer | dispatch-service |
| Consumers | realtime-gateway — `realtime-gateway.offer-countdown-fanout.v1`; trip-service — `trip-service.trip-provisioning.v1` |
| Partition key | `request_id` |
| Idempotency key | `offer:<offerId>:accepted` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-08 step 2–4 / F-12 step 5 |
| Transaction/outbox | dispatch-service's single conditional `UPDATE ride_offer SET status='ACCEPTED' ... WHERE status='SENT' AND expires_at > now()` (NewSchema §4.6) + sibling `WITHDRAWN` + `service_request.status='MATCHED'`, all one transaction, + outbox row. **This event only broadcasts a verdict already settled by `uq_offer_single_accept` — Kafka never adjudicates the race.** |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `offerId` | UUID | no | `ride_offer.id` | |
| `requestId` | UUID | no | `ride_offer.request_id` | |
| `driverId` | UUID | no | `ride_offer.driver_id` | the winner |
| `respondedAt` | string (ts) | no | `ride_offer.responded_at` | |

**Example**
```json
{
  "eventId": "e9fa...", "eventType": "RideOfferAccepted", "schemaVersion": "1.0",
  "correlationId": "d001...", "causationId": null, "occurredAt": "2026-07-28T09:30:11Z",
  "producerService": "dispatch-service", "aggregateType": "RideOffer", "aggregateId": "e5f6...",
  "idempotencyKey": "offer:e5f6...:accepted",
  "offerId": "e5f6...", "requestId": "d4e5...", "driverId": "7e2b...", "respondedAt": "2026-07-28T09:30:11Z"
}
```

**Versioning:** BACKWARD — add optional `roundNumber` (which dispatch round won) → `1.1`. Breaking — none
envisioned.

**trip-service's consumption (F-08 step 5):** creates `trip` + `booking` in its own transaction, guarded by
`uq_trip_one_live_per_provider` and `uq_booking_request` — an at-least-once redelivery is a safe no-op
because `uq_booking_request` rejects a second insert for the same `request_id`.

---

### 3.3 `RideOfferRejected`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.offer.rejected.v1` |
| Producer | dispatch-service |
| Consumers | driver-service — `driver-service.blacklist-evaluation.v1` |
| Partition key | `request_id` |
| Idempotency key | `offer:<offerId>:rejected` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-13 step 1 |
| Transaction/outbox | dispatch-service's `ride_offer.status='REJECTED'` UPDATE (`ck_offer_resp`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `offerId` | UUID | no | `ride_offer.id` | |
| `requestId` | UUID | no | `ride_offer.request_id` | |
| `driverId` | UUID | no | `ride_offer.driver_id` | evidence subject (D4) |
| `respondedAt` | string (ts) | no | `ride_offer.responded_at` | |

**Example**
```json
{
  "eventId": "faab...", "eventType": "RideOfferRejected", "schemaVersion": "1.0",
  "correlationId": "d112...", "causationId": null, "occurredAt": "2026-07-28T09:31:00Z",
  "producerService": "dispatch-service", "aggregateType": "RideOffer", "aggregateId": "e6a7...",
  "idempotencyKey": "offer:e6a7...:rejected",
  "offerId": "e6a7...", "requestId": "d4e5...", "driverId": "8f3c...", "respondedAt": "2026-07-28T09:31:00Z"
}
```

**Versioning:** BACKWARD — add optional `rejectionReasonCode` (if ever collected) → `1.1`. Breaking — none
envisioned.

**Consumer effect:** driver-service runs `SELECT COUNT(*) FROM ride_offer WHERE driver_id=:d AND status IN
('REJECTED','EXPIRED') AND offered_at >= CURRENT_DATE` against the config snapshot's
`driver.rejection.daily_threshold`; at threshold, produces `DriverBlacklistApplied` (§2.10).

---

### 3.4 `RideOfferExpired`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.offer.expired.v1` |
| Producer | dispatch-service |
| Consumers | driver-service — `driver-service.blacklist-evaluation.v1`; realtime-gateway — `realtime-gateway.offer-countdown-fanout.v1` |
| Partition key | `request_id` |
| Idempotency key | `offer:<offerId>:expired` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-14 step 1 |
| Transaction/outbox | dispatch-service's expiry sweep `UPDATE ... WHERE status='SENT' AND expires_at <= now()` (`ck_offer_resp` requires `responded_at IS NULL`) + outbox row. **One evidence-bearing fact per affected driver, not per offer** (F-14 step 1a), to avoid firing the same driver-day blacklist evaluation N times — `uq_bl_one_per_day` makes a duplicate harmless but this is the deliberate batching that avoids the waste. |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `offerId` | UUID | no | `ride_offer.id` | |
| `requestId` | UUID | no | `ride_offer.request_id` | |
| `driverId` | UUID | no | `ride_offer.driver_id` | evidence subject — **D4: this event is itself blacklist evidence** |

**Example**
```json
{
  "eventId": "0bbc...", "eventType": "RideOfferExpired", "schemaVersion": "1.0",
  "correlationId": "d223...", "causationId": null, "occurredAt": "2026-07-28T09:31:22Z",
  "producerService": "dispatch-service", "aggregateType": "RideOffer", "aggregateId": "e7b8...",
  "idempotencyKey": "offer:e7b8...:expired",
  "offerId": "e7b8...", "requestId": "d4e5...", "driverId": "9a4d..."
}
```

**Versioning:** BACKWARD — add optional `sweptBatchId` (operational correlation for the sweep run) → `1.1`.
Breaking — none envisioned.

**This is the D4/blacklist-as-evidence event.** Compared to §3.3 (`RideOfferRejected`), this is the
**timer-driven** trigger path — the same `driver-service.blacklist-evaluation.v1` consumer group handles
both, running the identical `COUNT(*)` query, so a driver's daily count is correct regardless of which path
produced the evidence.

---

### 3.5 `RideOfferWithdrawn`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.offer.withdrawn.v1` |
| Producer | dispatch-service |
| Consumers | realtime-gateway — `realtime-gateway.offer-countdown-fanout.v1` |
| Partition key | `request_id` |
| Idempotency key | `offer:<offerId>:withdrawn` |
| Ordering | **Yes** — must be seen after the `RideOfferAccepted` (or `ServiceRequestCancelled`) that caused it, on the same partition |
| `schemaVersion` | `1.0` |
| Trigger | F-08 step 4 (sibling withdrawal) / F-12 step 5 / F-17 step 1 (pre-match cancellation) |
| Transaction/outbox | Same transaction as the accept `UPDATE` (F-08 step 4) or the request-cancel `UPDATE` (F-17 step 1); one outbox row per withdrawn sibling |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `offerId` | UUID | no | `ride_offer.id` | |
| `requestId` | UUID | no | `ride_offer.request_id` | |
| `driverId` | UUID | no | `ride_offer.driver_id` | **never** blacklist evidence (D4) |

**Example**
```json
{
  "eventId": "1ccd...", "eventType": "RideOfferWithdrawn", "schemaVersion": "1.0",
  "correlationId": "d001...", "causationId": "e9fa...", "occurredAt": "2026-07-28T09:30:11Z",
  "producerService": "dispatch-service", "aggregateType": "RideOffer", "aggregateId": "e6a9...",
  "idempotencyKey": "offer:e6a9...:withdrawn",
  "offerId": "e6a9...", "requestId": "d4e5...", "driverId": "b2c3..."
}
```

**Versioning:** BACKWARD — add optional `withdrawnReason` (`LOST_RACE`\|`REQUEST_CANCELLED`) → `1.1`.
Breaking — none envisioned.

---

### 3.6 `ServiceRequestCreated`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.request.created.v1` |
| Producer | dispatch-service |
| Consumers | trip-service — `trip-service.history-projection.v1` |
| Partition key | `request_id` |
| Idempotency key | `request:<requestId>:created` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-07 step 3 / F-12 step 1 |
| Transaction/outbox | dispatch-service's `service_request` INSERT (`uq_req_one_live_per_rider`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `requestId` | UUID | no | `service_request.id` | |
| `riderId` | UUID | no | `service_request.rider_id` | |
| `mode` | string | no | `service_request.mode` | `RIDE`\|`CARPOOL`\|`WALK` |
| `vehicleCategory` | string | yes | `service_request.vehicle_category` | `null` for `WALK` (`ck_req_walk`) |
| `seatsRequested` | int | no | `service_request.seats_requested` | |
| `estFare` | decimal | yes | `service_request.est_fare` | |
| `estDistanceKm` | decimal | yes | `service_request.est_distance_km` | |
| `expiresAt` | string (ts) | no | `service_request.expires_at` | |

Excluded (§11): `pickup_address`/`drop_address` free text, raw `pickup_point`/`drop_point` geometry.
trip-service's history projector stores only what it needs to render a list row; a detail view fetches full
geometry/address from dispatch-service's sync API.

**Example**
```json
{
  "eventId": "2dde...", "eventType": "ServiceRequestCreated", "schemaVersion": "1.0",
  "correlationId": "d334...", "causationId": null, "occurredAt": "2026-07-28T09:29:50Z",
  "producerService": "dispatch-service", "aggregateType": "ServiceRequest", "aggregateId": "d4e5...",
  "idempotencyKey": "request:d4e5...:created",
  "requestId": "d4e5...", "riderId": "3f1a2b4c-...", "mode": "RIDE", "vehicleCategory": "CAB",
  "seatsRequested": 1, "estFare": 145.00, "estDistanceKm": 6.2, "expiresAt": "2026-07-28T09:34:50Z"
}
```

**Versioning:** BACKWARD — add optional `estFareCurrency` → `1.1`. Breaking — none envisioned.

---

### 3.7 `ServiceRequestMatched`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.request.matched.v1` |
| Producer | dispatch-service |
| Consumers | trip-service — `trip-service.history-projection.v1` |
| Partition key | `request_id` |
| Idempotency key | `request:<requestId>:matched` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-08 step 4 (RIDE/WALK, same transaction as accept) / F-10 step 8 hop (CARPOOL, applied on `BookingConfirmed`) |
| Transaction/outbox | Either the accept transaction (§3.2) or, for CARPOOL, a dedicated idempotent `UPDATE service_request SET status='MATCHED' WHERE status='SEARCHING'` driven by consuming `BookingConfirmed` + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `requestId` | UUID | no | `service_request.id` | |
| `matchedProviderId` | UUID | no | `derived` — the winning `driver_id` (RIDE/WALK) or `trip.provider_id` (CARPOOL) | not a `service_request` column; carried for convenience |

**Example**
```json
{
  "eventId": "3eef...", "eventType": "ServiceRequestMatched", "schemaVersion": "1.0",
  "correlationId": "d001...", "causationId": "e9fa...", "occurredAt": "2026-07-28T09:30:11Z",
  "producerService": "dispatch-service", "aggregateType": "ServiceRequest", "aggregateId": "d4e5...",
  "idempotencyKey": "request:d4e5...:matched",
  "requestId": "d4e5...", "matchedProviderId": "7e2b..."
}
```

**Versioning:** BACKWARD — add optional `matchedAt` (distinct from `occurredAt` if ever needed) → `1.1`.
Breaking — none envisioned.

---

### 3.8 `ServiceRequestExpired`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.request.expired.v1` |
| Producer | dispatch-service |
| Consumers | trip-service — `trip-service.history-projection.v1`; realtime-gateway — `realtime-gateway.offer-countdown-fanout.v1` |
| Partition key | `request_id` |
| Idempotency key | `request:<requestId>:expired` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-14 step 3 |
| Transaction/outbox | dispatch-service's sweep `UPDATE ... WHERE status='SEARCHING' AND expires_at <= now()` (idempotent) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `requestId` | UUID | no | `service_request.id` | |
| `riderId` | UUID | no | `service_request.rider_id` | so the rider's client can be told directly |
| `finalStatus` | string | no | `service_request.status` | `EXPIRED` \| `NO_MATCH` |
| `closedAt` | string (ts) | no | `service_request.closed_at` | |

**Example**
```json
{
  "eventId": "4ff0...", "eventType": "ServiceRequestExpired", "schemaVersion": "1.0",
  "correlationId": "d445...", "causationId": null, "occurredAt": "2026-07-28T09:34:51Z",
  "producerService": "dispatch-service", "aggregateType": "ServiceRequest", "aggregateId": "d5f6...",
  "idempotencyKey": "request:d5f6...:expired",
  "requestId": "d5f6...", "riderId": "3f1a2b4c-...", "finalStatus": "NO_MATCH", "closedAt": "2026-07-28T09:34:51Z"
}
```

**Versioning:** BACKWARD — add optional `roundsAttempted` → `1.1`. Breaking — none envisioned.

---

### 3.9 `ServiceRequestCancelled`

| Aspect | Value |
|---|---|
| Topic | `tutem.dispatch.request.cancelled.v1` |
| Producer | dispatch-service |
| Consumers | trip-service — `trip-service.history-projection.v1` |
| Partition key | `request_id` |
| Idempotency key | `request:<requestId>:cancelled` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-17 step 1 (rider, pre-match); also the derived cleanup from `AppUserDeleted` (§1.3) |
| Transaction/outbox | dispatch-service's `service_request.status='CANCELLED'`+`closed_at` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `requestId` | UUID | no | `service_request.id` | |
| `riderId` | UUID | no | `service_request.rider_id` | |
| `cancelledBy` | string | no | `derived` — `RIDER` \| `SYSTEM` | mirrors `booking.cancelled_by`'s vocabulary; `service_request` itself has no such column, so this is computed by the writing code path from the request context |

**Example**
```json
{
  "eventId": "5001...", "eventType": "ServiceRequestCancelled", "schemaVersion": "1.0",
  "correlationId": "d556...", "causationId": null, "occurredAt": "2026-07-28T09:32:00Z",
  "producerService": "dispatch-service", "aggregateType": "ServiceRequest", "aggregateId": "d6a7...",
  "idempotencyKey": "request:d6a7...:cancelled",
  "requestId": "d6a7...", "riderId": "3f1a2b4c-...", "cancelledBy": "RIDER"
}
```

**Versioning:** BACKWARD — add optional `cancelReasonCode` → `1.1`. Breaking — none envisioned.

---

## 4. Trip domain (trip-service)

### 4.1 `TripCreated`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.trip.created.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.carpool-matching.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` (= `trip.id`, own aggregate id — no exception here; the exception is `booking`, not `trip`) |
| Idempotency key | `trip:<tripId>:created` |
| Ordering | **Yes** — one trip's whole lifecycle stays on one partition |
| `schemaVersion` | `1.0` |
| Trigger | F-08 step 5 (RIDE) / F-10 step 2 (CARPOOL — **this is the offer**, D6/D7) / F-12 step 6 (WALK) |
| Transaction/outbox | trip-service's `trip` INSERT + outbox row (RIDE: same transaction as `booking` insert, §4.6; CARPOOL: trip-only, no booking yet, D7's trip-before-booking sequence) |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `tripId` | UUID | no | `trip.id` | |
| `mode` | string | no | `trip.mode` | `RIDE`\|`CARPOOL`\|`WALK` |
| `providerId` | UUID | no | `trip.provider_id` | |
| `vehicleId` | UUID | yes | `trip.vehicle_id` | `null` for `WALK` (`ck_trip_walk_veh`) |
| `status` | string | no | `trip.status` | `ASSIGNED` |
| `seatsTotal` | int | no | `trip.seats_total` | `1` for RIDE/WALK (`ck_trip_solo`) |
| `seatsBooked` | int | no | `trip.seats_booked` | `0` at creation for CARPOOL, `1` for RIDE/WALK (created alongside the first booking) |

Excluded (§11): `origin_point`/`destination_point`/`route_line` raw geometry. A consumer needing the corridor
(dispatch-service's matching module, F-10 step 4) fetches it via trip-service's internal read API, per §2.5's
ownership note (dispatch-service never re-derives geometry from an event payload) — the event only signals
*that* a new carpool offer exists and *where* to go fetch it.

> **§10-blocked field:** `departureTime` is **not** in this payload. Baseline §10 item 1
> (`trip.departure_time`) is proposed, not approved. Until approved, `TripCreated` cannot carry a departure
> time, and F-10 step 4's departure-window ranking degrades to corridor-only matching, exactly as the
> baseline's own degradation clause states.

**Example**
```json
{
  "eventId": "6112...", "eventType": "TripCreated", "schemaVersion": "1.0",
  "correlationId": "d667...", "causationId": "e9fa...", "occurredAt": "2026-07-28T09:30:12Z",
  "producerService": "trip-service", "aggregateType": "Trip", "aggregateId": "f7b8...",
  "idempotencyKey": "trip:f7b8...:created",
  "tripId": "f7b8...", "mode": "RIDE", "providerId": "7e2b...", "vehicleId": "9a11...",
  "status": "ASSIGNED", "seatsTotal": 1, "seatsBooked": 1
}
```

**Versioning:** BACKWARD — once §10 item 1 is approved, `departureTime` is added as an **optional, nullable**
field with no default requirement change → `1.1`, a textbook additive change. Breaking — removing `mode` (any
consumer branching on it breaks) forces `v2`.

---

### 4.2 `TripStarted`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.trip.started.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `trip:<tripId>:started` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-09 step 2 / F-10 step 13 / F-11 step 1 |
| Transaction/outbox | trip-service's `trip.status='ACTIVE'`+`started_at` UPDATE (`ck_trip_active`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `tripId` | UUID | no | `trip.id` | |
| `startedAt` | string (ts) | no | `trip.started_at` | |

**Example**
```json
{
  "eventId": "7223...", "eventType": "TripStarted", "schemaVersion": "1.0",
  "correlationId": "d778...", "causationId": null, "occurredAt": "2026-07-28T09:45:00Z",
  "producerService": "trip-service", "aggregateType": "Trip", "aggregateId": "f7b8...",
  "idempotencyKey": "trip:f7b8...:started",
  "tripId": "f7b8...", "startedAt": "2026-07-28T09:45:00Z"
}
```

**Versioning:** BACKWARD — add optional `startOtpVerifiedVia` → `1.1`. Breaking — none envisioned.

**Consumer effect (dispatch-service):** idempotent `UPDATE service_request SET status='ONGOING' WHERE
status='MATCHED'` — same shape as F-08's hop, safe at-least-once.

---

### 4.3 `TripCompleted`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.trip.completed.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; driver-service — `driver-service.total-trips-increment.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `trip:<tripId>:completed` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-09 step 4 / F-10 step 13 / F-11 step 5 |
| Transaction/outbox | trip-service's `trip.status='COMPLETED'`+`ended_at`+`distance_km` UPDATE (`ck_trip_done`, `ck_trip_times`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `tripId` | UUID | no | `trip.id` | |
| `providerId` | UUID | no | `trip.provider_id` | needed by driver-service's counter consumer |
| `endedAt` | string (ts) | no | `trip.ended_at` | |
| `distanceKm` | decimal | yes | `trip.distance_km` | |

**Example**
```json
{
  "eventId": "8334...", "eventType": "TripCompleted", "schemaVersion": "1.0",
  "correlationId": "d889...", "causationId": null, "occurredAt": "2026-07-28T10:05:00Z",
  "producerService": "trip-service", "aggregateType": "Trip", "aggregateId": "f7b8...",
  "idempotencyKey": "trip:f7b8...:completed",
  "tripId": "f7b8...", "providerId": "7e2b...", "endedAt": "2026-07-28T10:05:00Z", "distanceKm": 6.4
}
```

**Versioning:** BACKWARD — add optional `actualVsEstFareDeltaPct` (derived) → `1.1`. Breaking — none
envisioned.

**Flagged risk (baseline §16.4 item 1/2, carried forward, not resolved here):** driver-service's
`driver.total_trips` increment on this event has **no database unique-index backstop**. Mitigation is
`tutem:ops:idem:driver-service.total-trips-increment.v1:<eventId>` (Redis, TTL > max retry window); a
Redis loss concurrent with a redelivery remains a narrow, documented double-count risk, unchanged from Step
3's own carry-forward.

---

### 4.4 `TripCancelled`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.trip.cancelled.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `trip:<tripId>:cancelled` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-17 step 2 (RIDE/WALK) / step 6 (whole CARPOOL trip) |
| Transaction/outbox | trip-service's `trip.status='CANCELLED'` UPDATE + (CARPOOL) cascading every live `booking` to `CANCELLED` in the same transaction + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `tripId` | UUID | no | `trip.id` | |
| `cancelledBy` | string | no | `trip.cancelled_by` | `RIDER`\|`PROVIDER`\|`SYSTEM` |
| `cancelReason` | string | yes | `trip.cancel_reason` | short code, not free text |

**Example**
```json
{
  "eventId": "9445...", "eventType": "TripCancelled", "schemaVersion": "1.0",
  "correlationId": "d99a...", "causationId": null, "occurredAt": "2026-07-28T09:50:00Z",
  "producerService": "trip-service", "aggregateType": "Trip", "aggregateId": "f8c9...",
  "idempotencyKey": "trip:f8c9...:cancelled",
  "tripId": "f8c9...", "cancelledBy": "PROVIDER", "cancelReason": "VEHICLE_BREAKDOWN"
}
```

**Versioning:** BACKWARD — add optional `affectedBookingCount` (derived, CARPOOL only) → `1.1`. Breaking —
none envisioned.

---

### 4.5 `TripSeatsExhausted`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.trip.seats-exhausted.v1` |
| Producer | trip-service |
| Consumers | realtime-gateway — `realtime-gateway.carpool-seat-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `trip:<tripId>:seats-exhausted:<seatsBooked>` — the sequence suffix disambiguates the case where a trip fills, frees a seat, and fills again |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-10 step 11 |
| Transaction/outbox | **Derived fact, D6 — `seats_booked == seats_total` is computed, never persisted as a status.** Published in the same transaction as the `booking` INSERT that made the equality true; the outbox row's payload is computed at write time, not read back from any `FULL` column (none exists). |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `tripId` | UUID | no | `trip.id` | |
| `seatsTotal` | int | no | `trip.seats_total` | |
| `seatsBooked` | int | no | `trip.seats_booked` | equals `seatsTotal` at publish time — the entire content of "full" |
| `full` | boolean | no | **derived** — `seats_booked == seats_total` | always `true` on this topic; **published again with `full: false`** the moment a cancellation frees a seat (F-10 step 14, "step 11 in reverse") — same topic, same event class, opposite boolean, so realtime-gateway's single consumer handles both directions |

**Example**
```json
{
  "eventId": "a556...", "eventType": "TripSeatsExhausted", "schemaVersion": "1.0",
  "correlationId": "da0a...", "causationId": null, "occurredAt": "2026-07-28T11:00:00Z",
  "producerService": "trip-service", "aggregateType": "Trip", "aggregateId": "f9d0...",
  "idempotencyKey": "trip:f9d0...:seats-exhausted:4",
  "tripId": "f9d0...", "seatsTotal": 4, "seatsBooked": 4, "full": true
}
```

**Versioning:** BACKWARD — add optional `waitlistEligible` (future feature flag) → `1.1`. Breaking — none
envisioned; this event's whole payload is already minimal by design.

---

### 4.6 `BookingConfirmed`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.confirmed.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.carpool-seat-fanout.v1` |
| Partition key | `trip_id` (§0.6 exception 2 — co-locates every booking with its trip's own lifecycle events) |
| Idempotency key | `booking:<bookingId>:confirmed` — **not derivable from topic+record key** (key is `trip_id`, not `booking_id`); read from the envelope |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-08 step 5 (RIDE) / F-10 step 8 (CARPOOL) / F-12 step 6 (WALK) |
| Transaction/outbox | trip-service's atomic reservation: check offer open → check seats → increment `seats_booked` → INSERT `booking` → commit, `ck_trip_seats` as backstop (NewSchema §5.1-1) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | = record key |
| `requestId` | UUID | no | `booking.request_id` | `uq_booking_request` — a request is booked exactly once, ever |
| `riderId` | UUID | no | `booking.rider_id` (via `request_id` join, or a denormalised column if present) | |
| `seats` | int | no | `booking.seats` | |
| `pickupOrder` | int | yes | `booking.pickup_order` | CARPOOL only. **Flagged, not fixed here:** baseline §16.4 item 3 — no `uq_booking_pickup_order` backstop exists (§10 item 3, proposed, not approved), so two concurrent bookings can share a value; consumers must treat this field as **a hint, not a key** (F-11 step 2), exactly as the baseline states. |
| `fareAmount` | decimal | no | `booking.fare_amount` | |
| `status` | string | no | `booking.status` | `CONFIRMED` |

**Example**
```json
{
  "eventId": "b667...", "eventType": "BookingConfirmed", "schemaVersion": "1.0",
  "correlationId": "db1b...", "causationId": null, "occurredAt": "2026-07-28T09:41:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c1d2...",
  "idempotencyKey": "booking:c1d2...:confirmed",
  "bookingId": "c1d2...", "tripId": "f9d0...", "requestId": "d7a1...", "riderId": "3f1a2b4c-...",
  "seats": 2, "pickupOrder": 3, "fareAmount": 60.00, "status": "CONFIRMED"
}
```

**Versioning:** BACKWARD — once §10 item 4 (`booking.tip_amount`) is approved, add it as optional/nullable →
`1.1`. **§10-blocked field:** `tipAmount` is **not** in this payload today (D5's optional post-trip tip
cannot be persisted or carried until §10 item 4 is approved). Breaking — removing `requestId` (the field
`uq_booking_request` traceability depends on) forces `v2`.

---

### 4.7 `BookingOnboard`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.onboard.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:onboard` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-09 step 2 / F-11 step 2 |
| Transaction/outbox | trip-service's `booking.status='ONBOARD'`+`picked_up_at` UPDATE (`ck_booking_onboard`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `pickedUpAt` | string (ts) | no | `booking.picked_up_at` | |

**Example**
```json
{
  "eventId": "c778...", "eventType": "BookingOnboard", "schemaVersion": "1.0",
  "correlationId": "dc2c...", "causationId": null, "occurredAt": "2026-07-28T09:46:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c1d2...",
  "idempotencyKey": "booking:c1d2...:onboard",
  "bookingId": "c1d2...", "tripId": "f9d0...", "pickedUpAt": "2026-07-28T09:46:00Z"
}
```

**Versioning:** BACKWARD — add optional `otpAttempts` → `1.1`. Breaking — none envisioned.

---

### 4.8 `BookingCompleted`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.completed.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.trip-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:completed` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-09 step 4 / F-11 step 4 |
| Transaction/outbox | trip-service's `booking.status='COMPLETED'`+`dropped_at` UPDATE (`ck_booking_done`, `ck_booking_order`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `droppedAt` | string (ts) | no | `booking.dropped_at` | |

**Example**
```json
{
  "eventId": "d889...", "eventType": "BookingCompleted", "schemaVersion": "1.0",
  "correlationId": "dd3d...", "causationId": null, "occurredAt": "2026-07-28T10:05:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c1d2...",
  "idempotencyKey": "booking:c1d2...:completed",
  "bookingId": "c1d2...", "tripId": "f9d0...", "droppedAt": "2026-07-28T10:05:00Z"
}
```

**Versioning:** BACKWARD — add optional `finalFareAmount` if it can ever differ post-hoc → `1.1`. Breaking —
none envisioned.

---

### 4.9 `BookingCancelled`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.cancelled.v1` |
| Producer | trip-service |
| Consumers | dispatch-service — `dispatch-service.request-status-sync.v1`; realtime-gateway — `realtime-gateway.carpool-seat-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:cancelled` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-10 step 14 / F-17 steps 2, 5, 6 |
| Transaction/outbox | trip-service's `booking.status='CANCELLED'` UPDATE (`ck_booking_cancel`, `ck_booking_by`) + `trip.seats_booked` decrement, **same transaction** (F-10 step 14 — "the decrement must be in the same transaction... or the offer leaks seats") + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `seats` | int | no | `booking.seats` | how many seats were returned |
| `cancelledBy` | string | no | `booking.cancelled_by` | `RIDER`\|`PROVIDER`\|`SYSTEM` |
| `cancelReason` | string | yes | `booking.cancel_reason` | |

**Example**
```json
{
  "eventId": "e99a...", "eventType": "BookingCancelled", "schemaVersion": "1.0",
  "correlationId": "de4e...", "causationId": null, "occurredAt": "2026-07-28T09:50:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c2e3...",
  "idempotencyKey": "booking:c2e3...:cancelled",
  "bookingId": "c2e3...", "tripId": "f9d0...", "seats": 1, "cancelledBy": "RIDER", "cancelReason": "PLAN_CHANGED"
}
```

**Versioning:** BACKWARD — add optional `seatsRemainingAfter` (derived) → `1.1`. Breaking — none envisioned.

---

### 4.10 `BookingNoShow`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.no-show.v1` |
| Producer | trip-service |
| Consumers | realtime-gateway — `realtime-gateway.carpool-seat-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:no-show` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-11 step 3 |
| Transaction/outbox | trip-service's `booking.status='NO_SHOW'` UPDATE + `trip.seats_booked` decrement (if the trip has not passed that point) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `seats` | int | no | `booking.seats` | |
| `resold` | boolean | no | **derived** | whether the seat was freed for resale (trip position dependent) |

**Example**
```json
{
  "eventId": "faab...", "eventType": "BookingNoShow", "schemaVersion": "1.0",
  "correlationId": "df5f...", "causationId": null, "occurredAt": "2026-07-28T09:47:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c3f4...",
  "idempotencyKey": "booking:c3f4...:no-show",
  "bookingId": "c3f4...", "tripId": "f9d0...", "seats": 1, "resold": true
}
```

**Versioning:** BACKWARD — add optional `graceWindowSeconds` → `1.1`. Breaking — none envisioned.

---

### 4.11 `BookingPaid`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.paid.v1` |
| Producer | trip-service |
| Consumers | realtime-gateway — `realtime-gateway.payment-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:paid` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-16 step 2 (CASH) / step 3 (gateway callback success) |
| Transaction/outbox | trip-service's `payment_status='PAID'` UPDATE (`ck_booking_paid`, `ck_booking_cash`) + outbox row. Gateway callbacks are de-duplicated on `payment_ref` before this write, per Step 3 §3. |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `fareAmount` | decimal | no | `booking.fare_amount` | |
| `paymentMethod` | string | no | `booking.payment_method` | `CASH`\|`UPI`\|`CARD`\|`WALLET` — **applies uniformly to `WALK` too, D5** |

Excluded (§11): `payment_ref` (gateway transaction token) — never crosses the bus; reconciliation stays
inside trip-service's `payment` module, which is the only reader of that column.

**Example**
```json
{
  "eventId": "0bbc...", "eventType": "BookingPaid", "schemaVersion": "1.0",
  "correlationId": "e060...", "causationId": null, "occurredAt": "2026-07-28T10:06:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c1d2...",
  "idempotencyKey": "booking:c1d2...:paid",
  "bookingId": "c1d2...", "tripId": "f9d0...", "fareAmount": 60.00, "paymentMethod": "UPI"
}
```

**Versioning:** BACKWARD — once §10 item 4 (`booking.tip_amount`) is approved, add `tipAmount` as
optional/nullable → `1.1`. **§10-blocked field:** `tipAmount` is **not** in this payload today — F-16 step 5
/ F-18 step 1 both state the tip is not implementable until that column exists; the rating+tip prompt is
still pushed (via `SendPushCommand`) but the tip itself has nowhere to be recorded or carried, an explicit,
visible degradation rather than a silent gap. Breaking — none envisioned for the fields that do exist.

---

### 4.12 `BookingPaymentFailed`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.payment-failed.v1` |
| Producer | trip-service |
| Consumers | realtime-gateway — `realtime-gateway.payment-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:payment-failed:<occurredAt-iso>` — suffixed because a booking can retry payment more than once |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-16 step 3 (gateway callback failure) |
| Transaction/outbox | trip-service's `payment_status='FAILED'` UPDATE + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `paymentMethod` | string | no | `booking.payment_method` | |

**Example**
```json
{
  "eventId": "1ccd...", "eventType": "BookingPaymentFailed", "schemaVersion": "1.0",
  "correlationId": "e171...", "causationId": null, "occurredAt": "2026-07-28T10:06:30Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c4a5...",
  "idempotencyKey": "booking:c4a5...:payment-failed:2026-07-28T10:06:30Z",
  "bookingId": "c4a5...", "tripId": "faa1...", "paymentMethod": "CARD"
}
```

**Versioning:** BACKWARD — add optional `gatewayErrorCode` (non-identifying category, not `payment_ref`) →
`1.1`. Breaking — none envisioned.

---

### 4.13 `BookingRefunded`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.booking.refunded.v1` |
| Producer | trip-service |
| Consumers | realtime-gateway — `realtime-gateway.payment-status-fanout.v1` |
| Partition key | `trip_id` |
| Idempotency key | `booking:<bookingId>:refunded` |
| Ordering | **Yes** |
| `schemaVersion` | `1.0` |
| Trigger | F-16 step 4 |
| Transaction/outbox | trip-service's `payment_status='REFUNDED'` UPDATE + outbox row. **No partial-refund semantics** (NewSchema §6) — a refund is always the full `fare_amount`. |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `bookingId` | UUID | no | `booking.id` | |
| `tripId` | UUID | no | `booking.trip_id` | |
| `fareAmount` | decimal | no | `booking.fare_amount` | the full refunded amount |

**Example**
```json
{
  "eventId": "2dde...", "eventType": "BookingRefunded", "schemaVersion": "1.0",
  "correlationId": "e282...", "causationId": null, "occurredAt": "2026-07-28T10:10:00Z",
  "producerService": "trip-service", "aggregateType": "Booking", "aggregateId": "c4a5...",
  "idempotencyKey": "booking:c4a5...:refunded",
  "bookingId": "c4a5...", "tripId": "faa1...", "fareAmount": 60.00
}
```

**Versioning:** BACKWARD — add optional `refundReasonCode` → `1.1`. Breaking — introducing partial-refund
amounts (`refundedAmount != fareAmount`) would change the field's meaning for every existing consumer and
forces `v2`.

---

### 4.14 `RatingSubmitted`

| Aspect | Value |
|---|---|
| Topic | `tutem.trip.rating.submitted.v1` |
| Producer | trip-service |
| Consumers | identity-service — `identity-service.rating-average-recompute.v1`; driver-service — `driver-service.rating-average-recompute.v1` |
| Partition key | `booking_id` (§0.6: keeps both directions of one booking's ratings ordered) |
| Idempotency key | `rating:<ratingId>:submitted` |
| Ordering | **Yes** — both raters' submissions for one booking stay ordered; ordering across *different* bookings is order-independent (Step 3 §5.2) |
| `schemaVersion` | `1.0` |
| Trigger | F-18 step 2 |
| Transaction/outbox | trip-service's `rating` INSERT (`uq_rating_once`, `ck_rating_self`) + outbox row |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `ratingId` | UUID | no | `rating.id` | |
| `bookingId` | UUID | no | `rating.booking_id` | = record key |
| `raterId` | UUID | no | `rating.rater_id` | |
| `rateeId` | UUID | no | `rating.ratee_id` | |
| `rateeType` | string | no | **derived** — `USER` \| `DRIVER` | not a `rating` column; computed from whether the ratee has a `driver` row, so the correct consumer (identity vs driver) can act without a lookup |
| `score` | int | no | `rating.score` | 1–5 |

Excluded (§11): `comment` free text — potentially identifying; kept in Postgres only, read via a sync API if
ever surfaced.

**Example**
```json
{
  "eventId": "3eef...", "eventType": "RatingSubmitted", "schemaVersion": "1.0",
  "correlationId": "e393...", "causationId": null, "occurredAt": "2026-07-28T10:07:00Z",
  "producerService": "trip-service", "aggregateType": "Rating", "aggregateId": "d5b6...",
  "idempotencyKey": "rating:d5b6...:submitted",
  "ratingId": "d5b6...", "bookingId": "c1d2...", "raterId": "3f1a2b4c-...", "rateeId": "7e2b...",
  "rateeType": "DRIVER", "score": 5
}
```

**Versioning:** BACKWARD — add optional `hasComment` boolean (without the text itself) → `1.1`. Breaking —
none envisioned.

---

## 5. Config domain (config-service)

### 5.1 `SystemConfigChanged`

| Aspect | Value |
|---|---|
| Topic | `tutem.config.config.changed.v1` (**compacted**) |
| Producer | config-service |
| Consumers | identity-service, driver-service, dispatch-service, trip-service, notification-service, api-gateway, realtime-gateway — each `<service-name>.config-snapshot-refresh.v1` |
| Partition key | `config_key` (§0.6 exception 3 — not a UUID; the table is ~10 rows, skew is irrelevant) |
| Idempotency key | `config:<configKey>:changed:<updatedAt-iso>` — `<aggregateId>` here is the **key string itself**, not a UUID, since `SystemConfig` is a key/value aggregate |
| Ordering | **Yes**, per key; current-value/compacted semantics, not an append-only stream |
| `schemaVersion` | `1.0` |
| Trigger | F-20 step 2–3 |
| Transaction/outbox | config-service's `system_config` UPDATE + Redis snapshot refresh + outbox row, one transaction for the DB write (the Redis refresh is config-service's own synchronous side effect, not itself Kafka-mediated) |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `configKey` | string | no | `system_config.key` | e.g. `driver.rejection.daily_threshold` |
| `value` | string | no | `system_config.value` | stored/transmitted as text; each consumer type-coerces per its own known key |
| `updatedAt` | string (ts) | no | `system_config.updated_at` | |

**Example**
```json
{
  "eventId": "4ff0...", "eventType": "SystemConfigChanged", "schemaVersion": "1.0",
  "correlationId": "e4a4...", "causationId": null, "occurredAt": "2026-07-28T13:00:00Z",
  "producerService": "config-service", "aggregateType": "SystemConfig", "aggregateId": "driver.rejection.daily_threshold",
  "idempotencyKey": "config:driver.rejection.daily_threshold:changed:2026-07-28T13:00:00Z",
  "configKey": "driver.rejection.daily_threshold", "value": "3", "updatedAt": "2026-07-28T13:00:00Z"
}
```

**Versioning:** BACKWARD — add optional `updatedByAdminId` → `1.1`. Breaking — none envisioned; compacted
topics are especially sensitive to breaking changes because old keys are never re-read from a `delete`
window, so any breaking change here would require a coordinated re-key migration, not just a `v2` cutover.

**Invariant preserved (F-20 step 4):** already-issued `driver_blacklist` rows keep their **snapshotted**
`rejection_count`/`threshold` — this event changing `value` never rewrites history, only future evaluations.

---

## 6. Command event (multi-producer)

### 6.1 `SendPushCommand`

| Aspect | Value |
|---|---|
| Topic | `tutem.notification.send-push.command.v1` |
| Producers (multi, §0.2.3 + D3 extension) | dispatch-service (F-07.7, F-08.6, F-10.6, F-13.5, F-14.3, **F-17.1, F-17.5**); trip-service (F-09.5, F-10.10, **F-17.6**); driver-service (F-03.8, F-15.3) |
| Consumer | notification-service — `notification-service.push-delivery.v1` (the **sole** executing consumer, §0.2.3) |
| Partition key | `user_id` (per-recipient ordering, avoids an out-of-order push — §0.6) |
| Idempotency key | `notify:<userId>:<templateCode>:<eventId>` — deliberately keyed off the **causing** `eventId`, not a `SendPushCommand`-specific aggregate id, since this topic has no aggregate slot |
| Ordering | Per-recipient; matters only in that a stale "offer gone" must never arrive before "offer created" for the same user — guaranteed by the producing service publishing both from the same ordered transaction sequence on its own topic, then this command derivatively |
| `schemaVersion` | `1.0` |
| Trigger | any of the 12 call-sites listed above |
| Transaction/outbox | published from the **producing service's own outbox**, in the same transaction as the domain fact that motivated it (e.g. dispatch-service's F-08 accept transaction also inserts the `SendPushCommand` outbox row alongside `RideOfferAccepted`'s) |

**Payload fields**

| Field | Type | Nullable | Source column | Notes |
|---|---|---|---|---|
| `userId` | UUID | no | the recipient's `app_user.id` / `driver.user_id` | = record key |
| `templateCode` | string | no | **derived** | e.g. `OFFER_ACCEPTED_WINNER`, `OFFER_LOST_RACE`, `DOCUMENT_VERIFIED`, `BLACKLIST_APPLIED`, `REQUEST_EXPIRED`, `BOOKING_CANCELLED_PROVIDER_NOTICE` — not a stored column; a fixed vocabulary owned by `tutem-common` |
| `causingEventId` | UUID | no | the `eventId` of the domain fact this command renders | lets notification-service de-dup and lets an operator trace a push back to its cause |
| `causingEventType` | string | no | the `eventType` of the same | rendering hint |
| `contextIds` | map<string,string> | yes | e.g. `{"offerId": "...", "tripId": "..."}` | identifiers only — **never** free text, address, or PII per §11 |

Excluded (§11): any rendered message text, phone number, or address is composed by notification-service
itself from `templateCode` + localisation, never carried on the wire.

**Example**
```json
{
  "eventId": "5001...", "eventType": "SendPushCommand", "schemaVersion": "1.0",
  "correlationId": "d001...", "causationId": "e9fa...", "occurredAt": "2026-07-28T09:30:11Z",
  "producerService": "dispatch-service",
  "idempotencyKey": "notify:7e2b...:OFFER_ACCEPTED_WINNER:e9fa...",
  "userId": "7e2b...", "templateCode": "OFFER_ACCEPTED_WINNER", "causingEventId": "e9fa...",
  "causingEventType": "RideOfferAccepted", "contextIds": {"offerId": "e5f6...", "requestId": "d4e5..."}
}
```

Note: `aggregateType`/`aggregateId` are **absent**, per §0.10 — command topics carry no aggregate slot.

**Versioning:** BACKWARD — add optional `priority` (`HIGH`\|`NORMAL`) → `1.1`. Breaking — changing
`contextIds` from a flat map to a nested per-domain object forces `v2`, since every producer's serialization
would need to change simultaneously.

**Delivery mechanics (D3):** this command drives the **FCM** leg only. The **WebSocket** leg for the same
underlying fact is delivered independently by realtime-gateway consuming the *domain* topic directly (e.g.
`RideOfferAccepted` itself, not this command) — both legs carry the domain fact's original `eventId`
(copied into `causingEventId` here and into the WebSocket payload verbatim), which is what the Flutter
client's de-dup relies on. No server-side coordination between the two consumers exists or is needed.

---

## 7. Catalogue tables

### 7.1 Event → topic → producer → consumers

| Event class | Topic | Producer | Consumers |
|---|---|---|---|
| AppUserRegistered | `tutem.identity.user.registered.v1` | identity-service | trip-service |
| AppUserProfileUpdated | `tutem.identity.user.profile-updated.v1` | identity-service | trip-service |
| AppUserDeleted | `tutem.identity.user.deleted.v1` | identity-service | dispatch-service, trip-service |
| DriverCreated | `tutem.driver.driver.created.v1` | driver-service | notification-service |
| DriverWentOnline | `tutem.driver.driver.went-online.v1` | driver-service | realtime-gateway |
| DriverWentOffline | `tutem.driver.driver.went-offline.v1` | driver-service | realtime-gateway |
| DriverLocationUpdated | `tutem.driver.driver.location-updated.v1` | driver-service | driver-service, realtime-gateway |
| VehicleRegistered | `tutem.driver.vehicle.registered.v1` | driver-service | trip-service |
| VehicleDeactivated | `tutem.driver.vehicle.deactivated.v1` | driver-service | trip-service |
| DriverDocumentSubmitted | `tutem.driver.document.submitted.v1` | driver-service | driver-service |
| DriverDocumentVerified | `tutem.driver.document.verified.v1` | driver-service | driver-service |
| DriverDocumentRejected | `tutem.driver.document.rejected.v1` | driver-service | driver-service |
| DriverBlacklistApplied | `tutem.driver.blacklist.blacklist-applied.v1` | driver-service | driver-service |
| DriverBlacklistExpired | `tutem.driver.blacklist.blacklist-expired.v1` | driver-service | driver-service |
| RideOfferCreated | `tutem.dispatch.offer.created.v1` | dispatch-service | realtime-gateway |
| RideOfferAccepted | `tutem.dispatch.offer.accepted.v1` | dispatch-service | realtime-gateway, trip-service |
| RideOfferRejected | `tutem.dispatch.offer.rejected.v1` | dispatch-service | driver-service |
| RideOfferExpired | `tutem.dispatch.offer.expired.v1` | dispatch-service | driver-service, realtime-gateway |
| RideOfferWithdrawn | `tutem.dispatch.offer.withdrawn.v1` | dispatch-service | realtime-gateway |
| ServiceRequestCreated | `tutem.dispatch.request.created.v1` | dispatch-service | trip-service |
| ServiceRequestMatched | `tutem.dispatch.request.matched.v1` | dispatch-service | trip-service |
| ServiceRequestExpired | `tutem.dispatch.request.expired.v1` | dispatch-service | trip-service, realtime-gateway |
| ServiceRequestCancelled | `tutem.dispatch.request.cancelled.v1` | dispatch-service | trip-service |
| TripCreated | `tutem.trip.trip.created.v1` | trip-service | dispatch-service, realtime-gateway |
| TripStarted | `tutem.trip.trip.started.v1` | trip-service | dispatch-service, realtime-gateway |
| TripCompleted | `tutem.trip.trip.completed.v1` | trip-service | dispatch-service, driver-service, realtime-gateway |
| TripCancelled | `tutem.trip.trip.cancelled.v1` | trip-service | dispatch-service, realtime-gateway |
| TripSeatsExhausted | `tutem.trip.trip.seats-exhausted.v1` | trip-service | realtime-gateway |
| BookingConfirmed | `tutem.trip.booking.confirmed.v1` | trip-service | dispatch-service, realtime-gateway |
| BookingOnboard | `tutem.trip.booking.onboard.v1` | trip-service | dispatch-service, realtime-gateway |
| BookingCompleted | `tutem.trip.booking.completed.v1` | trip-service | dispatch-service, realtime-gateway |
| BookingCancelled | `tutem.trip.booking.cancelled.v1` | trip-service | dispatch-service, realtime-gateway |
| BookingNoShow | `tutem.trip.booking.no-show.v1` | trip-service | realtime-gateway |
| BookingPaid | `tutem.trip.booking.paid.v1` | trip-service | realtime-gateway |
| BookingPaymentFailed | `tutem.trip.booking.payment-failed.v1` | trip-service | realtime-gateway |
| BookingRefunded | `tutem.trip.booking.refunded.v1` | trip-service | realtime-gateway |
| RatingSubmitted | `tutem.trip.rating.submitted.v1` | trip-service | identity-service, driver-service |
| SystemConfigChanged | `tutem.config.config.changed.v1` | config-service | identity-service, driver-service, dispatch-service, trip-service, notification-service, api-gateway, realtime-gateway |
| SendPushCommand | `tutem.notification.send-push.command.v1` | dispatch-service, trip-service, driver-service | notification-service |

**39/39 topics from Deliverable A appear above; no topic is unused, no event's topic is missing from
Deliverable A, and no event class name or topic name repeats.**

### 7.2 Consumer group → topics subscribed

| Consumer group | Topics |
|---|---|
| `driver-service.geo-index-maintenance.v1` | `tutem.driver.driver.location-updated.v1` |
| `driver-service.parivahan-verification.v1` | `tutem.driver.document.submitted.v1` |
| `driver-service.verification-status-recompute.v1` | `tutem.driver.document.verified.v1`, `tutem.driver.document.rejected.v1` |
| `driver-service.blacklist-geo-sync.v1` | `tutem.driver.blacklist.blacklist-applied.v1`, `tutem.driver.blacklist.blacklist-expired.v1` |
| `driver-service.blacklist-evaluation.v1` | `tutem.dispatch.offer.rejected.v1`, `tutem.dispatch.offer.expired.v1` |
| `driver-service.total-trips-increment.v1` | `tutem.trip.trip.completed.v1` |
| `driver-service.rating-average-recompute.v1` | `tutem.trip.rating.submitted.v1` |
| `driver-service.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `identity-service.rating-average-recompute.v1` | `tutem.trip.rating.submitted.v1` |
| `identity-service.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `dispatch-service.user-deletion-cleanup.v1` | `tutem.identity.user.deleted.v1` |
| `dispatch-service.carpool-matching.v1` | `tutem.trip.trip.created.v1` |
| `dispatch-service.request-status-sync.v1` | `tutem.trip.trip.started.v1`, `tutem.trip.trip.completed.v1`, `tutem.trip.trip.cancelled.v1`, `tutem.trip.booking.confirmed.v1`, `tutem.trip.booking.onboard.v1`, `tutem.trip.booking.completed.v1`, `tutem.trip.booking.cancelled.v1` |
| `dispatch-service.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `trip-service.rider-directory-cache.v1` | `tutem.identity.user.registered.v1`, `tutem.identity.user.profile-updated.v1`, `tutem.identity.user.deleted.v1` |
| `trip-service.vehicle-snapshot-cache.v1` | `tutem.driver.vehicle.registered.v1`, `tutem.driver.vehicle.deactivated.v1` |
| `trip-service.trip-provisioning.v1` | `tutem.dispatch.offer.accepted.v1` |
| `trip-service.history-projection.v1` | `tutem.dispatch.request.created.v1`, `tutem.dispatch.request.matched.v1`, `tutem.dispatch.request.expired.v1`, `tutem.dispatch.request.cancelled.v1` |
| `trip-service.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `notification-service.driver-welcome.v1` | `tutem.driver.driver.created.v1` |
| `notification-service.push-delivery.v1` | `tutem.notification.send-push.command.v1` |
| `notification-service.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `api-gateway.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |
| `realtime-gateway.driver-presence-fanout.v1` | `tutem.driver.driver.went-online.v1`, `tutem.driver.driver.went-offline.v1` |
| `realtime-gateway.driver-location-fanout.v1` | `tutem.driver.driver.location-updated.v1` |
| `realtime-gateway.offer-countdown-fanout.v1` | `tutem.dispatch.offer.created.v1`, `tutem.dispatch.offer.accepted.v1`, `tutem.dispatch.offer.expired.v1`, `tutem.dispatch.offer.withdrawn.v1`, `tutem.dispatch.request.expired.v1` |
| `realtime-gateway.trip-status-fanout.v1` | `tutem.trip.trip.created.v1`, `tutem.trip.trip.started.v1`, `tutem.trip.trip.completed.v1`, `tutem.trip.trip.cancelled.v1`, `tutem.trip.booking.onboard.v1`, `tutem.trip.booking.completed.v1` |
| `realtime-gateway.carpool-seat-fanout.v1` | `tutem.trip.trip.seats-exhausted.v1`, `tutem.trip.booking.confirmed.v1`, `tutem.trip.booking.cancelled.v1`, `tutem.trip.booking.no-show.v1` |
| `realtime-gateway.payment-status-fanout.v1` | `tutem.trip.booking.paid.v1`, `tutem.trip.booking.payment-failed.v1`, `tutem.trip.booking.refunded.v1` |
| `realtime-gateway.config-snapshot-refresh.v1` | `tutem.config.config.changed.v1` |

Every group name follows `<service-name>.<purpose>.v<major>` with `<purpose>` naming a use case, never a
topic echo (baseline §0.5) — e.g. `dispatch-service.request-status-sync.v1` handles seven different topics
under one use-case name rather than seven topic-named groups.

---

## 8. Self-check (per the Orchestrator's FINAL STEP instruction)

| Check | Result |
|---|---|
| Every topic in Deliverable A used by ≥1 event here | ✓ — §7.1 lists all 39 |
| Every event's topic exists in Deliverable A | ✓ — no topic name introduced here that isn't in `02-kafka-topics.md` §1 |
| No duplicate topic or event names | ✓ — 39 distinct topics, 39 distinct event classes (38 domain events + `SendPushCommand`) |
| Every event topic has exactly one producer | ✓ — all 38 domain-fact topics list a single `<domain>`-owning service; only the command topic has multiple, per the sole exception (§0.2.3) |
| Every event has ≥1 consumer | ✓ — verified per-section above and in §7.1 |
| Every partition key matches §0.6, including the 3 exceptions | ✓ — `offer`→`request_id` (§3.1–3.5), `booking`→`trip_id` (§4.6–4.13), `config`→`config_key` (§5.1); every other event keys on its own aggregate id |
| Every consumer group matches the grammar with a use-case purpose | ✓ — §7.2; no group name echoes a topic's `<event-name>` |

---

**End of Step 5.** Awaiting Orchestrator approval.
