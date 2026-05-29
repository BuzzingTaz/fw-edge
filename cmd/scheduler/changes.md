The **main difference** between the old and new implementation is not really the scheduler itself — the scheduler code is barely used in both versions.

The **real architectural change** is in how WebSocket connections and gRPC streams are managed.

The new implementation fixes major concurrency and multi-client design problems that existed in the old version.

---

# High-Level Difference

## Old Implementation

The old code uses:

* a **global websocket connection**
* shared state across all users
* loosely managed goroutines
* temporary connection handling
* potential race conditions

It behaves like:

> "There is one active websocket/gRPC pipeline for everyone."

---

## New Implementation

The new code creates:

* one websocket connection per client
* one gRPC stream per client
* isolated goroutines
* proper lifecycle cleanup
* connection-scoped state

It behaves like:

> "Each client has its own independent streaming session."

---

# Core Architectural Difference

---

# 1. Global vs Local Connection Ownership

---

## OLD

```go
var tempConn *websocket.Conn
```

This is the biggest issue.

Every client shares the same global websocket.

Inside `wsHandler`:

```go
tempConn, err = upgrader.Upgrade(w, r, nil)
```

Meaning:

* User A connects → tempConn = A
* User B connects → tempConn = B
* User A's connection is overwritten

Now all goroutines may accidentally write to B.

---

## NEW

```go
conn, err := upgrader.Upgrade(w, r, nil)
```

Connection is local to the handler.

Each request gets:

* its own websocket
* its own goroutines
* its own stream

No shared mutable state.

This is the correct concurrent server design.

---

# 2. Shared gRPC Stream vs Per-Client Stream

---

## OLD

Inside a goroutine:

```go
computeVideoStreamer, err := computeStreamClient.StreamVideo(ctx)
```

But the response handling uses global websocket state.

This creates coupling problems.

Inference results can go to the wrong user because:

```go
tempConn.WriteJSON(inferenceData)
```

always writes to the latest websocket.

---

## NEW

Each client gets:

```go
stream, err := computeStreamClient.StreamVideo(ctx)
```

AND:

```go
go ReadInferenceData(conn, stream)
```

Now:

* stream belongs to one client
* websocket belongs to one client
* inference results return to the correct user

This is true session isolation.

---

# 3. Goroutine Lifecycle Management

---

## OLD

```go
go func() {
    ...
}()
```

Nested goroutines are launched without strong ownership.

Problems:

* goroutines can leak
* streams may stay alive
* websocket disconnects don't fully cancel work
* cleanup is unclear

Also:

```go
ctx, cancel := context.WithTimeout(..., 300*time.Second)
```

Hardcoded timeout.

After 5 minutes:

* stream dies automatically
* even if client is still connected

Bad for long-running streams.

---

## NEW

Uses:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
```

This ties stream lifetime directly to connection lifetime.

When websocket disconnects:

* handler returns
* defer executes
* context cancels
* stream closes
* goroutines exit cleanly

This is proper structured concurrency.

---

# 4. Cleanup Handling

---

## OLD

Missing cleanup:

* websocket not explicitly closed
* stream not closed
* goroutines may survive disconnects

Example:

```go
// defer conn.Close()
```

Commented out.

Potential memory/resource leaks.

---

## NEW

Explicit cleanup:

```go
defer conn.Close()
defer stream.CloseSend()
defer cancel()
```

This guarantees:

* socket cleanup
* stream cleanup
* context cancellation

Much safer production behavior.

---

# 5. ReadInferenceData Design

---

## OLD

```go
func ReadInferenceData(stream ...)
```

Uses hidden global dependency:

```go
tempConn.WriteJSON(...)
```

This is bad design because the function:

* secretly depends on global state
* is not reusable
* is not testable
* is unsafe concurrently

---

## NEW

```go
func ReadInferenceData(
    conn *websocket.Conn,
    stream grpc.BidiStreamingClient...
)
```

Dependencies are explicit.

Benefits:

* pure ownership
* easier testing
* thread safety
* better abstraction

This is idiomatic Go.

---

# 6. Multi-User Scalability

---

## OLD

Not truly multi-user safe.

If multiple users connect:

* websocket overwrites happen
* inference routing breaks
* race conditions occur

The system effectively behaves like:

> "single active user"

---

## NEW

Fully supports concurrent users.

Each user has:

| Resource    | Ownership |
| ----------- | --------- |
| websocket   | isolated  |
| grpc stream | isolated  |
| goroutines  | isolated  |
| context     | isolated  |

This is scalable server architecture.

---

# 7. Error Handling Improvements

---

## OLD

Some incorrect logging:

```go
log.Println("Failed to create rpc from client: %v", err)
```

`Println` does not format `%v`.

Should have used:

```go
log.Printf(...)
```

---

## NEW

Consistent structured logging:

```go
log.Println("grpc send error:", err)
```

Cleaner and simpler.

---

# 8. Simpler Control Flow

---

## OLD

Had:

* nested goroutines
* temporary websocket state
* unnecessary timeout logic
* hidden dependencies

Harder to reason about.

---

## NEW

Linear flow:

1. websocket connects
2. grpc stream created
3. goroutine reads inference
4. main loop sends RTP
5. disconnect cleans everything

Much more maintainable.

---

# Scheduler-Specific Observation

Interestingly:

```go
frameScheduler := scheduler.NewScheduler(nil, nil)
```

is identical in both versions.

AND:

```go
// go frameScheduler.Run()
```

is still commented out in the old version.

So the scheduler itself was not materially changed.

The real improvement is:

# Connection/session architecture

not scheduler logic.

---

# Overall Design Evolution

---

## OLD DESIGN

Shared-state concurrent server.

Problems:

* race conditions
* wrong-user responses
* resource leaks
* poor scalability
* hidden dependencies

---

## NEW DESIGN

Session-oriented concurrent architecture.

Benefits:

* isolated clients
* proper cleanup
* scalable concurrency
* easier debugging
* safer goroutines
* production-grade lifecycle management

---

# In One Sentence

The new implementation converts the system from a fragile globally shared connection model into a proper per-client streaming architecture with isolated state, correct concurrency handling, and safe lifecycle management.
