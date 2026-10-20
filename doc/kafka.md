# How Kafka works in AriaBC — a visual walkthrough

**Follow the arrows first; use the short captions to explain what each step means.** The [full implementation reference](../docs/kafka/implementation-reference.md) retains exact settings, wire offsets, source links, and failure details.

These diagrams describe the implementation reviewed on **2026-10-15**. Example requests, offsets, and event positions explain the mechanism; they are not measured runtime traces.

| Start here | Then look inside | Finally compare behavior |
| --- | --- | --- |
| [1–5: architecture and routing](#1-three-jobs-in-the-system) | [6–16: records, batching, consumption and encoding](#6-what-is-inside-one-result-record) | [17–26: votes, completion, recovery and guarantees](#17-how-the-gateway-counts-votes) |

## 1. Three jobs in the system

![NuRaft orders SQL work, Kafka carries results, and KRaft manages broker metadata](../docs/kafka/diagrams/01-roles.svg)

**NuRaft establishes the transaction order. Kafka carries the replicas' reports back to the gateway. KRaft manages Kafka itself.** The gateway's completion mode decides how much result evidence to wait for.

## 2. Follow one request from submission to voting

![Request 42 is ordered, executed by database nodes 1, 2 and 4, and reported through Kafka](../docs/kafka/diagrams/02-one-request.svg)

All three database replicas execute the ordered work. Each reports its own outcome. The gateway groups those reports by request identity and compares their digests.

In `kafka-only` mode, the gateway broadcasts preordered work directly to the replicas with Raft bypassed. Results still return through Kafka. There is no Kafka input topic ordering the SQL in that path. [Source and mode selection](../docs/kafka/implementation-reference.md#2-components-and-end-to-end-flow).

## 3. Where the processes run

![Three database hosts each contain a database replica and a Kafka broker, with one metadata controller](../docs/kafka/diagrams/03-topology.svg)

**DB node 4 uses Kafka broker 3.** They are different ID spaces. The first host also runs the only Kafka metadata controller. This is the runner's default colocated layout; existing deployments must be inspected separately.

## 4. What a Kafka partition actually contains

![Three partitions have independent offsets and messages containing batches of result records](../docs/kafka/diagrams/04-partition-records.svg)

Request 42 can appear in all three partitions because three replicas report it. Its Kafka offsets can differ in every partition. One offset identifies a **Kafka message**, which can hold several transaction results.

The runner's `--replica-assignment 1,2,3` gives each partition one broker replica: **RF=1**. It does not create three durable copies of every result. [Topic setup details](../docs/kafka/implementation-reference.md#3-brokers-partitions-and-routing).

## 5. Why localhost bootstrap does not decide the final route

![A producer discovers metadata through localhost, then sends to the advertised partition leader](../docs/kafka/diagrams/05-routing.svg)

Bootstrap is an address book lookup. The producer then connects to the advertised leader of its selected partition. The normal colocated assignment puts that leader on the same host; gateway fetches still cross hosts.

For the alternative remote-hub layout, see [25: colocated versus remote](#25-colocated-versus-the-remote-hub).

## 6. What is inside one result record

![A result record combines request identity, an outcome digest and MAC, and an optional full result body](../docs/kafka/diagrams/06-outcome-record.svg)

The record says **which request, which replica, and which outcome digest**. A digest of a result or terminal outcome is different from a whole-database Merkle root. The runner normally omits full SQL result bodies and sends compact votes.

## 7. How many results become how many messages

![Executor batching groups SQL result records into Kafka messages, and librdkafka can batch those messages again](../docs/kafka/diagrams/07-two-batching-layers.svg)

The executor first packs result records into a payload. librdkafka may then pack several Kafka messages into a network request. So “64 results”, “2 Kafka messages”, and “1 network request” describe different layers; the pictured grouping is only an example.

## 8. Which result publisher runs

![The executor selects background publishing for eligible ledger-off execution and worker or event batching for safe mode](../docs/kafka/diagrams/08-publisher-choice.svg)

Normal eligible execution uses a background publisher. Safe ledger mode keeps publication in the worker/event-loop path so it can wait before marking the task applied. Disabling the background publisher still leaves batching in those other paths.

## 9. When a background batch is sent

![The publisher waits, drains results, checks count and byte thresholds, then sends or briefly waits for more](../docs/kafka/diagrams/09-coalescing.svg)

The publisher can send when a cap or soft target is reached, when the delay expires, during shutdown, or immediately at an idle tail. **32 is a soft target.** The default 150 µs is a coalescing budget, not a bound on total queueing or result latency.

## 10. What a successful send call means

![Successful producev enqueue returns before later broker processing and delivery callback](../docs/kafka/diagrams/10-enqueue-and-delivery.svg)

`send_payload() == true` means the local librdkafka queue accepted a copy. A later callback reports delivery success or failure. Even a successful broker acknowledgment is still separate from the gateway consuming the record and counting its vote.

## 11. The current flush-helper limitation

![Both successful and failed delivery callbacks decrement the pending-message counter](../docs/kafka/diagrams/11-flush-limit.svg)

Both branches remove pending work. The current `wait_for_delivery()` checks that flush completed and no deliveries remain; it does **not** check `delivery_errors`. That matters when interpreting safe-mode applied marking. [Exact implementation](../docs/kafka/implementation-reference.md#enqueue-callbacks-flush-and-shutdown).

## 12. Where the gateway starts reading

![A gateway starts at the captured high watermark and skips historical Kafka records](../docs/kafka/diagrams/12-consumer-tail.svg)

At startup, the consumer manually assigns each partition at its captured tail. It does not resume a committed consumer offset. **Start the consumer before the workload.** A gateway restart loses its in-memory votes and captures a new tail, so Kafka history alone does not restore pending requests.

## 13. What happens inside the gateway

![The consumer thread parses messages before eight default dispatch workers verify MACs and insert votes](../docs/kafka/diagrams/13-consumer-workers.svg)

The consumer thread decodes records before handing them to the worker pool. Workers verify MACs outside the vote-store lock, then insert records in chunks of up to 128 per lock hold. Multiple workers can interleave; request identity keeps the reports associated correctly.

## 14. Which cryptography is used where

![BLAKE3 transaction signing is separate from SHA-256 outcome digests and HMAC-SHA256 result verification](../docs/kafka/diagrams/14-crypto.svg)

**Kafka result authentication uses HMAC-SHA256 with OpenSSL.** The gateway's BLAKE3 transaction signer is a different feature. The result key is shared; the development-key fallback and optional trusted fastpath limit the security claim. [Authenticated fields and fallback behavior](../docs/kafka/implementation-reference.md#6-digests-authentication-and-vote-resolution).

## 15. How B3, B4, and T1 are selected

![Text override selects T1; otherwise ledger-off uses B3 and safe ledger mode uses B4](../docs/kafka/diagrams/15-wire-choice.svg)

Normal binary execution uses **B3**. Safe ledger mode uses **B4**, adding epoch, ordinal, and explicit terminal outcome metadata. T1 is unsigned text for inspection/compatibility; parsing it does not make it a valid authenticated vote.

## 16. How the batch is laid out in bytes

![B3 and B4 share an eight-byte batch header but have different fixed and variable record fields](../docs/kafka/diagrams/16-wire-layout.svg)

B4 stores its epoch and digest as raw 32-byte values; its HMAC remains hex text. The record layout repeats for every result in the batch. [Exact offsets, terminal digest recipes, and parser checks](../docs/kafka/implementation-reference.md#7-wire-formats).

## 17. How the gateway counts votes

![Two distinct nodes report digest A while a third reports digest B, creating a majority and an audit mismatch](../docs/kafka/diagrams/17-vote-example.svg)

Here nodes 1 and 2 agree, so A has a 2-of-3 majority. Node 4's B remains an all-node inconsistency. A duplicate report from node 1 cannot create another distinct-node vote. Identity conflicts and invalid MACs have separate handling. [Vote-store rules](../docs/kafka/implementation-reference.md#vote-tracking).

## 18. When a request is counted as completed

![Direct completion, majority followed by asynchronous all-node audit, and strict all-node completion wait at different points](../docs/kafka/diagrams/18-completion-modes.svg)

`majority_async_all3` allows majority completion before the third-node audit finishes. The main deterministic `strict_majority` path waits for all-node consistency before recording completion. Direct mode uses terminal completion evidence from the direct path; Kafka votes do not gate it.

These are the **main deterministic file-workload semantics**. The generic helper and other gateway branches do not universally implement the same all-node barrier. [Completion branches and flags](../docs/kafka/implementation-reference.md#8-completion-and-all-node-validation).

## 19. How safe mode changes applied marking

![Normal execution can mark applied without waiting for Kafka delivery, while safe mode waits for its flush helper](../docs/kafka/diagrams/19-safe-versus-normal.svg)

Normal threaded execution can mark applied before asynchronous publication; the event path can enqueue and mark applied without awaiting delivery. Safe mode waits for its delivery helper before marking applied. **Both still use `result_fast` with `acks=1`**, and diagram 11's flush limitation still applies.

## 20. How an observed mismatch reaches recovery

![A divergence observation separately produces an error-topic diagnostic and directly calls the configured recovery manager](../docs/kafka/diagrams/20-recovery-and-alerts.svg)

The gateway directly passes an observation to the enabled recovery manager. Active mode can request repair; passive mode asks for state comparison first. Writing an `errTopic` diagnostic is a separate branch—the gateway does not consume that topic to trigger its own recovery.

## 21. What recovery boundary L means for auditing

![State replacement covers historical entries through L, while replay above L is audited normally](../docs/kafka/diagrams/21-recovery-boundary.svg)

Once a damaged node's state is replaced through L, its old votes at/below that boundary do not describe the repaired state. The audit can account for replacement coverage there and check replay above L. Consequently, recovery-attributed completion is different from receiving three matching Kafka votes for every historical request.

## 22. Why settings must be checked in the actual process

![A runner-only hard-cap assignment is not explicitly exported, while selected target, delay and full-body settings are](../docs/kafka/diagrams/22-settings-reach-process.svg)

The runner locally assigns an async cap of 256 but does not explicitly export it in the server-launch block; an unconfigured remote executor uses 512. It does export selected settings such as target 32, delay 150 µs, and full-result limit −1. Numeric replica limits select **node IDs**, not whichever node is leader.

The [configuration tables](../docs/kafka/implementation-reference.md#10-configuration-reference) separate bare executable defaults, runner overrides, and environment propagation.

## 23. What the latency counters include

![Execution, publisher dwell, encoding timestamp, enqueue, callback, and gateway vote processing occupy different portions of the pipeline](../docs/kafka/diagrams/23-timing.svg)

`delivery_ns` measures enqueue to callback, including queue/retry/callback-service delay. Consume lag starts at the timestamp sampled during payload construction; it excludes SQL execution and earlier publisher dwell. It also depends on cross-host clock alignment. [Counter meanings](../docs/kafka/implementation-reference.md#11-telemetry-and-performance-interpretation).

## 24. What each piece of evidence establishes

![Local enqueue, broker delivery, majority, all-node audit and post-run checks establish different bounded facts](../docs/kafka/diagrams/24-guarantees.svg)

Read every success at its own layer. The current design does not provide an atomic PostgreSQL/Kafka commit, durable gateway vote replay, or end-to-end exactly-once processing. The [failure reference](../docs/kafka/implementation-reference.md#12-failure-semantics-and-implementation-limits) explains the actual behavior when each layer fails.

## 25. Colocated versus the remote hub

![Colocated brokers run on database hosts, whereas remote routing puts three broker processes on the default hub host](../docs/kafka/diagrams/25-topology-comparison.svg)

Colocation usually keeps production on the database host while the gateway fetches across hosts. Remote routing sends production to the hub. In both scripted layouts, result partitions normally have RF=1; three broker processes on one hub also share that host's resources and failure domain.

## 26. What the result MAC does—and what it leaves unchecked

![The result MAC authenticates metadata and digest, while full result bodies are excluded and are not independently rehashed by signed-wire resolution](../docs/kafka/diagrams/26-body-authentication.svg)

The MAC covers the digest and identity metadata, not the full result bytes directly. The current signed-wire majority path compares available majority full bodies with each other but does not independently recompute the digest from them. A single supplied body therefore has no second body to compare against. This is an implementation limit, not a fix made by this documentation change.

---

**For exact details:** [implementation reference](../docs/kafka/implementation-reference.md) · [source map](../docs/kafka/implementation-reference.md#2-components-and-end-to-end-flow) · [configuration](../docs/kafka/implementation-reference.md#10-configuration-reference) · [operating checks and dated evidence](../docs/kafka/implementation-reference.md#13-operating-checks-and-review-evidence).

The figures are pre-rendered SVG images, so this guide does not require Mermaid support or an internet connection to display them. Open an individual image for a larger view. [Diagram build and review notes](../docs/kafka/README.md).
