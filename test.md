Ran command: `graphify query "modulo hash xor"`
Ran command: `graphify query "AdHash addition modulo"`
Viewed COMBINE_SITES.md:1-39
Ran command: `find Final_Results/OOM_100M/adhash_20261012 -type f`
Viewed README.md:1-56
Viewed CORRECTNESS.md:1-100
Viewed merkleutil.c:310-370
Ran command: `git log -n 1 6b19ff92 --stat`
Ran command: `git log -n 1 6a1c66d6`
Ran command: `git show --stat 6a1c66d6`
Viewed README_MERKLE.md:1-100
Viewed README_MERKLE.md:101-200
Viewed README_MERKLE.md:201-235
Viewed replica_repair.cxx:670-715
Viewed remote_db.py:125-165
Viewed remote_db.py:190-250
Viewed merkledelta.c:165-210
Viewed merkledelta.c:211-240
Viewed shm_transaction.c:3075-3110
Viewed shm_transaction.c:3250-3290
Viewed shm_transaction.c:3360-3430
Viewed merkle.h:20-60
Viewed merkle.h:61-120

### Executive Summary

In AriaBC, the Merkle tree and partition root digest computation transitioned from **bitwise XOR ($\oplus$)** to **unsigned little-endian addition modulo $2^{256}$ (AdHash)** (introduced in commit `6a1c66d6`, merged in commit `6b19ff92`). 

While the historical C/SQL function name [`merkle_hash_xor()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L321-L334) and struct member names like `xor_delta` were retained to avoid wide code churn, **the underlying arithmetic was replaced entirely with 256-bit addition with byte carry, paired with two's complement negation for deletions.**

---

### 1. Why Was XOR Replaced? The Pair-Cancellation Flaw

In the earlier architecture (`MERKLE_VERSION 10`), node and root hashes were combined using bitwise XOR:
$$\text{Digest} = h(r_1) \oplus h(r_2) \oplus \dots \oplus h(r_n)$$

Because XOR is self-inverting ($x \oplus x = 0$):
* **Identical rows cancelled out in pairs:** In tables without a unique constraint or primary key, if a table had two identical rows $\{r, r\}$, their BLAKE3 hashes satisfied:
  $$h(r) \oplus h(r) = 0$$
* A table containing $\{r, r\}$ hashed to the exact same Merkle root as an **empty table** $\{\}$.
* Similarly, three duplicate rows $\{r, r, r\}$ hashed identically to a single row $\{r\}$.
* This created severe false-equality vulnerabilities in replica consistency checks and Merkle verification on tables with duplicate records.

---

### 2. What Addition Modulo $2^{256}$ (AdHash) Does

To solve this without breaking multiset order-independence, the system adopted **AdHash** over the abelian group $(\mathbb{Z}_{2^{256}}, +)$:
$$\text{Digest} = \left( \sum_{i=1}^{n} h(r_i) \right) \pmod{2^{256}}$$

1. **Order Independence:** Addition is commutative ($A + B = B + A$) and associative, so row insertion order does not affect the final Merkle root.
2. **Duplicate-Sensitive:** Identical rows now accumulate rather than cancelling:
   $$h(r) + h(r) = 2 \cdot h(r) \pmod{2^{256}} \neq 0$$
   Multiplicity is preserved modulo $2^{256}$.
3. **Constant-Time Incremental Updates & Deletions:** Deleting an element is equivalent to adding its additive inverse (two's complement negation $-h \pmod{2^{256}}$), and updates can be staged as a direct delta $(\text{new} - \text{old})$.

---

### 3. Core Engine Implementation

Each hash is a 32-byte array (`uint8 data[32]`) interpreted as an unsigned 256-bit integer in little-endian byte order.

#### 3.1 256-Bit Addition: [`merkle_hash_xor()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L321-L334)
In [src/backend/access/merkle/merkleutil.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L321-L334), [`merkle_hash_xor()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L321-L334) computes:
$$\text{dest} = (\text{dest} + \text{src}) \pmod{2^{256}}$$

```c
void
merkle_hash_xor(MerkleHash *dest, const MerkleHash *src)
{
    unsigned int carry = 0;
    int i;

    for (i = 0; i < MERKLE_HASH_BYTES; i++) /* 32 bytes */
    {
        unsigned int sum = (unsigned int) dest->data[i] + src->data[i] + carry;

        dest->data[i] = (uint8) sum;
        carry = sum >> 8;
    }
    /* Final carry past byte 31 is discarded, giving modulo 2^256 */
}
```

#### 3.2 Two's Complement Negation: [`merkle_hash_negate()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L337-L350)
Subtraction is performed by computing the two's complement: $-h \equiv (\sim h) + 1 \pmod{2^{256}}$:

```c
void
merkle_hash_negate(MerkleHash *hash)
{
    unsigned int carry = 1; /* + 1 for two's complement */
    int i;

    for (i = 0; i < MERKLE_HASH_BYTES; i++)
    {
        unsigned int sum = (unsigned int) (hash->data[i] ^ 0xff) + carry;

        hash->data[i] = (uint8) sum;
        carry = sum >> 8;
    }
}
```

---

### 4. How Database Operations (INSERT, DELETE, UPDATE) Work

The addition modulo model flows through the transaction delta staging pipeline in [src/backend/access/merkle/merkledelta.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkledelta.c#L210-L228):

* **INSERT:**
  Supplies the positive 32-byte BLAKE3 row hash $+h$ and $+1$ tuple count.
  $$\Delta_{\text{hash}} = +h, \quad \Delta_{\text{count}} = +1$$
* **DELETE:**
  The caller provides the positive old row hash. Staging negates it once using [`merkle_hash_negate()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L337-L350) to produce $-h$, and adds it to the staged accumulator:
  $$\Delta_{\text{hash}} = -h, \quad \Delta_{\text{count}} = -1$$
* **UPDATE:**
  * **Same Leaf / Same Route:** Handled in [`apply_optim_update()`](file:///work/ARIABC/AriaBC/src/backend/bcdb/shm_transaction.c#L3382-L3390) by calculating $(\text{new} - \text{old})$:
    ```c
    MerkleHash delta = oldHash;
    merkle_hash_negate(&delta);           /* -old */
    merkle_hash_xor(&delta, &newHash);   /* (-old) + new = new - old */
    merkle_stage_delta_event(..., MERKLE_DELTA_UPDATE_SAME_LEAF, ..., &delta);
    ```
    This updates the leaf in a single operation with $\Delta_{\text{count}} = 0$.
  * **Cross Leaf / Route Move:** Staged as a paired DELETE of old hash ($-h_{\text{old}}$) and INSERT of new hash ($+h_{\text{new}}$).
* **Transaction Rollback:**
  Because deltas are kept in sub-transaction memory frames until commit, an abort simply frees the frame; no complex undo replay or reverse arithmetic is required.

---

### 5. Cross-Subsystem Consistency

Every component across the stack uses identical little-endian 256-bit modular addition:

1. **PostgreSQL C Engine:**
   * [`merkle_hash_xor()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L321-L334) and [`merkle_hash_negate()`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleutil.c#L337-L350) in `merkleutil.c`.
   * Dynamic split/merge logic in [merkleapply.c](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleapply.c) aggregates child buckets and ancestor sums using addition.
   * Internal verification ([`merkle_verify`](file:///work/ARIABC/AriaBC/src/backend/access/merkle/merkleverify.c)) computes heap sums and compares them with partition root sums.
2. **C++ Server / Replica Repair ([replica_repair.cxx](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx#L687-L703)):**
   [`compute_merkle_digest()`](file:///work/ARIABC/AriaBC/ariabc_pg/src/replica_repair.cxx#L678-L707) aggregates 32-byte partition root hashes into the table digest via a byte accumulator with carry:
   ```cpp
   unsigned int carry = 0;
   for (size_t i = 0; i < acc.size(); ++i) {
       const unsigned int sum = static_cast<unsigned int>(acc[i]) + h[i] + carry;
       acc[i] = static_cast<uint8_t>(sum);
       carry = sum >> 8;
   }
   ```
3. **Python Orchestrator ([remote_db.py](file:///work/ARIABC/AriaBC/scripts/distributed/recovery/remote_db.py#L195-L201)):**
   Computes root digests in Python using native arbitrary-precision arithmetic:
   ```python
   accumulator = (accumulator + int.from_bytes(h, "little")) % (1 << 256)
   root_hex = accumulator.to_bytes(32, "little").hex()
   ```

---

### 6. Version Guard & Performance

* **Format Barrier:** [`MERKLE_VERSION`](file:///work/ARIABC/AriaBC/src/include/access/merkle.h#L105) was bumped to **11** (format 10 was XOR). Old format 10 indexes are refused by the engine with SQLSTATE `XX002` and an instruction to run `REINDEX` or `SELECT merkle_rebuild_legacy_indexes()`.
* **Zero Performance Cost:** As documented in the [OOM 100M campaign results](file:///work/ARIABC/AriaBC/Final_Results/OOM_100M/adhash_20261012/README.md), the 256-bit addition loop has virtually zero overhead compared to XOR:
  * Across 28 benchmark points (84 Merkle trials), the median AdHash / XOR TPS ratio was **1.003** (21 of 28 points were within $\pm 1\%$).