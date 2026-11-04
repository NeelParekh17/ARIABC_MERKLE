#pragma once

/*
 * Per-replica signed result messages (Ed25519, see ed25519_keys.hxx).
 *
 * Each Kafka result message (a B3/B4/T1 payload holding many records) is
 * sealed once by the replica that produced it, with that replica's own
 * private key.  The gateway holds one public key per replica, so a faulty
 * replica cannot produce a message that verifies as another replica's, and a
 * vote counts for node N only when node N's key signed it.
 *
 * Envelope ("S1"):
 *   off 0   'S' '1'
 *   off 2   u16le signer node id
 *   off 4   u32le inner payload length L
 *   off 8   inner payload (L bytes)
 *   off 8+L 64-byte Ed25519 signature over
 *           "ariabc.result-msg.v1" || bytes[0, 8+L)
 *
 * One signature per message, not per record: verification cost is divided
 * by the records per message (about 20 at the default batching).
 */

#include "ed25519_keys.hxx"

#include <cstdint>
#include <string>

namespace ariabc_pg {

const char kResultEnvelopeDomain[] = "ariabc.result-msg.v1";
const size_t kResultEnvelopeHeaderBytes = 8;

inline bool is_result_envelope(const std::string& p) {
    return p.size() >= kResultEnvelopeHeaderBytes && p[0] == 'S' && p[1] == '1';
}

inline bool seal_result_envelope(const ed25519_private_key& key,
                                 uint16_t node_id,
                                 const std::string& inner,
                                 std::string& out) {
    const size_t dlen = sizeof(kResultEnvelopeDomain) - 1;
    if (inner.size() > 0xFFFFFFFFull) return false;
    std::string buf;
    buf.reserve(dlen + kResultEnvelopeHeaderBytes + inner.size() + kEd25519SigBytes);
    buf.append(kResultEnvelopeDomain, dlen);
    buf.push_back('S');
    buf.push_back('1');
    buf.push_back(static_cast<char>(node_id & 0xffu));
    buf.push_back(static_cast<char>((node_id >> 8) & 0xffu));
    const uint32_t len = static_cast<uint32_t>(inner.size());
    for (int i = 0; i < 4; ++i) buf.push_back(static_cast<char>((len >> (8 * i)) & 0xffu));
    buf.append(inner);
    uint8_t sig[kEd25519SigBytes];
    if (!key.sign(buf.data(), buf.size(), sig)) return false;
    buf.erase(0, dlen);
    buf.append(reinterpret_cast<const char*>(sig), kEd25519SigBytes);
    out.swap(buf);
    return true;
}

enum class envelope_status : uint8_t {
    OK = 0,
    MALFORMED = 1,
    UNKNOWN_SIGNER = 2,
    BAD_SIGNATURE = 3,
};

/*
 * Splits an "S1" message.  On anything but MALFORMED, signer and inner are
 * filled; only OK means the signer's key signed exactly these bytes.
 */
inline envelope_status open_result_envelope(const std::string& p,
                                            const ed25519_key_roster* roster,
                                            uint16_t& signer,
                                            std::string& inner) {
    if (!is_result_envelope(p)) return envelope_status::MALFORMED;
    const unsigned char* b = reinterpret_cast<const unsigned char*>(p.data());
    signer = static_cast<uint16_t>(b[2] | (b[3] << 8));
    const uint32_t len = static_cast<uint32_t>(b[4]) | (static_cast<uint32_t>(b[5]) << 8) |
                         (static_cast<uint32_t>(b[6]) << 16) | (static_cast<uint32_t>(b[7]) << 24);
    if (static_cast<uint64_t>(kResultEnvelopeHeaderBytes) + len + kEd25519SigBytes != p.size()) {
        return envelope_status::MALFORMED;
    }
    inner.assign(p, kResultEnvelopeHeaderBytes, len);
    const ed25519_public_key* pk = roster ? roster->find(signer) : nullptr;
    if (!pk) return envelope_status::UNKNOWN_SIGNER;
    const size_t dlen = sizeof(kResultEnvelopeDomain) - 1;
    std::string signed_bytes;
    signed_bytes.reserve(dlen + kResultEnvelopeHeaderBytes + len);
    signed_bytes.append(kResultEnvelopeDomain, dlen);
    signed_bytes.append(p, 0, kResultEnvelopeHeaderBytes + len);
    const uint8_t* sig = b + kResultEnvelopeHeaderBytes + len;
    return pk->verify(signed_bytes.data(), signed_bytes.size(), sig) ? envelope_status::OK
                                                                     : envelope_status::BAD_SIGNATURE;
}

} // namespace ariabc_pg
