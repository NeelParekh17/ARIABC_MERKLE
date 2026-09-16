// ariabc_pg/tests/blake3_tx_sign_test.cxx
// Unit test and micro-benchmark for on-the-fly BLAKE3 transaction signing & verification.

#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <cassert>
#include <stdexcept>

#include "blake3_tx_signer.hxx"

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "Requirement failed at line " << __LINE__ << ": " << #expr << std::endl; \
            throw std::runtime_error("requirement failed: " #expr); \
        } \
    } while (0)

int main() {
    std::cout << "Running BLAKE3 transaction signer unit tests..." << std::endl;

    ariabc_pg::blake3_tx_signer signer;
    signer.init(true, "test_cluster_blake3_signing_key");

    REQUIRE(signer.is_enabled());

    const uint64_t req_num = 12345;
    const std::string req_id = "cli-000012345";
    const std::string sql = "UPDATE usertable_small SET field1 = 'updated_val_1' WHERE ycsb_key = 1042;";

    // Test 1: Sign on the fly
    ariabc_pg::blake3_sig_256 sig = signer.sign(req_num, req_id, sql);
    REQUIRE(!sig.is_zero());
    std::cout << "Generated BLAKE3 transaction signature: " << sig.to_hex() << std::endl;

    // Test 2: Authenticity verification matches
    REQUIRE(signer.verify(req_num, req_id, sql, sig));

    // Test 3: Tampering detection - modified SQL
    const std::string tampered_sql = "UPDATE usertable_small SET field1 = 'evil_tampered' WHERE ycsb_key = 1042;";
    REQUIRE(!signer.verify(req_num, req_id, tampered_sql, sig));

    // Test 4: Tampering detection - modified req_num
    REQUIRE(!signer.verify(req_num + 1, req_id, sql, sig));

    // Test 5: Tampering detection - modified req_id
    REQUIRE(!signer.verify(req_num, "cli-000099999", sql, sig));

    // Test 6: Different signer key fails
    ariabc_pg::blake3_tx_signer other_signer;
    other_signer.init(true, "completely_different_secret_key");
    REQUIRE(!other_signer.verify(req_num, req_id, sql, sig));

    // Test 7: Micro-benchmark (100,000 operations)
    std::cout << "\nRunning micro-benchmark (100,000 iterations)..." << std::endl;
    const int iterations = 100000;
    std::vector<ariabc_pg::blake3_sig_256> sigs(iterations);

    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        sigs[i] = signer.sign(static_cast<uint64_t>(i), "cli-" + std::to_string(i), sql);
    }
    const auto t1 = std::chrono::steady_clock::now();
    const double sign_ns = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()) / iterations;

    const auto t2 = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        bool ok = signer.verify(static_cast<uint64_t>(i), "cli-" + std::to_string(i), sql, sigs[i]);
        if (!ok) throw std::runtime_error("verification failed during micro-benchmark");
    }
    const auto t3 = std::chrono::steady_clock::now();
    const double verify_ns = static_cast<double>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count()) / iterations;

    std::cout << "Average BLAKE3 sign latency:   " << sign_ns << " ns / transaction" << std::endl;
    std::cout << "Average BLAKE3 verify latency: " << verify_ns << " ns / transaction" << std::endl;
    std::cout << "Total sign + verify latency:   " << (sign_ns + verify_ns) << " ns / transaction" << std::endl;

    REQUIRE(sign_ns < 1000.0); // well under 1 microsecond (typically ~30-60 ns)
    REQUIRE(verify_ns < 1000.0);

    std::cout << "\nALL BLAKE3 transaction signer unit tests PASSED!" << std::endl;
    return 0;
}
