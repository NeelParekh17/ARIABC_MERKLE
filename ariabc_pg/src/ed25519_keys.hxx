#pragma once

/*
 * Ed25519 keys for per-principal signatures (OpenSSL >= 1.1.1).
 *
 * Every principal owns its own key pair, so a signature identifies exactly one
 * signer: a replica cannot produce a valid signature for another replica or
 * for a client, unlike a shared-key MAC.  Two uses:
 *   - clients (gateways) sign each request item; replicas verify it
 *     (tx_request_sig.hxx);
 *   - each replica signs its Kafka result messages; the gateway verifies them
 *     (result_envelope.hxx).
 *
 * Key files are standard PEM, created with
 *   openssl genpkey -algorithm ed25519 -out node_1.key.pem
 *   openssl pkey -in node_1.key.pem -pubout -out node_1.pub.pem
 * (scripts/distributed/gen_ed25519_keys.sh does this for a whole cluster).
 *
 * Key directory layout (default ~/.ariabc/keys, else $ARIABC_KEY_DIR):
 *   node_<id>.key.pem    private key of replica <id>   (only on that replica)
 *   node_<id>.pub.pem    public key of replica <id>    (on every gateway)
 *   client_<id>.key.pem  private key of client <id>    (only on that gateway)
 *   client_<id>.pub.pem  public key of client <id>     (on every replica)
 */

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <sstream>
#include <string>

#include <dirent.h>
#include <sys/stat.h>

#ifndef SSL_LIBRARY_NOT_FOUND
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#endif

namespace ariabc_pg {

const size_t kEd25519SigBytes = 64;
const size_t kEd25519PubBytes = 32;

inline const char* key_dir_env() { return "ARIABC_KEY_DIR"; }

// $HOME/.ariabc/keys; empty when HOME is unset.
inline std::string default_key_dir() {
    const char* home = std::getenv("HOME");
    if (!home || !*home) return std::string();
    return std::string(home) + "/.ariabc/keys";
}

// Explicit directory, else $ARIABC_KEY_DIR, else ~/.ariabc/keys.
inline std::string resolve_key_dir(const std::string& explicit_dir) {
    if (!explicit_dir.empty()) return explicit_dir;
    const char* env = std::getenv(key_dir_env());
    if (env && *env) return std::string(env);
    return default_key_dir();
}

// role is "node" or "client".
inline std::string private_key_path(const std::string& dir, const char* role, uint32_t id) {
    std::ostringstream oss;
    oss << dir << "/" << role << "_" << id << ".key.pem";
    return oss.str();
}

inline std::string public_key_path(const std::string& dir, const char* role, uint32_t id) {
    std::ostringstream oss;
    oss << dir << "/" << role << "_" << id << ".pub.pem";
    return oss.str();
}

inline bool key_file_exists(const std::string& path) {
    struct stat st;
    return !path.empty() && ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

inline std::string hex_bytes(const uint8_t* p, size_t n) {
    static const char kHex[] = "0123456789abcdef";
    std::string s(n * 2, '0');
    for (size_t i = 0; i < n; ++i) {
        s[2 * i] = kHex[(p[i] >> 4) & 0xf];
        s[2 * i + 1] = kHex[p[i] & 0xf];
    }
    return s;
}

#ifndef SSL_LIBRARY_NOT_FOUND

inline std::string openssl_error_string() {
    const unsigned long e = ERR_get_error();
    ERR_clear_error();
    if (e == 0) return "unknown OpenSSL error";
    char buf[256];
    ERR_error_string_n(e, buf, sizeof(buf));
    return buf;
}

struct evp_pkey_free_fn {
    void operator()(EVP_PKEY* p) const { if (p) EVP_PKEY_free(p); }
};
struct evp_md_ctx_free_fn {
    void operator()(EVP_MD_CTX* c) const { if (c) EVP_MD_CTX_free(c); }
};
typedef std::unique_ptr<EVP_PKEY, evp_pkey_free_fn> evp_pkey_ptr;
typedef std::unique_ptr<EVP_MD_CTX, evp_md_ctx_free_fn> evp_md_ctx_ptr;

/*
 * The EVP_PKEY is shared read-only between threads; each sign/verify call uses
 * its own EVP_MD_CTX, which is the thread-safe pattern for OpenSSL 1.1.1/3.x.
 */
class ed25519_private_key {
public:
    bool load_pem_file(const std::string& path, std::string& err) {
        key_.reset();
        FILE* f = std::fopen(path.c_str(), "r");
        if (!f) {
            err = "cannot open Ed25519 private key " + path;
            return false;
        }
        EVP_PKEY* k = PEM_read_PrivateKey(f, nullptr, nullptr, nullptr);
        std::fclose(f);
        if (!k) {
            err = "cannot parse private key PEM " + path + ": " + openssl_error_string();
            return false;
        }
        evp_pkey_ptr owned(k);
        if (EVP_PKEY_id(k) != EVP_PKEY_ED25519) {
            err = "key " + path + " is not an Ed25519 key";
            return false;
        }
        struct stat st;
        readable_by_others_ = (::stat(path.c_str(), &st) == 0) && (st.st_mode & 0077) != 0;
        key_ = std::move(owned);
        path_ = path;
        return true;
    }

    bool loaded() const { return static_cast<bool>(key_); }
    bool readable_by_others() const { return readable_by_others_; }
    const std::string& path() const { return path_; }

    bool sign(const void* msg, size_t len, uint8_t sig[kEd25519SigBytes]) const {
        if (!key_) return false;
        evp_md_ctx_ptr ctx(EVP_MD_CTX_new());
        if (!ctx) return false;
        if (EVP_DigestSignInit(ctx.get(), nullptr, nullptr, nullptr, key_.get()) != 1) return false;
        size_t sig_len = kEd25519SigBytes;
        if (EVP_DigestSign(ctx.get(), sig, &sig_len,
                           static_cast<const unsigned char*>(msg), len) != 1) {
            return false;
        }
        return sig_len == kEd25519SigBytes;
    }

    bool raw_public_key(uint8_t out[kEd25519PubBytes]) const {
        if (!key_) return false;
        size_t n = kEd25519PubBytes;
        return EVP_PKEY_get_raw_public_key(key_.get(), out, &n) == 1 && n == kEd25519PubBytes;
    }

    // First 8 bytes of the public key, for logs.
    std::string public_fingerprint() const {
        uint8_t raw[kEd25519PubBytes];
        if (!raw_public_key(raw)) return "?";
        return hex_bytes(raw, 8);
    }

private:
    evp_pkey_ptr key_;
    std::string path_;
    bool readable_by_others_ = false;
};

class ed25519_public_key {
public:
    bool load_pem_file(const std::string& path, std::string& err) {
        key_.reset();
        FILE* f = std::fopen(path.c_str(), "r");
        if (!f) {
            err = "cannot open Ed25519 public key " + path;
            return false;
        }
        EVP_PKEY* k = PEM_read_PUBKEY(f, nullptr, nullptr, nullptr);
        std::fclose(f);
        if (!k) {
            err = "cannot parse public key PEM " + path + ": " + openssl_error_string();
            return false;
        }
        evp_pkey_ptr owned(k);
        if (EVP_PKEY_id(k) != EVP_PKEY_ED25519) {
            err = "key " + path + " is not an Ed25519 key";
            return false;
        }
        key_ = std::move(owned);
        return true;
    }

    bool set_raw(const uint8_t raw[kEd25519PubBytes], std::string& err) {
        EVP_PKEY* k = EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, nullptr, raw, kEd25519PubBytes);
        if (!k) {
            err = "invalid raw Ed25519 public key: " + openssl_error_string();
            return false;
        }
        key_.reset(k);
        return true;
    }

    bool loaded() const { return static_cast<bool>(key_); }

    bool verify(const void* msg, size_t len, const uint8_t sig[kEd25519SigBytes]) const {
        if (!key_) return false;
        evp_md_ctx_ptr ctx(EVP_MD_CTX_new());
        if (!ctx) return false;
        if (EVP_DigestVerifyInit(ctx.get(), nullptr, nullptr, nullptr, key_.get()) != 1) {
            ERR_clear_error();
            return false;
        }
        const int rc = EVP_DigestVerify(ctx.get(), sig, kEd25519SigBytes,
                                        static_cast<const unsigned char*>(msg), len);
        if (rc != 1) ERR_clear_error();
        return rc == 1;
    }

    std::string fingerprint() const {
        if (!key_) return "?";
        uint8_t raw[kEd25519PubBytes];
        size_t n = kEd25519PubBytes;
        if (EVP_PKEY_get_raw_public_key(key_.get(), raw, &n) != 1 || n != kEd25519PubBytes) return "?";
        return hex_bytes(raw, 8);
    }

private:
    evp_pkey_ptr key_;
};

#else  // SSL_LIBRARY_NOT_FOUND: signatures are unavailable, every load fails closed.

class ed25519_private_key {
public:
    bool load_pem_file(const std::string&, std::string& err) {
        err = "built without OpenSSL: Ed25519 signatures unavailable";
        return false;
    }
    bool loaded() const { return false; }
    bool readable_by_others() const { return false; }
    const std::string& path() const { return path_; }
    bool sign(const void*, size_t, uint8_t*) const { return false; }
    bool raw_public_key(uint8_t*) const { return false; }
    std::string public_fingerprint() const { return "?"; }
private:
    std::string path_;
};

class ed25519_public_key {
public:
    bool load_pem_file(const std::string&, std::string& err) {
        err = "built without OpenSSL: Ed25519 signatures unavailable";
        return false;
    }
    bool set_raw(const uint8_t*, std::string& err) {
        err = "built without OpenSSL: Ed25519 signatures unavailable";
        return false;
    }
    bool loaded() const { return false; }
    bool verify(const void*, size_t, const uint8_t*) const { return false; }
    std::string fingerprint() const { return "?"; }
};

#endif

/*
 * Public keys of one role ("node" or "client"), indexed by numeric id, loaded
 * from <dir>/<role>_<id>.pub.pem.  Loading fails closed: one unreadable or
 * non-Ed25519 file in the directory rejects the whole roster.
 */
class ed25519_key_roster {
public:
    bool load_dir(const std::string& dir, const char* role, std::string& err) {
        keys_.clear();
        dir_ = dir;
        DIR* d = ::opendir(dir.c_str());
        if (!d) {
            err = "cannot open key directory " + dir;
            return false;
        }
        const std::string prefix = std::string(role) + "_";
        const std::string suffix = ".pub.pem";
        bool ok = true;
        while (struct dirent* e = ::readdir(d)) {
            const std::string name = e->d_name;
            if (name.size() <= prefix.size() + suffix.size()) continue;
            if (name.compare(0, prefix.size(), prefix) != 0) continue;
            if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) continue;
            const std::string digits =
                name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
            if (digits.empty() || digits.size() > 9 ||
                digits.find_first_not_of("0123456789") != std::string::npos) {
                continue;
            }
            const uint32_t id = static_cast<uint32_t>(std::strtoul(digits.c_str(), nullptr, 10));
            std::shared_ptr<ed25519_public_key> key(new ed25519_public_key());
            if (!key->load_pem_file(dir + "/" + name, err)) {
                ok = false;
                break;
            }
            keys_[id] = key;
        }
        ::closedir(d);
        if (!ok) keys_.clear();
        return ok;
    }

    void add(uint32_t id, std::shared_ptr<ed25519_public_key> key) { keys_[id] = std::move(key); }

    const ed25519_public_key* find(uint32_t id) const {
        auto it = keys_.find(id);
        return it == keys_.end() ? nullptr : it->second.get();
    }

    size_t size() const { return keys_.size(); }
    bool empty() const { return keys_.empty(); }
    const std::string& dir() const { return dir_; }

    std::string ids_csv() const {
        std::ostringstream oss;
        bool first = true;
        for (const auto& kv : keys_) {
            if (!first) oss << ",";
            oss << kv.first;
            first = false;
        }
        return oss.str();
    }

private:
    std::map<uint32_t, std::shared_ptr<ed25519_public_key>> keys_;
    std::string dir_;
};

} // namespace ariabc_pg
