// Safety of a restarted HotStuff replica, on the protocol core with no network.
// Every case also runs a replica that restarted with nothing, which must get it
// wrong: that shows each case really exercises the vote and lock rules.
//
//   hotstuff_vote_state_test <scratch_dir>
#include "hotstuff_vote_state.hxx"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>

using namespace hotstuff;

#define CHECK(c)                                                                \
    do {                                                                        \
        if (!(c)) {                                                             \
            std::cerr << "CHECK FAILED " << __FILE__ << ":" << __LINE__ << ": " \
                      << #c << std::endl;                                       \
            std::exit(1);                                                       \
        }                                                                       \
    } while (0)

namespace {

const ReplicaID kLeader = 3;

class core : public HotStuffCore {
public:
    core() : HotStuffCore(0, new PrivKeyDummy()) {
        for (ReplicaID i = 0; i < 4; ++i) add_replica(i, salticidae::PeerId(), new PubKeyDummy());
        on_init(1);
    }

    part_cert_bt create_part_cert(const PrivKey&, const uint256_t& h) override { return new PartCertDummy(h); }
    part_cert_bt parse_part_cert(DataStream&) override { return new PartCertDummy(); }
    quorum_cert_bt create_quorum_cert(const uint256_t& h) override { return new QuorumCertDummy(get_config(), h); }
    quorum_cert_bt parse_quorum_cert(DataStream&) override { return new QuorumCertDummy(); }

    // Hashes depend only on content, so every core builds the same chain.
    block_t add(const block_t& parent, const block_t& qc_ref, const std::string& tag) {
        block_t b = storage->add_blk(new Block(std::vector<block_t>{parent}, std::vector<uint256_t>(),
                                               create_quorum_cert(qc_ref->get_hash()),
                                               bytearray_t(tag.begin(), tag.end()),
                                               parent->get_height() + 1, qc_ref, nullptr));
        CHECK(on_deliver_blk(b));
        return b;
    }

    // Returns whether this replica voted for the block.
    bool propose(const block_t& b) {
        const size_t before = votes.size();
        on_receive_proposal(Proposal(kLeader, b, this));
        return votes.size() > before;
    }

    std::vector<uint256_t> votes;
    std::vector<VoteState> persisted;
    size_t votes_without_save = 0;

protected:
    void do_decide(Finality&&) override {}
    void do_consensus(const block_t&) override {}
    void do_broadcast_proposal(const Proposal&) override {}
    void do_vote(ReplicaID, const Vote& v) override {
        // The state saved just before must already cover this vote.
        if (persisted.empty() || persisted.back().vheight != storage->find_blk(v.blk_hash)->get_height()) {
            votes_without_save++;
        }
        votes.push_back(v.blk_hash);
    }
    void do_persist_vote(const VoteState& s) override { persisted.push_back(s); }
};

// b1..b4 in a straight line, each carrying the QC of its parent.  Having voted for
// b4 a replica has vheight 4 and lock b2.
std::vector<block_t> line(core& c, bool as_proposals) {
    std::vector<block_t> out{c.get_genesis()};
    for (int i = 1; i <= 4; ++i) {
        block_t b = c.add(out.back(), out.back(), "b" + std::to_string(i));
        if (as_proposals) CHECK(c.propose(b));
        out.push_back(b);
    }
    return out;
}

VoteState voted_through_b4(core& c) {
    line(c, true);
    CHECK(c.votes_without_save == 0);
    CHECK(c.persisted.size() == 4);
    const VoteState s = c.persisted.back();
    CHECK(s.vheight == 4 && s.lock_height == 2);
    return s;
}

void same_height_block(const VoteState& saved) {
    core restored, forgot;
    restored.restore_vote_state(saved);
    auto a = line(restored, false);
    auto b = line(forgot, false);
    CHECK(!restored.propose(restored.add(a[3], a[3], "other-b4")));
    CHECK(forgot.propose(forgot.add(b[3], b[3], "other-b4")));
    CHECK(restored.votes_without_save == 0);
    std::cout << "ok: no second vote at the same height" << std::endl;
}

void conflict_with_lock(const VoteState& saved) {
    core restored, forgot;
    restored.restore_vote_state(saved);
    auto a = line(restored, false);
    auto b = line(forgot, false);
    // A branch from b1 above the voted height, still carrying the QC of b1, below
    // the lock.
    auto branch = [](core& c, const block_t& b1) {
        block_t x = b1;
        for (int i = 2; i <= 5; ++i) x = c.add(x, b1, "z" + std::to_string(i));
        return x;
    };
    CHECK(!restored.propose(branch(restored, a[1])));
    CHECK(forgot.propose(branch(forgot, b[1])));
    std::cout << "ok: no vote against the saved lock" << std::endl;
}

void waits_for_lock(const VoteState& saved) {
    core restored;
    restored.restore_vote_state(saved);
    CHECK(restored.is_restoring());
    // Without the locked block it does not vote, even where the height allows it.
    block_t g = restored.get_genesis();
    block_t x = g;
    for (int i = 1; i <= 5; ++i) x = restored.add(x, g, "y" + std::to_string(i));
    CHECK(!restored.propose(x));
    CHECK(restored.is_restoring());
    CHECK(restored.persisted.empty());
    // Once delivered, a block extending it gets the vote, and the height still counts.
    auto a = line(restored, false);
    block_t b5 = restored.add(a[4], a[3], "b5");
    CHECK(restored.propose(b5));
    CHECK(!restored.is_restoring());
    CHECK(restored.persisted.back().vheight == 5);
    CHECK(restored.votes_without_save == 0);
    std::cout << "ok: votes again once the lock is back" << std::endl;
}

void lock_moved_past(const VoteState& saved) {
    // Certificates above the saved lock, on a chain without it.  A replica that never
    // stopped would have moved its lock the same way.
    core restored;
    VoteState s = saved;
    s.lock_hash = uint256_t();
    restored.restore_vote_state(s);
    block_t x = restored.get_genesis();
    for (int i = 1; i <= 5; ++i) x = restored.add(x, x, "w" + std::to_string(i));
    CHECK(restored.propose(x));
    CHECK(!restored.is_restoring());
    CHECK(restored.persisted.back().lock_height == 3);
    std::cout << "ok: votes again once the lock moved past the saved one" << std::endl;
}

void corrupt(const std::string& path, long offset) {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    CHECK(f.good());
    f.seekg(offset);
    char c = 0;
    f.get(c);
    f.seekp(offset);
    f.put(static_cast<char>(c ^ 0x5a));
}

void vote_file(const std::string& dir) {
    const std::string path = dir + "/hotstuff_vote_state_test.bin";
    std::remove(path.c_str());
    std::string err;
    uint256_t hash = salticidae::get_hash(std::string("lock"));
    VoteState got{0, 0, uint256_t()};
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(!f.open_existing(path, got));
    }
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(f.create(path, err));
        ariabc_pg::hotstuff_vote_file empty;
        CHECK(!empty.open_existing(path, got));  // created, crashed before the first save
        f.save(VoteState{1, 0, hash});
        f.save(VoteState{2, 0, hash});
        f.save(VoteState{3, 1, hash});  // third save: sequence 3, slot 1
    }
    {
        ariabc_pg::hotstuff_vote_file again;
        CHECK(!again.create(path, err));
    }
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(f.open_existing(path, got));
        CHECK(got.vheight == 3 && got.lock_height == 1 && got.lock_hash == hash);
        // It keeps counting from there, overwriting the older record.
        f.save(VoteState{4, 2, hash});  // sequence 4, slot 0
    }
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(f.open_existing(path, got));
        CHECK(got.vheight == 4 && got.lock_height == 2);
    }
    corrupt(path, 30);  // torn newest record: falls back to the one before
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(f.open_existing(path, got));
        CHECK(got.vheight == 3 && got.lock_height == 1);
    }
    corrupt(path, 4096 + 30);  // nothing intact
    {
        ariabc_pg::hotstuff_vote_file f;
        CHECK(!f.open_existing(path, got));
    }
    std::remove(path.c_str());
    std::cout << "ok: vote file survives a torn write and refuses to be recreated" << std::endl;
}

} // namespace

int main(int argc, char** argv) {
    CHECK(argc == 2);
    core original;
    const VoteState saved = voted_through_b4(original);
    same_height_block(saved);
    conflict_with_lock(saved);
    waits_for_lock(saved);
    lock_moved_past(saved);
    vote_file(argv[1]);
    std::cout << "PASS" << std::endl;
    return 0;
}
