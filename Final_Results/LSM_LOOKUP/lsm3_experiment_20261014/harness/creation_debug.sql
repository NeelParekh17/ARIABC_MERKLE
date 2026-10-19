CREATE TABLE debug_lsm(k bigint,v text);
CREATE INDEX debug_lsm_idx ON debug_lsm USING lsm3(merkle_key_hash(k),k);
