UPDATE usertable_small SET field1='tiny_verify_1' WHERE ycsb_key=1;
INSERT INTO ariabc_internal.raft_apply_epoch (epoch_id, epoch_label, protocol_version) VALUES (decode('00752de8c889e1b56715c9575b4cbe9d1d0e090e102ef20926debe1db49c5866', 'hex'), 'dml-route-test', 1);
UPDATE ariabc_internal.raft_apply_epoch SET epoch_label = 'dml-route-test-updated' WHERE epoch_id = decode('00752de8c889e1b56715c9575b4cbe9d1d0e090e102ef20926debe1db49c5866', 'hex');
DELETE FROM ariabc_internal.raft_apply_epoch WHERE epoch_id = decode('00752de8c889e1b56715c9575b4cbe9d1d0e090e102ef20926debe1db49c5866', 'hex');
