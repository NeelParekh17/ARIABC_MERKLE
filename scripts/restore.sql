
CREATE TABLE usertable_restore (LIKE usertable INCLUDING ALL);
INSERT INTO usertable_restore SELECT * FROM usertable WHERE ycsb_key <= 12000;
DROP TABLE usertable;
ALTER TABLE usertable_restore RENAME TO usertable;
