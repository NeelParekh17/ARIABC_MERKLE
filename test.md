We need to read this paper:
Nianzu Sheng, Tong Zhou, He Zhao, Xiaofeng Li, and Jinlin Xu. 2026. SWMT: A
Sliding Window Merkle Tree with Delayed Writes for Scalable Blockchain State
Management. Proceedings of the ACM on Management of Data 4, 1 (2026), 1–25.

Basically the high overheads of individual updates to LSM trees which we have observed can be addressed by LSM trees, something I was planning to look ta.  However, it looks like someone has already done that.  We need to understand what they have done, and ensure it is included in our related work.

https://reilabs.io/blog/scaling-sparse-merkle-trees-to-billions-of-keys-with-largesmt/

https://scalus.org/docs/advanced-data-structures/incremental-merkle-tree

https://scalus.org/docs/advanced-data-structures/merkle-patricia-forestry

Alternatives for proof of database state for query result: Credereum  https://pgconf.ru/en/talk/1588173   also  https://github.com/postgrespro/pg_credereum

We should cite this paper: VeriBench  https://www.vldb.org/pvldb/vol16/p2145-ooi.pdf

This talks of different systems with different threat models and features.  We can say that our approach is different from those studied in VeriBench and explain why.

Also note this about LMPT and SWMT from the references I shared earlier:

The core idea is to separate MPT node computation from MPT node
persistence. The delayed write itself ....  LMPT [2]
and SWMT [3] already adopt it. MPT node computation inserts and updates the MPT nodes
in memory and recomputes the root hash, on the critical path. MPT
node persistence writes those nodes to the key-value store and need
not block the next block.