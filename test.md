[10/13, 5:04 PM] Sudarshan Sir: https://github.com/al8n/lsmtree
[10/13, 7:10 PM] Sudarshan Sir: I think they mean something else by the term LSM, it seems it stands for Libra Sparse Merkle tree, nothing to do with LSM.
But anyway, we should be thinking of using LSM storage organization instead of B+-tree index for our merkle relation, which will reduce
random IO greatly.
[10/13, 8:22 PM] Sudarshan Sir: They have a structure called sparse merkle tree which basically is a regular merkle tree, but if a subtree has only one data item it is condensed to a single leaf.    Not very relevant to our use case.
https://diem-developers-components.netlify.app/papers/the-diem-blockchain/2020-05-26.pdf
We should add citation to this white paper though.