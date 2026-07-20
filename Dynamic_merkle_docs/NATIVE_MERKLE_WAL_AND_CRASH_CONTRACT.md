# WAL and crash contract

Native records and root-head publication use PostgreSQL Generic WAL.  A root
is published only after its immutable records have WAL protection, and the
directory head and root record are registered in one Generic WAL operation.
Readers select the newest visible committed root and validate record checksums,
page envelopes and locator generations.  Aborted roots are hints only.

Crash recovery must therefore expose either the pre-transaction root or the
post-transaction root, never a partially published root.  Every mutation uses
the sequence `GenericXLogStart -> GenericXLogRegisterBuffer -> modify the
registered temporary page -> GenericXLogFinish`; the real `BufferGetPage`
image is never initialized or modified before registration, and temporary
page pointers are not dereferenced after finish.  The
`after_native_register_before_finish` failpoint exercises the most important
error/crash boundary.  The existing native record, leaf, internal-node and
root-publication failpoints remain part of the regression/crash matrix.
FREE-to-APPEND reuse increments generation under the buffer lock and is WAL
logged.
