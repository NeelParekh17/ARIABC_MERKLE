/* Reviewed illustrative schedules. The reducer is the sole source of visible state. */
const scenarios = [];
function scene(id, title, group, question, take, refs, base, frames, kind='flow') {
  scenarios.push({id,title,group,question,take,refs,base,kind,frames:frames.map((f,i)=>({t:i*3,title:f[0],body:f[1],ref:f[2],ops:f.slice(3)})),duration:(frames.length-1)*3+2});
}
const m=(id,phase,note)=>['move',id,phase,note];
const sql=(id,mode,queue)=>['sql',id,mode,queue];
const v=(id,end)=>['validate',id,end];
const p=id=>['publish',id];
const c=(id,delta={})=>['commit',id,delta];
const a=id=>['abort',id];
const box=(title,items)=>['panel',title,items];
const lens=data=>['lens',data];
const dep=(from,to,label)=>['dep',from,to,label]; // frame-only arrow: writer → reader
const err=(id,code)=>['sqlerror',id,code];
const base=(ids,db={},P=-1,C=-1)=>({ids,db,P,C});

scene('overview','Two execution paths','Start here','What changed in the merged implementation?',
 'Ordinary covered operations keep optimistic execution. Operations needing PostgreSQL semantics restart before publication and execute physically at their ordered position.',
 ['opf','opfwait','publish','commit'],base([0,1],{A:10,B:4}),[
 ['An agreed order','IDs are deterministic log positions, not PostgreSQL XIDs. These paths share the same publication and finalization frontiers.','gate',box('Agreed order',['T0 → T1','P = published prefix','C = finalized prefix'])],
 ['T0 computes privately','Simulation reads its snapshot and builds deferred writes. Nothing is visible in PostgreSQL yet.','baseline',sql(0,'fast',['A ← 15']),m(0,'simulate','A=10 → A←15')],
 ['T0 validates and publishes','With no predecessor, validation is complete. Its footprint releases T1 before deferred apply.','publish',v(0,-1),p(0),m(0,'publish','writer tags for A')],
 ['T1 encounters a trigger','The simulation requests BC010. The entire speculative attempt is discarded before publication.','trigger',sql(1,'fast',[]),['fallback',1,'trigger'],a(1),m(1,'fallback','BC010 → discard')],
 ['The physical route waits','T1 needs its publication turn and every predecessor finalized. P=0 is sufficient for the turn; C is still −1.','opfwait',m(1,'wait','C must reach 0')],
 ['T0 commits','Applying A←15 and finishing the PostgreSQL transaction makes it visible, then finalizes slot 0.','commit',m(0,'commit','visible A=15'),c(0,{A:15})],
 ['T1 runs physical SQL','Only now is its fresh snapshot taken. Ordinary PostgreSQL DML and trigger semantics run while T1 owns the turn.','opfwait',sql(1,'physical',[]),m(1,'physical','trigger runs in log order')],
 ['Release after physical SQL','T1 publishes its footprint after SQL returns. Final PostgreSQL commit is still a separate step.','publish',p(1),m(1,'publish','SQL done; P=1')],
 ['T1 commits','The physical path skips deferred apply and finishes the transaction before finalizing its slot.','commit',m(1,'commit','B=5 visible'),c(1,{B:5})]
]);
scene('paper','Paper: wait for committed predecessors','Paper → source','What exactly does Algorithm 1 allow?',
 'The paper already checks newly committed intervals. Independent SQL avoids resimulation but still waits for every predecessor to commit. Separate P and frozen-queue settlement are implementation refinements.',
 ['paper','baseline','publish'],base([0,1],{A:10,B:4}),[
 ['Run_Tx on a snapshot','ProtectDB §4, printed pages 6–7: record the committed baseline, run SQL, and discover dependencies.','paper',sql(0,'fast',['A ← 15']),sql(1,'fast',['B ← 8']),m(0,'simulate','writes A'),m(1,'simulate','writes B')],
 ['No conflict is not permission to commit','T1 is independent, but Algorithm 1 waits while the committed boundary differs from k−1. The diagram uses source IDs starting at zero.','paper',m(1,'wait','independent; still waits'),box('Paper gate',['T1 needs predecessor commit','Check newly committed intervals','Conflict → new execution'])],
 ['T0 reaches Commit','Algorithm 1 abstracts data commit, footprint storage and boundary advancement in one operation. This scene presents them together; P is the matching metadata boundary for comparison with the code.','paper',v(0,-1),p(0),c(0,{A:15}),m(0,'commit','Commit completed')],
 ['T1 checks the new committed interval','T1 checks the newly committed predecessor. No predecessor changes B, so its attempt remains valid without rerunning SQL.','paper',v(1,0),m(1,'wait','all predecessors committed')],
 ['T1 reaches its serial position','Now Algorithm 1 permits Commit: apply its accepted changes, store metadata, advance the committed boundary and signal.','paper',p(1),c(1,{B:8}),m(1,'commit','Commit completed')],
 ['Compare the implementation next','Here data commit and metadata readiness progressed together. The code’s fast path releases publication before deferred apply and commit; the next scene shows that refinement.','paper',box('Paper versus source',['Paper Commit: data + metadata boundary','Independent SQL still waits for predecessors','Source refinement: separate P can outrun C'])]
],'paper');
scene('overlap','Publication outruns commit','Fast path','How can later transactions finish before an earlier one?',
 'P releases ordered metadata. C crosses only a contiguous run of ready slots. Independent commits can finish out of order without making C skip a hole.',
 ['publish','prefix','commit'],base([0,1,2],{A:0,B:0,D:0}),[
 ['Three private simulations','Each writes a different logical key. All snapshots begin before any commit.','baseline',sql(0,'fast',['A ← 1']),sql(1,'fast',['B ← 1']),sql(2,'fast',['D ← 1']),m(0,'simulate','writes A'),m(1,'simulate','writes B'),m(2,'simulate','writes D')],
 ['T0 publishes first','Writer tags become shared; A is still zero in the visible database.','publish',v(0,-1),p(0),m(0,'apply','slow apply')],
 ['T1 follows','Validation finds no missing predecessor writer for B. Publication stays in sequence.','publish',v(1,0),p(1),m(1,'apply','independent apply')],
 ['T2 follows','T2 validates through 1 and publishes. P=2 while C=−1.','publish',v(2,1),p(2),m(2,'apply','independent apply')],
 ['T1 physically commits first','Slot 1 is ready. C remains −1 because slot 0 is not ready.','prefix',c(1,{B:1}),m(1,'commit','ready; behind hole')],
 ['T2 physically commits','Slot 2 is ready too. The missing slot 0 still prevents prefix advancement.','prefix',c(2,{D:1}),m(2,'commit','ready; behind hole')],
 ['T0 closes the hole','After T0 commits, the contiguous scan crosses ready slots 0,1,2. C jumps to 2.','prefix',c(0,{A:1}),m(0,'commit','prefix now 2')]
]);
scene('early','Conflict before publication','Fast path','Why validate while waiting?',
 'A waiter can detect a newly published predecessor before owning its turn. It discards the private attempt, waits for that writer to finalize, then obtains a fresh snapshot.',
 ['early','turn','baseline'],base([0,1,2],{A:0,B:0,D:0}),[
 ['T2 computes from old A','T2 reads A=0 and queues B←0. T1 will write A. None of this SQL is published yet.','baseline',sql(0,'fast',['D ← 1']),sql(1,'fast',['A ← 1']),sql(2,'fast',['B ← 0']),m(0,'simulate','writes D'),m(1,'simulate','writes A'),m(2,'simulate','reads A=0')],
 ['T0 publishes','T2 remains behind T1: P=0 does not give T2 its publication turn.','gate',v(0,-1),p(0),m(0,'apply','apply D'),m(2,'wait','needs P≥1')],
 ['T1 publishes A','The shared writer metadata now contains a predecessor write that T2 did not see.','publish',v(1,0),p(1),m(1,'apply','A←1 not committed')],
 ['Waiting validation detects A','A scheduled early-validation check sees the predecessor tag and rejects T2’s private attempt. A publication alone does not interrupt every worker.','early',a(2),m(2,'conflict','R(A) meets W₁(A)'),dep(1,2,'W₁(A) ↦ R(A)'),box('Dependency match',['T2 read: A','T1 published write: A','Discard B←0 before release'])],
 ['Wait for the conflicting writer','T0 and T1 finish. The next attempt waits for the remembered conflicting predecessor, not just its metadata.','baseline',c(0,{D:1}),c(1,{A:1}),m(0,'commit','C=1 after both'),m(1,'commit','A=1 visible'),m(2,'wait','writer finalized')],
 ['Fresh SQL attempt','Now T2 reads A=1. Its old deferred queue was discarded, so B←1 is computed anew.','baseline',sql(2,'fast',['B ← 1']),m(2,'simulate','attempt 2; reads A=1')],
 ['Validate and release','T2 validates through 1, then publishes the new accepted footprint.','turn',v(2,1),p(2),m(2,'publish','accepted B←1')],
 ['Apply and commit','The resulting state equals T0,T1,T2 serially: A=1, B=1.','commit',c(2,{B:1}),m(2,'commit','B=1 visible')]
],'tags');
scene('relation','Empty scans now have coverage','Coverage','How is an empty secondary range protected?',
 'No chosen-key equality prefix means a relation read tag, even with no returned tuple. Every tracked write publishes the matching relation tag. It is conservative: unrelated rows in the same relation can cause retries.',
 ['relationread','relationwrite','btfirst','heapscan'],base([0,1],{matches:0,B:0}),[
 ['T1 finds no matching rows','An empty secondary-range scan cannot derive tuple tags from returned rows. The scan registers a relation tag when no chosen-key prefix is bound.','relationread',sql(0,'fast',['insert matching row']),sql(1,'fast',['B ← 0']),m(0,'simulate','insert into R'),m(1,'simulate','COUNT matches = 0'),lens({type:'tags',read:'relation R',write:'relation R',shared:false,match:false}),box('Read coverage',['R(R): relation-level read','Empty result still has a tag','Chosen-key equality can use a narrower prefix'])],
 ['Every writer announces relation membership','T0 reserves ordinary write keys and a publish-only relation tag. Publish-only membership does not make every writer conflict with every other writer.','relationwrite',v(0,-1),p(0),m(0,'apply','W₀(R) published'),lens({shared:true}),box('Shared writer metadata',['W₀(key)','publish-only W₀(R)','Readers of R will check it'])],
 ['The relation dependency conflicts','T1’s relation read meets the missing predecessor write. It cannot publish the zero-count decision.','digest',a(1),m(1,'conflict','R(R) ∩ W₀(R)'),dep(0,1,'W₀(R) ↦ R(R)'),lens({match:true}),box('Why the old gap is closed',['No returned tuple needed','Tag reaches digest and writer maps','Conservative false positives are possible'])],
 ['Predecessor commit then fresh scan','The inserted row becomes visible only at T0’s commit.','commit',c(0,{matches:1}),m(0,'commit','matches=1'),lens({match:false,covered:true})],
 ['Recomputed result is one','The new SQL attempt sees the serial predecessor state. Heap/non-B-tree scans also reserve relation reads.','heapscan',sql(1,'fast',['B ← 1']),m(1,'simulate','COUNT matches = 1')],
 ['T1 finalizes the corrected decision','Validate, publish, apply and commit B←1.','publish',v(1,0),p(1),c(1,{B:1}),m(1,'commit','B=1')]
],'tags');
scene('oldkey','A key move announces both ends','Coverage','What happens when primary key 7 becomes 8?',
 'When UPDATE targets a chosen-key column, the hook fetches the old tuple and reserves its logical write tags as well as the new-slot tags. An old-key reader now detects the predecessor.',
 ['oldkey','modifyold','relationwrite'],base([0,1],{'row key':7,'found key 7':true}),[
 ['T1 reads key 7','Its old snapshot finds the row. T0 plans to change the chosen primary key from 7 to 8.','oldkey',sql(0,'fast',['key 7 → key 8']),sql(1,'fast',['found7 ← true']),m(0,'simulate','key move 7→8'),m(1,'simulate','R(key 7)'),lens({type:'tags',read:'key 7',write:'keys 7 + 8',shared:false,match:false}),box('Old and new slots',['old key: 7','new key: 8','Read tag: key 7'])],
 ['Fetch old key before losing it','The UPDATE hook passes updated/extraUpdated columns; a chosen-key change triggers old-version tag reservation.','modifyold',m(0,'simulate','W(7) + W(8)'),box('Writer footprint',['old key 7','new key 8','relation membership'])],
 ['Publish both logical keys','T1’s key-7 dependency is present even though key 7 will disappear.','publish',v(0,-1),p(0),m(0,'apply','old + new tags published'),lens({shared:true})],
 ['Reject the stale read','T1 discards its decision before publication, then waits for the key-moving predecessor.','early',a(1),m(1,'conflict','R(7) meets W₀(7)'),dep(0,1,'W₀(7) ↦ R(7)'),lens({match:true})],
 ['The move becomes visible','T0 finishes PostgreSQL commit.','commit',c(0,{'row key':8}),m(0,'commit','key=8')],
 ['Fresh lookup no longer finds 7','Re-executing at the refreshed snapshot produces the serial result.','baseline',sql(1,'fast',['found7 ← false']),m(1,'simulate','key 7 absent'),lens({match:false,covered:true})],
 ['The new result finalizes','The example concerns the chosen primary key; it does not assert arbitrary index expressions have identical tagging.','commit',v(1,0),p(1),c(1,{'found key 7':false}),m(1,'commit','found7=false')]
],'tags');
scene('overlay','See your own deferred writes','Own writes','How can later commands read an UPDATE that was deferred?',
 'A backend-local map overlays earlier-command updates and hides deletes. Repeated updates to the same TID compose into one queue entry. Same-command writes stay invisible until the command counter advances.',
 ['overlay','compose','indexonly','scanpending','lockrows'],base([0],{value:10}),[
 ['Fetch the committed row','This example changes a non-indexed value, so the optimistic overlay is sufficient.','overlay',sql(0,'fast',[]),m(0,'simulate','physical value=10'),lens({type:'overlay',heap:'10',pending:'—',visible:'10',command:0}),box('Fetch → overlay → SQL',['Heap/index fetch: 10','Pending TID entry: none','SQL sees: 10'])],
 ['First command queues 11','The shared database remains 10. The deferred entry belongs to command 0.','compose',['queue',0,['same TID: value ← 11 (cid 0)']],m(0,'simulate','private value←11'),lens({pending:'11'}),box('Command visibility',['entry.cid = 0','current command = 0','Same-command fetch sees physical 10'])],
 ['Command counter advances','A later command fetches physical 10, then the overlay substitutes the earlier deferred value 11.','overlay',lens({visible:'11',command:1}),box('Fetch → overlay → SQL',['Heap/index fetch: 10','Pending earlier command: 11','SQL sees: 11']),m(0,'simulate','next command sees 11')],
 ['Second update composes to 12','The operation starts from 11, not 10. The same TID queue entry is replaced with the latest value.','compose',['queue',0,['same TID: value ← 12 (cid 1)']],lens({pending:'12',visible:'11'}),box('Composed queue',['One TID entry','Latest value: 12','No duplicate physical UPDATE needed'])],
 ['Index-only plans consult the heap','With pending writes, an all-visible index-only scan still fetches the heap so overlay and deletion checks run.','indexonly',lens({visible:'12',command:2}),box('Index-only read path',['Index tuple → heap fetch','Overlay earlier command value','SQL sees: 12']),m(0,'simulate','index-only sees 12')],
 ['A deferred delete hides the row','After a further command boundary, the overlay clears the slot and returns false.','overlay',['queue',0,['same TID: DELETE']],lens({pending:'DELETE',visible:'no row',command:3}),box('Fetch → overlay → SQL',['Heap still stores: 10','Pending entry: DELETE','Later SQL sees: no row']),m(0,'simulate','later command: row absent')],
 ['Apply the composed delete once','Only after validation and publication does the queued deletion reach the physical database.','publish',v(0,-1),p(0),m(0,'apply','frozen DELETE queue')],
 ['Commit changes visibility','The physical database now has no row. Inserts or scans affected by indexed changes use the fallback scene instead.','commit',c(0,{value:'absent'}),m(0,'commit','row absent'),lens({heap:'no row',pending:'—'})]
],'overlay');
const fallbackReasons = {
 trigger:{name:'Trigger / foreign-key trigger',ref:'trigger',why:'A target relation has triggers. Fallback is requested before statement-trigger execution; ordinary PostgreSQL runs the trigger on the physical attempt.'},
 upsert:{name:'INSERT … ON CONFLICT',ref:'upsert',why:'The INSERT conflict path requests fallback, including DO NOTHING and DO UPDATE. PostgreSQL resolves the conflict on the ordered physical attempt.'},
 sequence:{name:'nextval() / setval()',ref:'sequence',why:'Sequence mutation requests fallback before speculative consumption. The physical attempt reaches the sequence only after predecessors finish.'},
 insert:{name:'Scan after own INSERT',ref:'scanpending',why:'A pending INSERT is absent from the physical heap/index. A later scan requests fallback rather than silently missing that row.'},
 indexed:{name:'Scan after indexed UPDATE',ref:'indexpending',why:'Affected index columns or a changed partial-index membership can add a row with no old index entry. The guard runs before the index scan can miss it.'}
};
scene('fallback','Ordered physical fallback','Own writes','When must ordinary PostgreSQL take over?',
 'BC010 abandons the entire unpublished simulation. The new attempt waits for its publication turn AND the committed predecessor prefix, then runs physical SQL. It releases P after SQL returns, before final commit.',
 ['opf','opfheader','opfwait','opfretry','trigger','upsert','sequence','indexpending','indexguard'],base([0,1],{value:10,effect:0}),[
 ['T1 begins optimistically','Choose a reason above. The route is common; reason-specific guards have separate source references.','opf',sql(0,'fast',['value ← 11']),sql(1,'fast',['speculative work']),m(0,'simulate','predecessor update'),m(1,'simulate','unpublished attempt')],
 ['Guard requests BC010','needs_opf is set before raising the internal error. No successor has been released by T1.','opf',['fallback',1,'selected guard'],a(1),m(1,'fallback','discard whole attempt'),box('Restart boundary',['needs_opf = true','Abort simulation and deferred queue','Keep fallback state across PG error unwinding'])],
 ['Acquire the publication turn','T0 publishes. T1 can own the next turn but still cannot run physical SQL while C=−1.','opfwait',v(0,-1),p(0),m(0,'apply','value←11'),m(1,'wait','has turn; awaits C≥0')],
 ['Every predecessor finalizes','T0 commits. Now T1 can obtain its fresh baseline and snapshot.','opfwait',c(0,{value:11}),m(0,'commit','C=0'),m(1,'wait','predecessors complete')],
 ['Ordinary physical SQL','Deferral is disabled for the fallback attempt. SQL, its own writes, triggers and normal PostgreSQL checks execute at this ordered position.','physical',sql(1,'physical',[]),m(1,'physical','SQL holds publication turn'),box('Physical route',['C≥s−1 before snapshot','No speculative sequence/trigger effects retained','Business SQL executes before P release'])],
 ['SQL returns, then metadata publishes','Physical DML records tags. Deferred apply is skipped. P advances now; final PostgreSQL commit follows.','publish',p(1),m(1,'publish','P=1; not committed')],
 ['Final commit makes effects visible','Success finalization occurs after PostgreSQL commit; errors use the ordered terminal outcome path.','commit',c(1,{effect:1}),m(1,'commit','physical effect visible')]
]);
scene('caught','Catching BC010 cannot bypass fallback','Own writes','What if a PL/pgSQL exception handler catches the internal error?',
 'The attempt remains simulated and needs_opf stays set. The worker checks this after SQL returns and raises BC010 again; it never accepts a partially simulated attempt as physical execution.',
 ['caught','opfretry','opf'],base([0],{effect:0}),[
 ['Begin a simulated function','A PL/pgSQL block starts ordinary SQL execution in the unpublished attempt.','caught',sql(0,'fast',['earlier deferred update']),m(0,'simulate','PL/pgSQL simulation')],
 ['A guarded operation requests fallback','needs_opf becomes true. A handler can catch ERROR, but it must not turn simulation into immediate physical writes.','opf',['fallback',0,'caught guard'],m(0,'fallback','BC010 raised')],
 ['The handler returns from SQL','For this illustration the handler catches the error. needs_opf is still true and bcdb_dt_simulating stays true.','caught',m(0,'simulate','caught; still deferred'),box('Flags after caught error',['bcdb_dt_simulating = true','needs_opf = true','No publication yet'])],
 ['Worker re-raises the internal restart','The post-SQL check rejects this attempt; PG_CATCH goes to opf_retry only before publication.','caught',a(0),m(0,'fallback','discard caught attempt')],
 ['Rerun physically at the serial position','T0 has no predecessors. A fresh transaction and snapshot execute ordinary SQL with simulation off.','opfwait',sql(0,'physical',[]),m(0,'physical','complete physical attempt')],
 ['Publish after physical SQL','Only the complete physical attempt releases successors.','publish',p(0),m(0,'publish','P=0')],
 ['Commit and finalize','No speculative partial result or deferred queue is carried into the final outcome.','commit',c(0,{effect:1}),m(0,'commit','C=0')]
]);
scene('settle','After publication, freeze decisions','Fast path','Why is a late SQL rerun forbidden?',
 'Under publication gating with default post-publish settle enabled, eligible apply failures retry the same deferred queue. They do not re-execute business SQL after successors were released.',
 ['settle','applyretry','terminal'],base([0,1,2],{A:0,B:0,D:0}),[
 ['T1 computes B from A','T1 reads A=0, producing B←0. T2 will independently write A=1. T0 writes another key.','baseline',sql(0,'fast',['D ← 1']),sql(1,'fast',['B ← 0']),sql(2,'fast',['A ← 1']),m(0,'simulate','writes D'),m(1,'simulate','A=0 → B←0'),m(2,'simulate','A←1')],
 ['T0 and T1 release successors','T1 is validated through 0 and publishes its B footprint. Its computed B←0 is now frozen.','publish',v(0,-1),p(0),v(1,0),p(1),m(0,'apply','slow predecessor'),m(1,'apply','frozen B←0')],
 ['T1’s apply needs settlement','Internal subtransaction rollback removes the failed application’s partial effects. Business SQL remains at one accepted computation.','applyretry',m(1,'settle','same queue; wait C≥0'),box('Frozen decision',['Original read: A=0','Deferred write: B←0','Rerunning SQL now would be too late'])],
 ['T2 publishes and commits A=1','Its writer key A does not conflict with the published writer key B. T1 is still unfinished.','publish',v(2,1),p(2),c(2,{A:1}),m(2,'commit','A=1 visible')],
 ['T0 finalizes','Now C=0. Eligible settlement retries apply the unchanged B←0 queue. A fresh business-SQL rerun could incorrectly observe successor A=1.','settle',c(0,{D:1}),m(0,'commit','C=0'),m(1,'apply','retry exactly B←0')],
 ['T1 commits the original decision','B remains 0, matching log order T0,T1,T2. C can now cross the already-ready T2.','commit',c(1,{B:0}),m(1,'commit','B=0; C=2'),box('Serial-equivalent example',['Final A=1, B=0','SQL attempts for T1: 1','Queue values never changed after publication'])]
]);
scene('digest','Validate only the new suffix','Coverage','How do digests speed up conflict checks?',
 'The first full-map check anchors validated_through. Later checks inspect new publication slots, including relation tags. Owner mismatch, overflow or unavailable history falls back to full writer maps.',
 ['digest','ring','dedup'],base([3,4,5],{A:0},2,2),[
 ['T5 simulates at prefix 2','Its dependencies are private and deduplicated with distinct read/checked-write/publish-only membership.','dedup',sql(3,'fast',[]),sql(4,'fast',[]),sql(5,'fast',[]),m(3,'simulate','independent'),m(4,'simulate','independent'),m(5,'simulate','read A'),box('Validation interval',['baseline = 2','validated_through = 2','No predecessor skipped'])],
 ['T3 publishes','T5 remains behind unpublished T4. First full-map checking anchors publication boundary 3.','digest',v(3,2),p(3),v(5,3),m(3,'apply','digest owner 3'),m(5,'wait','validated through 3'),box('Digest history',['Slot owner: 3','Read/checked-write overlap: none','validated_through → 3'])],
 ['T4 publishes','A later check sees only suffix (3,4]. The upper endpoint never exceeds T5−1.','digest',v(4,3),p(4),v(5,4),m(4,'apply','digest owner 4'),box('Digest history',['New suffix: (3,4]','Relation tags are included','validated_through → 4'])],
 ['A damaged or reused slot is not trusted','The validator checks slot ownership around payload reads and falls back on overflow/absence. This card explains the branch; no actual overflow is being measured.','ring',box('Fallback conditions',['8,192 ring slots','Up to 384 tag hashes per slot','Owner mismatch / overflow → full maps'])],
 ['T5 owns the turn','Its complete checked interval reaches 4, so it can publish. Correct map-history retention remains a requirement.','turn',p(5),m(5,'publish','through s−1=4')],
 ['Finalize the illustrated items','All successful items commit, closing the contiguous prefix. Digest hashes are dependency summaries, not Merkle roots.','prefix',c(3),c(4),c(5),m(3,'commit','ready'),m(4,'commit','ready'),m(5,'commit','C=5')]
],'digest');
scene('random','Repeat the same random stream','Coverage','How does random() survive retries and replicas?',
 'Each DT business-SQL attempt seeds PostgreSQL random() from the deterministic ID. The same ID and same call order repeat the stream; this does not normalize wall-clock time, external data or arbitrary SQL side effects.',
 ['seed','random','longsql','signature'],base([0,1],{A:0,sample:'unset'}),[
 ['Two replicas execute T1','Both derive the same seed from ID 1. Symbols r₁[0], r₁[1] denote stream positions, not measured numeric samples.','random',sql(0,'fast',['A ← 1']),sql(1,'fast',['sample ← r₁[0]']),m(0,'simulate','writes A'),m(1,'simulate','seed(ID=1)'),lens({type:'random',attempt:1}),box('Replica streams',['Replica α: r₁[0], r₁[1]','Replica β: r₁[0], r₁[1]','seed = ID XOR 0xBCDB13579BDF'])],
 ['A predecessor invalidates the attempt','T1’s read of A was stale. The private attempt and its queue are discarded before publication.','early',v(0,-1),p(0),a(1),m(0,'apply','A←1'),m(1,'conflict','discard attempt 1'),dep(0,1,'W₀(A) ↦ R(A)')],
 ['Wait for that predecessor','T0 commits before T1 takes its retry snapshot.','baseline',c(0,{A:1}),m(0,'commit','A=1')],
 ['Seed again before attempt 2','The code calls bcdb_seed_random before business SQL on every attempt, including the physical fallback route.','seed',sql(1,'fast',['sample ← r₁[0]']),m(1,'simulate','same stream restarts'),lens({type:'random',attempt:2}),box('Retry stream',['Attempt 1: r₁[0], r₁[1]','Attempt 2: r₁[0], r₁[1]','Same call order is required'])],
 ['The complete SQL reaches the worker','Long SQL now uses length-aware signature copying and DSA storage beyond the shared inline buffer. This protects the actual program being replayed.','longsql',box('Program fidelity',['Inline SQL buffer: 1,024 bytes','Long SQL: separate DSA allocation','No silent truncation at the old boundary'])],
 ['Finalize the accepted sample','The illustrative stream symbol is identical across replicas; no claim is made about arbitrary volatile functions.','commit',v(1,0),p(1),c(1,{sample:'r₁[0]'}),m(1,'commit','same specified sample')]
],'random');
scene('errors','SQL errors are completed outcomes','Outcomes','How is an ordered failure returned without becoming an infrastructure failure?',
 'An error raised while simulating is trusted only if the snapshot already contained every predecessor (baseline = s−1). Otherwise it is re-run on the ordered physical route, and an error there is the serial one. The catch path writes its real SQLSTATE to the item’s result slot and finalizes the slot in log order; bcdb_finalized=1 makes the executor complete the item instead of failing the task.',
 ['errorcatch','staleerror','execconstraints','finalized','errorhelper','errorcomplete','terminalerror'],base([0,1],{value:10}),[
 ['T1 violates a CHECK while simulating','T1 inserts −1 into a CHECK(v ≥ 0) column. ExecConstraints runs before the write would be deferred, so the error is raised inside the speculative attempt, which rolls back.','execconstraints',sql(0,'fast',['value ← 11']),sql(1,'fast',[]),m(0,'simulate','predecessor, still private'),err(1,'23514'),m(1,'simerror','SQLSTATE 23514')],
 ['A possibly stale error is re-run','T1’s baseline is −1, below s−1 = 0, so its snapshot may miss T0. The catch path does not trust the error: it requests BC010 and restarts on the ordered physical route.','staleerror',['fallback',1,'stale error'],a(1),m(1,'fallback','error → BC010')],
 ['T0 publishes and commits','T1 waits for its turn and for every predecessor to commit.','opfwait',v(0,-1),p(0),c(0,{value:11}),m(0,'commit','C = 0'),m(1,'wait','has turn; C ≥ 0')],
 ['The physical run fails too: the error is genuine','The fresh snapshot includes every predecessor and the INSERT still violates the CHECK. The catch path writes “ERROR 23514” into result slot 1, marks it publication-ready and advances C. P and C move to 1; no T1 data becomes visible.','errorcatch',sql(1,'physical',[]),err(1,'23514'),['pready',1],['terminal',1,'23514'],m(1,'error','final: ERROR 23514'),box('Slot 1 after the catch',['result: ERROR 23514','publication-ready, no footprint','P = C = 1'])],
 ['The client gets the real SQLSTATE','The error is re-raised with DETAIL bcdb_finalized=1. Inline “s <txid>” callers whose terminal error was decided after the turn get it from a re-raise placed after PG_TRY.','finalized',m(1,'result','ERROR 23514'),box('Backend response',['ERROR SQLSTATE=23514','DETAIL bcdb_finalized=1','No T1 data effects'])],
 ['The executor completes the item','Parsing the detail adds finalized=1 to the error result. Only marked outcomes are excluded from task-failure handling; unmarked infrastructure errors still fail.','errorhelper',box('Executor classification',['Business outcome: ERROR 23514','Protocol item: completed','Unmarked infrastructure errors still fail'])],
 ['Completion is not SQL success','Consumers receive the error code. Direct-completion success_count bookkeeping is not a count of successful business SQL. The next chapter shows a stale error that the re-run turns into success.','errorcomplete',box('Separate meanings',['Item completed: yes','SQL succeeded: no','Business error code preserved'])]
],'errors');
scene('staleerror','A stale error is re-run, not finalized','Outcomes','What if the error only happens on a stale snapshot?',
 'An error raised while simulating on a snapshot that may miss predecessors is a stale decision. It now takes the ordered physical route (BC010): wait for the turn and for every predecessor to commit, then re-run the SQL. Only an error on that run is final. Before this fix the catch path finalized the stale error, and det differed from serial (reproduced on .247 at W=8, 32/32).',
 ['staleerror','opfwait','execconstraints','errorcatch'],base([0,1],{v:0}),[
 ['Serial order says both succeed','T0 adds 1 to v (slowly). T1 subtracts 1 under CHECK(v ≥ 0). Run serially, T0 then T1, v ends at 0 and both statements succeed.','execconstraints',sql(0,'fast',['v ← 1']),sql(1,'fast',[]),m(0,'simulate','slow: v ← 1'),m(1,'simulate','reads v = 0 (stale)'),box('Serial reference',['T0: v 0 → 1','T1: v 1 → 0, UPDATE 1','Final v = 0'])],
 ['T1 fails on its old snapshot','T1’s snapshot predates T0, so it computes v = −1 and ExecConstraints raises 23514 inside the simulation. T0’s write to v is still unpublished.','execconstraints',err(1,'23514'),m(1,'simerror','v = −1 fails CHECK'),dep(0,1,'W₀(v) not yet visible'),box('Why the error is suspect',['Snapshot baseline < s−1','A predecessor may change the outcome','The error is a speculative decision'])],
 ['The error becomes BC010','The catch path sees an error raised while simulating with baseline < s−1. Instead of finalizing it, it sets needs_opf and restarts through the ordered physical route. The old build finalized ERROR 23514 here.','staleerror',['fallback',1,'stale error'],a(1),m(1,'fallback','stale error → BC010'),box('Catch-path decision',['Simulating and baseline < s−1 → re-run','Baseline = s−1 → error is final','Safe-ledger whitelisted errors: same rule'])],
 ['T0 publishes and commits','T1 waits for its turn and for every predecessor to commit. T0 publishes and commits v = 1.','opfwait',v(0,-1),p(0),c(0,{v:1}),m(0,'commit','v = 1; C = 0'),m(1,'wait','has turn; C ≥ 0')],
 ['T1 re-runs physically','A fresh snapshot now includes T0, so T1 reads v = 1 and computes 0. The CHECK passes. An error on this run would be the serial one and would be finalized in order.','opfwait',sql(1,'physical',[]),m(1,'physical','v 1 → 0, CHECK passes')],
 ['Publish and commit: same as serial','T1 publishes after SQL returns and commits. Final v = 0 and T1 = UPDATE 1, matching serial order. Repro after the fix: 5 stale-error families, 15 / 15 PASS at W=8; the mixed case keeps exactly the 21 genuine serial errors.','commit',p(1),c(1,{v:0}),m(1,'commit','v = 0, UPDATE 1'),box('Repro on .247 (fixed build)',['Old build: 5 / 5 families differ from serial','Fixed build: 15 / 15 runs PASS','Genuine errors still returned in order'])]
],'errors');
scene('merkle','Coalesced deltas carry net counts','Outcomes','Why must a staged Merkle entry carry its own row count?',
 'When changes merge into the same delta key, hash changes XOR but row-count changes add. Drop an entry only if both are zero. The newest commit carries count_delta through staging, merging and apply.',
 ['merklemerge','merklestage','merkleapply','merklebatched'],base([0],{'rows at leaf':0}),[
 ['Start with an empty leaf','Begin after selecting the ordered physical route; the unpublished simulation was discarded. One physical transaction inserts, deletes, then reinserts identical logical row bytes with symbolic hash h.','merklestage',sql(0,'fast',[]),['fallback',0,'physical route selected'],a(0),sql(0,'physical',[]),m(0,'physical','transaction-local deltas'),lens({type:'merkle',ih:'0',ic:0,dh:'0',dc:0}),box('Transaction-local staging',['INSERT entry: XOR=0, count=0','DELETE entry: XOR=0, count=0','Committed leaf: hash 0, count 0'])],
 ['First INSERT delta','Stage an INSERT contribution with hash h and count +1. The row labels are illustrative; only changes sharing the same delta key coalesce.','merklestage',lens({ih:'h',ic:1}),box('Transaction-local staging',['INSERT entry: XOR=h, count=+1','DELETE entry: XOR=0, count=0','Committed leaf unchanged'])],
 ['A DELETE contribution','Deletion carries the same hash h but count −1 in its own delta type/key.','merklestage',lens({dh:'h',dc:-1}),box('Transaction-local staging',['INSERT entry: XOR=h, count=+1','DELETE entry: XOR=h, count=−1','Committed leaf unchanged'])],
 ['Second identical INSERT merges','The INSERT hash becomes h XOR h = 0, but its count becomes +2. The entry must survive even with zero hash delta.','merklemerge',lens({ih:'0',ic:2}),box('Merged entries',['INSERT: XOR=0, count=+2 — keep','DELETE: XOR=h, count=−1','Combined change: XOR=h, count=+1'])],
 ['Apply uses the accumulated count','Both normal and batched apply use entry.count_delta. It must not infer one row from the number of entries.','merkleapply',box('Apply rule',['hash ← hash XOR entry.xor_delta','count ← count + entry.count_delta','Discard only if hash=0 AND count=0'])],
 ['A failed subtransaction discards both','If an additional subtransaction adds tentative changes and aborts, its entire frame is discarded. The already illustrated outer-transaction contributions remain intact.','merklemerge',box('Rollback boundary',['Aborted frame: hash changes discarded','Aborted frame: count changes discarded','Outer net change remains: XOR=h, count=+1'])],
 ['Commit the net effect','The final logical leaf has one row and hash h. The two identical INSERT hashes cancelled, but their +2 count survived to combine with the DELETE’s −1.','merkleapply',p(0),c(0,{'rows at leaf':1}),m(0,'commit','leaf hash h; count 1'),box('Final leaf',['hash: 0 XOR h = h','count: 0 + 2 − 1 = 1','Both state components agree'])]
],'merkle');
scene('lanes','FIFO lanes keep predecessors runnable','Scheduling','Why do connections use deterministic lanes?',
 's modulo W assigns a lane; ordered admission and FIFO dispatch let earlier IDs run before later IDs occupy that lane. The decreasing-ID wait argument assumes fairness and finite predecessor work.',
 ['lanes','fifo','gate'],base([0,1,2],{A:0}),[
 ['Assign three items to two lanes','W=2: T0 and T2 belong to lane 0, T1 to lane 1. One active item per lane.','lanes',box('Connection queues',['Lane 0: T0 → T2','Lane 1: T1','Head item dispatches first']),m(2,'queued','lane 0 queue')],
 ['Dispatch FIFO heads','T0 and T1 can simulate concurrently. T2 stays queued until T0 releases its lane.','fifo',sql(0,'fast',[]),sql(1,'fast',[]),m(0,'simulate','lane 0 active'),m(1,'wait','lane 1; waits for 0')],
 ['Earlier ID publishes and finishes','The missing predecessor is already runnable, rather than stuck outside a pool filled by later IDs.','gate',v(0,-1),p(0),c(0),m(0,'result','lane 0 now available')],
 ['Dispatch the next lane-0 item','T2 can simulate. T1 publishes and finalizes at its ordered position.','fifo',sql(2,'fast',[]),m(2,'simulate','lane 0 active'),v(1,0),p(1),c(1),m(1,'result','lane 1 complete')],
 ['T2 follows in order','Modulo affinity alone is insufficient without ordered arrival and FIFO execution; this scene assumes both.','gate',v(2,1),p(2),c(2),m(2,'result','C=2'),box('Scope of the liveness argument',['Ordering/lane waits point to smaller IDs','Does not rule out DB-lock deadlocks','Missing IDs or crashed workers need recovery'])]
],'lanes');
scene('boundaries','State, results and delivery','Outcomes','What does each boundary actually guarantee?',
 'Deterministic execution, PostgreSQL commit, state hashing, result payload selection and Kafka delivery are separate mechanisms. The animation models the reviewed paths, not a live database or a universal SQL proof.',
 ['payload','actual','delivery','defaults','paper'],base([0],{value:10}),[
 ['Execute the agreed SQL','This walkthrough selects conflict-tracked DT and publication gating. Source defaults and a real run’s effective settings must be read separately.','defaults',sql(0,'fast',['value ← 11']),m(0,'simulate','covered deterministic SQL'),box('Assumptions',['Same starting logical state and ordered log','Complete dependency coverage and retained history','Same admitted SQL semantics on every replica'])],
 ['Publication releases an ordering turn','P=0 does not mean the data is visible or a client was acknowledged.','publish',v(0,-1),p(0),m(0,'publish','metadata shared')],
 ['Commit then finalization','The successful SQL effects become visible before C represents this slot. Merkle maintenance does not by itself establish which roots are compared by every mode.','commit',c(0,{value:11}),m(0,'commit','data visible; C=0')],
 ['Select result payload','Ordinary block mode can return completion receipts unless actual results are enabled. Safe-ledger and business-abort handling have their own paths.','payload',m(0,'result','result or receipt'),box('Payload choice',['Completion receipt ≠ SELECT result equality','Actual-result setting changes payload','Finalized SQL errors preserve SQLSTATE'])],
 ['Queue or wait for delivery','Ordinary Kafka sends queue asynchronous work. Safe paths explicitly wait for delivery; client majority acceptance is another boundary.','delivery',box('Delivery boundaries',['PostgreSQL commit','Kafka send → delivery callback','Gateway/client acceptance'])],
 ['Read the guarantee at its actual scope','random() now repeats by ID; clock time/external input still need a contract. Source excerpts support the illustrated mechanisms; finite audits do not certify unrestricted SQL.','paper',box('What the draft and code establish',['Paper: ordered committed-prefix execution','Code: fast path + physical fallback refinements','This page: source-derived illustrative schedules'])]
],'boundaries');

function initialState(s) {
  return {P:s.base.P,C:s.base.C,startP:s.base.P,startC:s.base.C,db:{...s.base.db},tx:Object.fromEntries(s.base.ids.map(id=>[id,{id,phase:'queued',note:'ordered input',queue:[],sqlRuns:0,aborts:0,published:false,pready:false,done:false,opf:false,validated:s.base.C,mode:'fast',outcome:'',error:''}])),published:[],pready:[],ready:[],panel:{title:'Watch the state change',items:['Private execution → ordered publication → commit','P and C are derived from these events']}};
}
function applyEvent(st,f) {
 for (const op of f.ops) {
  const [type,id,arg,extra]=op;const tx=st.tx[id];
  if(type==='panel'){st.panel={title:id,items:arg};continue;}
  if(type==='lens'){st.lens={...st.lens,...id};continue;}
  if(type==='dep'){if(!st.tx[id]||!st.tx[arg])throw Error('Unknown dependency endpoint');continue;}
  if(!tx)throw Error('Unknown transaction '+id);
  if(type==='move'){tx.phase=arg;tx.note=extra;}
  else if(type==='sql'){
    if(tx.published||tx.done)throw Error('Business SQL after publication: '+id);
    if(arg==='physical'&&(!tx.opf||st.C<id-1||st.P<id-1))throw Error('Physical SQL before fallback selection or ordered predecessors: '+id);
    tx.mode=arg;tx.sqlRuns++;tx.queue=[...extra];tx.validated=st.C;tx.error='';
  }
  else if(type==='queue'){if(tx.published)throw Error('Frozen queue changed');tx.queue=[...arg];}
  else if(type==='validate'){if(arg>st.P||arg>id-1)throw Error('Validation beyond predecessor publication');tx.validated=arg;}
  else if(type==='fallback'){if(tx.published)throw Error('Fallback after publication');tx.opf=true;}
  else if(type==='abort'){if(tx.published)throw Error('Abort/resimulation after publication');tx.queue=[];tx.aborts++;}
  else if(type==='sqlerror'){if(tx.published||tx.done)throw Error('SQL error after publication');tx.queue=[];tx.error=arg;}
  else if(type==='pready'){
    // Error catch path: mark the slot publication-ready; P scans forward over contiguous ready slots.
    if(tx.published||id<=st.P)throw Error('Duplicate publication-ready mark: '+id);
    tx.pready=true;st.pready.push(id);while(st.pready.includes(st.P+1))st.P++;
  }
  else if(type==='publish'){
    if(id!==st.P+1)throw Error('Out-of-order publication: '+id);
    if(tx.mode!=='physical'&&tx.validated<id-1)throw Error('Unvalidated publication: '+id);
    tx.published=true;st.published.push(id);st.P=id;while(st.pready.includes(st.P+1))st.P++;
  }
  else if(type==='commit'||type==='terminal'){
    if(!(tx.published||tx.pready)||tx.done)throw Error('Invalid finalization: '+id);
    if(type==='commit'&&tx.error)throw Error('Commit after SQL error: '+id);
    if(type==='commit')Object.assign(st.db,arg);
    tx.done=true;tx.outcome=type==='commit'?'committed':'ERROR '+arg;st.ready.push(id);
    while(st.ready.includes(st.C+1))st.C++;
  } else throw Error('Unknown model operation '+type);
 }
 return st;
}
function modelAt(scene,time) {
 const st=initialState(scene);for(const f of scene.frames){if(f.t>time)break;applyEvent(st,f);}return st;
}
function validateModel() {
 for(const s of scenarios){let st=initialState(s);for(const f of s.frames)applyEvent(st,f);}
 return {scenes:scenarios.length,events:scenarios.reduce((n,s)=>n+s.frames.length,0)};
}
