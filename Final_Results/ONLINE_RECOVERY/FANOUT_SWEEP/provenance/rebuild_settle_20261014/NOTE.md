# Mid-sweep rebuild: settle unconfirmed mismatches before stopping

Trigger: matrix_n4000000_f128_M_mixed_r2_a1 REJECTED. Fault injected on node4; at L=416 all three
digests differed (node1 3057.., node2 40b2.., node4 4675..: a transient bad digest on a healthy node
plus the real fault), so no strict majority; at L=565 node4 was flagged 'unconfirmed'; the workload
ended (L=627) before a confirming round, so the corruption was never repaired and the final check failed.

Change: gateway_recovery_manager.hxx compare_loop now settles pending suspects (confirm+queue repair or
clear) on the quiescent replicas before stop_passive_and_drain() drains recoveries and runs the final
check. Only affects the moment comparisons stop with a suspect pending; accepted runs 1-14 had no
pending suspect at stop (A: recovery off; M_mixed: confirmed during the run).
