# Tests for GTID-wrapped commands in master-slave setup.
#
# Cases:
#   1. GTID + module cmd, multiple RM_Replicate (numops>1 → MULTI/EXEC)
#   2. GTID + module cmd, single  RM_Replicate  (numops=1 → no MULTI/EXEC)
#   3. GTID + module cmd, no RM_Replicate       (numops=0 → nothing propagated to slave)
#   4. GTID + read-only command                  → rejected
#   5. GTID + nondeterministic write command      → rejected (e.g. SPOP)
#   6. GTID + deterministic write command         → normal propagation
#   7. GTID idempotency                           → same gno re-executed is skipped
#
# Note: the GTID command's third argument is dbid. All module commands below
# use dbid=0 to ensure consistent key lookup on the slave side.

source tests/support/aofmanifest.tcl

set testmodule [file normalize tests/modules/propagate.so]
set gtid_repl_aof_server_path [tmpdir gtid.module.replicate.aof]

proc gtidModuleReplicateReadAofCommands {aof_path} {
    set fp [open $aof_path r]
    fconfigure $fp -translation binary
    fconfigure $fp -blocking 1

    set commands {}
    while {1} {
        set cmd [read_from_aof $fp]
        if {$cmd eq ""} break
        lappend commands $cmd
    }

    close $fp
    return $commands
}

tags {"modules" "gtid"} {
    start_server [list overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
        set slave  [srv 0 client]

        start_server [list overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            $slave replicaof $master_host $master_port
            # Keep keyspace notifications enabled to verify has_effect mechanism
            # properly handles the dirty flag increments from keyspace notification callbacks
            wait_for_sync $slave

            # Seed one write to create replication backlog (needed for gtid_seq).
            # Use db 0 explicitly so all subsequent checks are in the same db.
            $master select 0
            $slave select 0
            $master set seed-key seed-value
            wait_for_condition 50 100 {
                [$slave get seed-key] eq "seed-value"
            } else {
                fail "seed-key did not replicate"
            }

            # ------------------------------------------------------------------
            # Case 1: GTID + module cmd, multiple RM_Replicate → MULTI/EXEC wrap
            # propagate-test.simple calls RM_Replicate twice (INCR counter-1/2).
            # dbid=0 in the GTID command ensures keys land in db 0.
            # ------------------------------------------------------------------
            test {GTID + module cmd: multiple RM_Replicate, MULTI/EXEC, data correct on slave} {
                assert_equal OK [$master GTID "modtest:1" 0 propagate-test.simple]

                wait_for_gtid_sync $master $slave

                # slave must have counter-1 and counter-2 in db 0
                $slave select 0
                assert_equal 1 [$slave get counter-1]
                assert_equal 1 [$slave get counter-2]
                # slave recorded the caller-supplied GTID (not auto-allocated server.uuid:N)
                assert_match "*modtest:1*" [status $slave gtid_set]
                assert {[gtid_set_is_equal \
                    [status $master gtid_set] [status $slave gtid_set]]}
                assert_equal PONG [$master ping]
            }

            # ------------------------------------------------------------------
            # Case 2: GTID + module cmd, single RM_Replicate → no MULTI/EXEC
            # propagate-test.single calls RM_Replicate once (INCR single-counter).
            # ------------------------------------------------------------------
            test {GTID + module cmd: single RM_Replicate, no MULTI/EXEC, data correct on slave} {
                assert_equal OK [$master GTID "modtest:2" 0 propagate-test.single]

                wait_for_gtid_sync $master $slave

                $slave select 0
                assert_equal 1 [$slave get single-counter]
                assert_match "*modtest:1-2*" [status $slave gtid_set]
                assert {[gtid_set_is_equal \
                    [status $master gtid_set] [status $slave gtid_set]]}
                assert_equal PONG [$master ping]
            }

            # ------------------------------------------------------------------
            # Case 3: GTID + module cmd, no RM_Replicate → numops=0
            # propagate-test.noreplicate does not call RM_Replicate.
            # No replicable effect → gno not consumed on either side
            # ------------------------------------------------------------------
            test {GTID + module cmd: no RM_Replicate, gno not consumed, both sides in sync} {
                set gno_before_m [status $master gtid_executed_gno_count]
                set gno_before_s [status $slave  gtid_executed_gno_count]

                assert_equal OK [$master GTID "modtest:3" 0 propagate-test.noreplicate]
                after 300

                # Module cmd without RM_Replicate: gno not consumed
                assert_equal $gno_before_m \
                    [status $master gtid_executed_gno_count]
                assert_equal $gno_before_s \
                    [status $slave  gtid_executed_gno_count]
                assert_equal PONG [$master ping]
                assert_equal PONG [$slave  ping]
            }

            # ------------------------------------------------------------------
            # Case 4: GTID + read-only command → rejected with error
            # ------------------------------------------------------------------
            test {GTID + read-only command is rejected} {
                set before_m [status $master gtid_set]
                set before_s [status $slave  gtid_set]

                catch {$master GTID "rdtest:1" 0 get seed-key} err
                assert_match "*readonly*" $err
                after 200

                assert_equal $before_m [status $master gtid_set]
                assert_equal $before_s [status $slave  gtid_set]
                assert_equal PONG [$master ping]
                assert_equal PONG [$slave  ping]
            }

            # ------------------------------------------------------------------
            # Case 5: GTID + nondeterministic write command → rejected
            # ------------------------------------------------------------------
            test {GTID + nondeterministic write (spop) is rejected, set unmodified} {
                $master select 0
                $master sadd myset a b c d e
                wait_for_condition 50 100 {[$slave scard myset] == 5} else {
                    fail "myset did not replicate to slave"
                }

                set before_m [lsort [$master smembers myset]]
                set before_s [lsort [$slave  smembers myset]]
                set before_gtid_m [status $master gtid_set]
                set before_gtid_s [status $slave  gtid_set]

                catch {$master GTID "ndtest:1" 0 spop myset} err
                assert_match "*not permitted*nondeterministic*" $err
                after 200

                assert_equal $before_m [lsort [$master smembers myset]]
                assert_equal $before_s [lsort [$slave  smembers myset]]
                assert_equal $before_gtid_m [status $master gtid_set]
                assert_equal $before_gtid_s [status $slave  gtid_set]
                assert_equal PONG [$master ping]
                assert_equal PONG [$slave  ping]
            }

            # ------------------------------------------------------------------
            # Case 6: GTID + deterministic write command → normal propagation
            # ------------------------------------------------------------------
            test {GTID + deterministic write (set) propagates correctly} {
                assert_equal OK [$master GTID "dettest:1" 0 set det-key det-value]

                wait_for_gtid_sync $master $slave

                $slave select 0
                assert_equal "det-value" [$slave get det-key]
                assert_match "*dettest:1*" [status $slave gtid_set]
                assert {[gtid_set_is_equal \
                    [status $master gtid_set] [status $slave gtid_set]]}
                assert_equal PONG [$master ping]
            }

            # ------------------------------------------------------------------
            # Case 7: GTID idempotency — re-executing same gno is silently skipped
            # ------------------------------------------------------------------
            test {GTID idempotency: re-executing same gno is silently skipped} {
                $slave select 0
                set before_c1 [$slave get counter-1]
                set r [$master GTID "modtest:1" 0 propagate-test.simple]
                assert_match "*already executed*" $r
                after 200
                assert_equal $before_c1 [$slave get counter-1]
                assert_equal PONG [$master ping]
            }
        }
    }

    start_server [list overrides [list \
        dir $gtid_repl_aof_server_path \
        appendonly yes \
        appendfilename appendonly.aof \
        appenddirname appendonlydir \
        auto-aof-rewrite-percentage 0 \
        aof-load-truncated yes \
        save "" \
        gtid-enabled yes] keep_persistence true] {
        test {prepare AOF for GTID stale embedded identity regression} {
            set raw [redis [srv host] [srv port] 0 $::tls]
            assert_equal OK [$raw gtid "repro:1" 0 set leak-seed seed]
            $raw close
        }
    }

    start_server [list overrides [list \
        dir $gtid_repl_aof_server_path \
        appendonly yes \
        appendfilename appendonly.aof \
        appenddirname appendonlydir \
        auto-aof-rewrite-percentage 0 \
        aof-load-truncated yes \
        save "" \
        gtid-enabled yes] keep_persistence true] {
        test {GTID AOF reload does not leak stale embedded identity to first plain write} {
            # Use a fresh raw client so the first command after restart is the plain
            # write under test instead of the harness's auto-SELECT.
            set raw [redis [srv host] [srv port] 0 $::tls]
            assert_equal OK [$raw set leak-fresh value]

            set aof [get_last_incr_aof_path $raw]
            set commands [gtidModuleReplicateReadAofCommands $aof]
            $raw close

            assert_equal 4 [llength $commands]
            assert_equal {select 0} [lindex $commands 0]
            assert_equal {gtid repro:1 0 set leak-seed seed} [lindex $commands 1]
            assert_equal {select 0} [lindex $commands 2]

            set replay [lindex $commands 3]
            assert_equal gtid [lindex $replay 0]
            assert_no_match "repro:1" [lindex $replay 1]
            assert_equal 0 [lindex $replay 2]
            assert_equal set [lindex $replay 3]
            assert_equal leak-fresh [lindex $replay 4]
            assert_equal value [lindex $replay 5]
        }
    }
}
