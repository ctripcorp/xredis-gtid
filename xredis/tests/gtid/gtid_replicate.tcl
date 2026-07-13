# Tests for GTID-wrapped commands in master-slave setup.
#
# Cases:
#   1. GTID + module cmd, multiple RM_Replicate (numops>1 → MULTI/EXEC)
#   2. GTID + read-only command                  → rejected
#   3. GTID + nondeterministic write command      → rejected (e.g. SPOP)
#   4. GTID + deterministic write command         → normal propagation
#   5. GTID idempotency                           → same gno re-executed is skipped
#
# Note: the GTID command's third argument is dbid. All module commands below
# use dbid=0 to ensure consistent key lookup on the slave side.

set testmodule [file normalize tests/modules/propagate.so]
set gtid_module_repl_overrides [list gtid-enabled yes loadmodule "$testmodule"]

tags {"modules" "gtid"} {
    start_server [list overrides $gtid_module_repl_overrides] {
        set slave  [srv 0 client]

        start_server [list overrides $gtid_module_repl_overrides] {
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
            # Case 2: GTID + read-only command → rejected with error
            # ------------------------------------------------------------------
            test {GTID + read-only command is rejected} {
                set before_m [status $master gtid_set]
                set before_s [status $slave  gtid_set]

                catch {$master GTID "rdtest:1" 0 get seed-key} err
                assert_match "*readonly*" $err
                after 200

                assert_equal $before_m [status $master gtid_set]
                assert_equal $before_s [status $slave gtid_set]
                assert_equal PONG [$master ping]
                assert_equal PONG [$slave  ping]
            }

            # ------------------------------------------------------------------
            # Case 3: GTID + nondeterministic write command → rejected
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
                assert_match "*nondeterminism in gtid command*" $err
                after 200

                assert_equal $before_m [lsort [$master smembers myset]]
                assert_equal $before_s [lsort [$slave  smembers myset]]
                assert_equal $before_gtid_m [status $master gtid_set]
                assert_equal $before_gtid_s [status $slave  gtid_set]
                assert_equal PONG [$master ping]
                assert_equal PONG [$slave  ping]
            }

            # ------------------------------------------------------------------
            # Case 4: GTID + deterministic write command → normal propagation
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
            # Case 5: GTID idempotency — re-executing same gno is silently skipped
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
}
