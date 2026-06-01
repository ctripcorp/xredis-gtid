# Regression test: explicit GTID command wrapping a module command that calls
# RedisModule_Replicate() must NOT crash the server.
#
# Root cause (fixed in xredis_gtid.c::gtidCommand):
#   When gtidCommand wraps a module command, two paths both write to
#   server.also_propagate:
#     (1) The module's RedisModule_Replicate() calls → also_propagate[0..N]
#     (2) call()'s alsoPropagate() for the outer gtidCommand (CMD_MODULE=0) → also_propagate[N+1]
#   numops > 1 triggers auto-MULTI/EXEC in propagatePendingCommands().
#   Both the GTID body command and the EXEC GTID use gtid_offset_at_multi=M+1,
#   causing assert(tail_offset < offset) in gtidSeqAppend to fire → SIGABRT.
#
# Fix: after the inner command executes, if it is a MODULE command, discard its
#   also_propagate entries (the outer "GTID uuid:N 0 <cmd>" is the canonical
#   propagation form; the module's entries are redundant since the slave
#   suppresses its own RedisModule_Replicate via shouldPropagate()==false).
#
# This test uses the built-in propagate.so test module.  The command
# "propagate-test.simple" calls RedisModule_Replicate() twice (INCR counter-1,
# INCR counter-2) without calling preventCommandPropagation(), which is the
# minimal non-RediSearch reproducer for the same class of crash.
#
# Why a connected replica is required:
#   server.gtid_seq is only non-NULL when the replication backlog exists,
#   which is created when the first slave connects.  Without gtid_seq,
#   gtidSeqAppend is skipped and the crash does not occur.

set testmodule [file normalize tests/modules/propagate.so]
if {![file exists $testmodule]} {
    set _modules_dir [file normalize tests/modules]
    if {[catch {exec make -C $_modules_dir propagate.so 2>@1} _out]} {
        error "propagate.so not found at $testmodule and auto-build failed: $_out"
    }
    unset _modules_dir _out
}

start_server [list tags {"gtid" "modules"} \
    overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
    start_server [list overrides [list gtid-enabled yes loadmodule "$testmodule"]] {
        set master [srv -1 client]
        set mhost  [srv -1 host]
        set mport  [srv -1 port]
        set slave  [srv  0 client]

        $slave  slaveof $mhost $mport
        wait_for_sync $slave

        # Ensure both clients are on DB 0 (test framework may default to DB 9
        # in non-singledb mode; replication lands on the correct DB but clients
        # must select the same DB to read back the keys).
        $master select 0
        $slave  select 0

        # Seed write — ensures the replication backlog and gtid_seq are created.
        $master set gtid-module-crash-seed seed
        wait_for_gtid_sync $master $slave

        # ── Test 1: crash reproducer ──────────────────────────────────────────
        # "propagate-test.simple" calls RedisModule_Replicate() twice without
        # preventCommandPropagation().  Before the fix this crashes the master.
        test {GTID explicit command wrapping module command with RedisModule_Replicate does not crash} {
            # Use a foreign UUID so the GNO never conflicts with the master's own sequence.
            $master GTID "modtest:1" 0 propagate-test.simple

            # Reaching this line means the master survived.
            wait_for_gtid_sync $master $slave

            # The foreign GTID must appear in both nodes' executed sets.
            assert_match "*modtest:1*" [status $master gtid_set]
            assert_match "*modtest:1*" [status $slave  gtid_set]
        }

        # ── Test 2: GTID seq consistency after fix ────────────────────────────
        # Follow-up normal writes must still produce correct GTID and converge.
        test {GTID module crash fix: subsequent writes produce correct GTID and converge} {
            set before [status $master gtid_executed_gno_count]

            $master set after-fix-key value
            wait_for_gtid_sync $master $slave

            assert_equal [expr {$before + 1}] [status $master gtid_executed_gno_count]
            assert_equal value [$slave get after-fix-key]
        }

        # ── Test 3: multiple explicit GTIDs from same foreign UUID ────────────
        test {GTID module crash fix: multiple explicit module GTIDs accumulate correctly} {
            $master GTID "modtest:2" 0 propagate-test.simple
            $master GTID "modtest:3" 0 propagate-test.simple
            wait_for_gtid_sync $master $slave

            # modtest:1 from test 1, modtest:2 and modtest:3 from this test.
            # The GTID representation collapses contiguous ranges: modtest:1-3.
            assert_match "*modtest:1-3*" [status $master gtid_set]
            assert_match "*modtest:1-3*" [status $slave  gtid_set]
        }
    }
}
