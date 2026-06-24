# Redis 8.x-only cases split from gtid.tcl.
# Run explicitly:
#   ./runtest --single gtid/8_x/gtid --clients 1

proc restart_server_gtided {level wait_ready rotate_logs gtid_enabled {reconnect 1} {shutdown sigterm}} {
    set srv [lindex $::servers end+$level]
    if {$shutdown ne {sigterm}} {
        catch {[dict get $srv "client"] shutdown $shutdown}
    }
    kill_server $srv
    dict unset srv "client"

    set pid [dict get $srv "pid"]
    set stdout [dict get $srv "stdout"]
    set stderr [dict get $srv "stderr"]
    if {$rotate_logs} {
        set ts [clock format [clock seconds] -format %y%m%d%H%M%S]
        file rename $stdout $stdout.$ts.$pid
        file rename $stderr $stderr.$ts.$pid
    }
    set prev_ready_count [count_message_lines $stdout "Ready to accept"]

    if {[info exists ::cur_test]} {
        set fd [open $stdout "a+"]
        puts $fd "### Restarting server for test $::cur_test"
        close $fd
    }

    set config_file [dict get $srv "config_file"]
    set fileId [open $config_file "a"]
    puts $fileId [format "\ngtid-enabled %s" $gtid_enabled]
    flush $fileId
    close $fileId

    set pid [spawn_server $config_file $stdout $stderr]
    wait_server_started $config_file $stdout $pid

    dict set srv "pid" $pid
    lset ::servers end+$level $srv

    if {$wait_ready} {
        while 1 {
            if {[count_message_lines $stdout "Ready to accept"] > $prev_ready_count} {
                break
            }
            after 10
        }
    }
    if {$reconnect} {
        reconnect $level
        if {$::swap} {
            wait_done_loading [srv $level client]
        }
    }
}

start_server {tags {"gtid" "gtid_8x"} overrides {gtid-enabled yes}} {
    set master_repl [attach_to_replication_stream]
    set orig_db 0

    start_server {overrides {gtid-enabled yes}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        catch {$slave config set repl-rdb-channel no}
        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        set slave_repl [attach_to_replication_stream]

        $master SET key val0
        wait_for_gtid_sync $master $slave
        assert_equal [$master GET key] val0
        assert_equal [$slave GET key] val0

        set myuuid [status $master gtid_uuid]
        set mygno  [status $master gtid_executed_gno_count]

        assert_replication_stream $master_repl [list {select *} "gtid $myuuid:$mygno * SET key val0"]
        assert_replication_stream $slave_repl  [list {select *} "gtid $myuuid:$mygno * SET key val0"]

        test "propagte repl: GTID-ENABLED(yes) auto-wrapped alsoPropagate TX maps to one GTID" {
            set sadd_argv [list SADD spop_gtid]
            for {set i 1} {$i <= 3000} {incr i} {
                lappend sadd_argv $i
            }
            $master {*}$sadd_argv
            wait_for_gtid_sync $master $slave

            incr mygno
            assert_equal [$master SCARD spop_gtid] 3000
            assert_equal [$slave SCARD spop_gtid] 3000
            assert_replication_stream $master_repl [list "gtid $myuuid:$mygno * SADD spop_gtid *"]
            assert_replication_stream $slave_repl  [list "gtid $myuuid:$mygno * SADD spop_gtid *"]

            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SPOP spop_gtid 2500
            wait_for_gtid_sync $master $slave

            incr mygno
            set mygtidset "$myuuid:1-$mygno"

            assert_equal [$master SCARD spop_gtid] 500
            assert_equal [$slave SCARD spop_gtid] 500

            # 8.x batches SREM inside MULTI (~3 commands for 2500 members).
            set exec_pattern [format {gtid %s:%s * [Ee][Xx][Ee][Cc]} $myuuid $mygno]
            assert_replication_stream $master_repl [list multi {srem spop_gtid *} {srem spop_gtid *} {srem spop_gtid *} $exec_pattern]
            assert_replication_stream $slave_repl  [list multi {srem spop_gtid *} {srem spop_gtid *} {srem spop_gtid *} $exec_pattern]

            assert_match "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: expire" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SET key val6 PX 100
            # Wait for active expire to fire and write DEL to the replication
            # stream. Using wait_for_condition instead of a hard after-delay
            # avoids flakiness in SWAP mode where cold-key expiry may not
            # complete within a fixed 200 ms window.
            wait_for_condition 100 100 {
                [$master EXISTS key] == 0
            } else {
                fail "key val6 did not expire on master"
            }

            wait_for_gtid_sync $master $slave

            assert_equal [$master EXISTS key] 0
            assert_equal [$slave EXISTS key] 0

            # 8.x replicates PXAT absolute expiry (6.x uses PX; see gtid_6x.tcl).
            assert_replication_stream $master_repl [list "gtid $myuuid:[expr $mygno+1] * SET key val6 PXAT *" "gtid $myuuid:[expr $mygno+2] * DEL key"]
            assert_replication_stream $slave_repl [list "gtid $myuuid:[expr $mygno+1] * SET key val6 PXAT *" "gtid $myuuid:[expr $mygno+2] * DEL key"]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:[expr $mygno+1]-[expr $mygno+2]"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:[expr $mygno+1]-[expr $mygno+2]"

            incr mygno 2
            set mygtidset "$myuuid:1-$mygno"
            assert_match "*$mygtidset*" [status $master gtid_executed]
        }
    }
}

start_server {tags {"gtid" "gtid_8x"} overrides {gtid-enabled yes}} {
    test "multi-exec select db" {
        set repl [attach_to_replication_stream]
        r set k v
        r select 0
        r set k v

        if {$::swap} {
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v}
                {gtid * 0 set k v}
            }
        } else {
            assert_replication_stream $repl {
                {select *}
                {gtid * * set k v}
                {select *}
                {gtid * 0 set k v}
            }
        }
        r select $::target_db
    }

    test "multi-exec select db" {
        set repl [attach_to_replication_stream]
        r multi
        r set k v
        r select 0
        r set k v
        r exec
        r set k v1

        if {$::swap} {
            assert_replication_stream $repl {
                {multi}
                {select *}
                {set k v}
                {set k v}
                {gtid * * [Ee][Xx][Ee][Cc]}
                {gtid * 0 set k v1}
            }
        } else {
            assert_replication_stream $repl {
                {multi}
                {select *}
                {set k v}
                {select 0}
                {set k v}
                {gtid * * [Ee][Xx][Ee][Cc]}
                {gtid * 0 set k v1}
            }
        }
    }
}

start_server {tags {"repl" "gtid_8x"} overrides} {
    set master [srv 0 client]
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set gtid-enabled yes
    set repl [attach_to_replication_stream]
    start_server {tags {"slave"}} {
        set slave [srv 0 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        $master multi
        $master select 1
        $master select 2
        $master set k v
        $master select 3
        $master set k v1
        $master exec

        if {$::swap} {
            assert_replication_stream $repl {
                {multi}
                {select 2}
                {set k v}
                {select 3}
                {set k v1}
                {gtid * 0 [Ee][Xx][Ee][Cc]}
            }
        } else {
            assert_replication_stream $repl {
                {multi}
                {select 2}
                {set k v}
                {select 3}
                {set k v1}
                {gtid * 9 [Ee][Xx][Ee][Cc]}
            }
        }

        after 1000
        assert_equal [$slave get k] {}
        $slave select 2
        assert_equal [$slave get k] v
        $slave select 3
        assert_equal [$slave get k] v1
    }
}

start_server {tags {"repl" "gtid_8x"} overrides} {
    set master [srv 0 client]
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set gtid-enabled yes
    start_server {tags {"slave"}} {
        set slave [srv 0 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave

        test {GTID cross-DB transaction preserves body DB when stream already selected body DB} {
            # Seed DB0 first so the replication stream may otherwise omit SELECT 0
            # before the transaction body.
            $master select 0
            $master set gtid-cross-db-seed seed
            wait_for_gtid_sync $master $slave
            set repl [attach_to_replication_stream]

            $master select 9
            $master multi
            $master select 0
            $master set gtid-cross-db-key:1 value-in-db0
            $master set gtid-cross-db-key:2 value-in-db0
            $master exec

            assert_replication_stream $repl {
                {multi}
                {select 0}
                {set gtid-cross-db-key:1 value-in-db0}
                {set gtid-cross-db-key:2 value-in-db0}
                {gtid * 9 [Ee][Xx][Ee][Cc]}
            }

            wait_for_gtid_sync $master $slave

            $master select 0
            $slave select 0
            assert_equal value-in-db0 [$master get gtid-cross-db-key:1]
            assert_equal value-in-db0 [$slave get gtid-cross-db-key:1]
            assert_equal value-in-db0 [$master get gtid-cross-db-key:2]
            assert_equal value-in-db0 [$slave get gtid-cross-db-key:2]

            $master select 9
            $slave select 9
            assert_equal {} [$master get gtid-cross-db-key:1]
            assert_equal {} [$slave get gtid-cross-db-key:1]
            assert_equal {} [$master get gtid-cross-db-key:2]
            assert_equal {} [$slave get gtid-cross-db-key:2]
        }
    }
}

# ---------------------------------------------------------------------------
# PSYNC2 hash-field-expiry restart test — Redis 8.x / non-swap only.
# Uses hpexpire and hash-max-listpack-entries which do not exist in Redis 6.x.
# The swap-compatible variant lives in psync2-master-restart.tcl.
# ---------------------------------------------------------------------------

start_server {tags {"psync2" "gtid_8x" "external:skip"} overrides {gtid-enabled yes}} {
start_server {overrides {gtid-enabled yes}} {
start_server {overrides {gtid-enabled yes}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set replica [srv -1 client]
    set replica_host [srv -1 host]
    set replica_port [srv -1 port]
    set sub_replica [srv -2 client]

    $master config set save "3600 1"
    $master config set repl-timeout 3600
    $replica config set repl-timeout 3600
    $sub_replica config set repl-timeout 3600
    $master config set repl-ping-replica-period 3600
    $master config rewrite

    $replica replicaof $master_host $master_port
    $sub_replica replicaof $replica_host $replica_port

    wait_for_condition 50 100 {
        [status $replica master_link_status] eq {up} &&
        [status $sub_replica master_link_status] eq {up}
    } else {
        fail "Replication not started."
    }

    createComplexDataset $master 1000

    tags {memonly} {
    test "PSYNC2: Full resync after Master restart - hash-field expiry (8.x / non-swap)" {
        $master config set repl-backlog-size 16384
        $master config rewrite

        $master debug set-active-expire 0
        for {set j 0} {$j < 2048} {incr j} {
            $master select [expr $j%16]
            $master set $j somevalue px 10
        }

        # hash-field-expiration: OBJ_ENCODING_LISTPACK_EX survives RDB load even
        # when expired; RDB_TYPE_HASH_METADATA is discarded on load.
        $master hset myhash1 f1 v1 f2 v2 f3 v3
        $master hpexpire myhash1 10 FIELDS 3 f1 f2 f3
        $master config set hash-max-listpack-entries 0
        $master hset myhash2 f1 v1 f2 v2
        $master hpexpire myhash2 10 FIELDS 2 f1 f2
        $master config set hash-max-listpack-entries 1

        after 20

        wait_for_condition 500 100 {
            [status $master master_repl_offset] == [status $replica master_repl_offset] &&
            [status $master master_repl_offset] == [status $sub_replica master_repl_offset]
        } else {
            fail "Replicas and master offsets were unable to match *exactly*."
        }

        $replica config resetstat

        catch {
            restart_server_gtided 0 true false "yes"
            set master [srv 0 client]
        }

        wait_for_condition 50 1000 {
            [status $replica master_link_status] eq {up} &&
            [status $sub_replica master_link_status] eq {up}
        } else {
            fail "Replicas didn't sync after master restart"
        }

        # Backlog overflows because non-swap expire DELs are written on RDB load.
        assert {[status $master repl_backlog_first_byte_offset] > [status $master second_repl_offset]}
        assert {[status $master sync_partial_ok] == 1}
        assert {[status $master sync_full] == 0}
        assert {[status $master rdb_last_load_keys_expired] == 2048}
        assert {[status $replica sync_partial_ok] == 1}
    }
    }
}}}
