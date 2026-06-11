

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-001: slave reconnects same master triggers xcontinue" {
            $S replicaof $M_host $M_port
            wait_for_sync $S

            $M set m_key1 m_val1
            $M set m_key2 m_val2
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            $S set s_key1 s_val1
            $S set s_key2 s_val2

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 2


            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 2]

            set result_str [join $result " "]
            assert_match "*s_key1*" $result_str
            assert_match "*s_key2*" $result_str

            assert_equal [$S get m_key1] m_val1
            assert_equal [$S get m_key2] m_val2

            assert_equal [$S get s_key1] s_val1
            assert_equal [$S get s_key2] s_val2
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
            set MA [srv -2 client]
            set MA_host [srv -2 host]
            set MA_port [srv -2 port]
            set MB [srv -1 client]
            set MB_host [srv -1 host]
            set MB_port [srv -1 port]
            set S [srv 0 client]

            test "GAPLOG-XSYNC-002: master's GTID in gaplog after slave switch" {

                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                $MA set ma_key1 ma_val1
                $MA set ma_key2 ma_val2
                $MA set ma_key3 ma_val3
                wait_for_ofs_sync $S $MA

                $S replicaof no one
                after 100

                $MA set ma_key4 ma_val4
                $MA set ma_key5 ma_val5

                set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                wait_for_condition 50 100 {
                    [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
                } else {
                    fail "xsync xcontinue not detected"
                }

                set gaplog_len [get_gaplog_entries $S]
                assert_equal $gaplog_len 0

                assert_equal [$S get ma_key1] ma_val1
                assert_equal [$S get ma_key4] ma_val4
                assert_equal [$S get ma_key5] ma_val5
            }
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-003: slave independent MULTI/EXEC transaction" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            
            $S replicaof no one
            after 100

            $S select 1
            $S MULTI
            $S set s_multi_key1 s_multi_val1
            $S select 2
            $S set s_multi_key2 s_multi_val2
            $S select 3
            $S set s_multi_key3 s_multi_val3
            $S EXEC

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1


            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            set result_str [join $result " "]

            assert_match "*s_multi_key1*" $result_str
            assert_match "*s_multi_key2*" $result_str
            assert_match "*s_multi_key3*" $result_str

            $S select $::target_db
            assert_equal [$S get m_key] m_val
            $S select 1
            assert_equal [$S get s_multi_key1] s_multi_val1
            $S select 2
            assert_equal [$S get s_multi_key2] s_multi_val2
            $S select 3
            assert_equal [$S get s_multi_key3] s_multi_val3
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-004: slave independent Lua script multi-key" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            set lua_script {
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("SET", KEYS[2], ARGV[2])
                redis.call("SET", KEYS[3], ARGV[3])
                return "OK"
            }
            $S EVAL $lua_script 3 s_lua_key1 s_lua_key2 s_lua_key3 s_lua_val1 s_lua_val2 s_lua_val3

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            set result_str [join $result " "]
            assert_match "*s_lua_key1*" $result_str
            assert_match "*s_lua_key2*" $result_str
            assert_match "*s_lua_key3*" $result_str

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_lua_key1] s_lua_val1
            assert_equal [$S get s_lua_key2] s_lua_val2
            assert_equal [$S get s_lua_key3] s_lua_val3
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    test "GAPLOG-XSYNC-005: config and basic commands" {
        set client [srv 0 client]

        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled yes} $enabled

        $client CONFIG SET gtid-gaplog-enabled no
        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled no} $enabled

        $client CONFIG SET gtid-gaplog-enabled yes

        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 10000} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 100
        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 100} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 10000

        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0

        $client GTIDX GAPLOG CLEAR
        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-006: edge case - empty data sync" {

            $S replicaof $M_host $M_port
            wait_for_sync $S

            $S replicaof no one
            after 100

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 0
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-007: edge case - many independent writes" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            set num_writes 10
            for {set i 1} {$i <= $num_writes} {incr i} {
                $S set s_key_$i s_val_$i
            }

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len $num_writes

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $num_writes]
            set result_str [join $result " "]
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_match "*s_key_$i*" $result_str
            }

            assert_equal [$S get m_key] m_val
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_equal [$S get s_key_$i] s_val_$i
            }
        }
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-008: edge case - different data types" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # String
            $S set s_str_key s_str_val
            # Hash
            $S hset s_hash_key field1 val1 field2 val2
            # List
            $S lpush s_list_key elem1 elem2 elem3
            # Set
            $S sadd s_set_key member1 member2
            # Sorted Set
            $S zadd s_zset_key 1 member1 2 member2

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 5

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 5]
            set result_str [join $result " "]
            assert_match "*s_str_key*" $result_str
            assert_match "*s_hash_key*" $result_str
            assert_match "*s_list_key*" $result_str
            assert_match "*s_set_key*" $result_str
            assert_match "*s_zset_key*" $result_str

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_str_key] s_str_val
            assert_equal [$S hget s_hash_key field1] val1
            assert_equal [$S lrange s_list_key 0 -1] {elem3 elem2 elem1}
            assert_equal [lsort [$S smembers s_set_key]] {member1 member2}
            assert_equal [$S zrange s_zset_key 0 -1] {member1 member2}
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-009: edge case - SELECT command" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            $M set m_key m_val
            wait_for_ofs_sync $S $M

            assert_equal [$S get m_key] m_val

            $S replicaof no one
            after 100

            $S set s_db0_key s_db0_val

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            set result_str [join $result " "]
            assert_match "*s_db0_key*" $result_str

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_db0_key] s_db0_val
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-010: edge case - DEL command" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key1 m_val1
            $M set m_key2 m_val2
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            $S set s_key s_val
            $S del m_key1

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 2

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 2]
            set result_str [join $result " "]
            assert_match "*s_key*" $result_str
            assert_match "*m_key1*" $result_str

            assert_equal [$S get m_key2] m_val2
            assert_equal [$S get s_key] s_val
        }
    }
}





proc write_n_keys {client prefix start count} {
    for {set i $start} {$i < $start + $count} {incr i} {
        $client set ${prefix}_key${i} value${i}
    }
}


proc assert_gaplog_contains {client uuid gno} {
    set result [$client GTIDX GAPLOG LIST 0 1000]
    foreach entry $result {
        set entry_uuid [lindex $entry 0]
        set entry_gno [lindex $entry 1]
        if {$entry_uuid eq $uuid && $entry_gno == $gno} {
            return 1
        }
    }
    fail "Gaplog does not contain $uuid:$gno"
}





start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    # A (master)
    set A [srv 0 client]
    set A_host [srv 0 host]
    set A_port [srv 0 port]

    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        # B (slave of A)
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
            # C (slave of B)
            set C [srv 0 client]

            test "Complex-Scenario-3: Chain replication topology switch" {
                puts "DEBUG: Setup chain replication A -> B -> C"

 
                $B replicaof $A_host $A_port
                wait_for_sync $B

                $C replicaof $B_host $B_port
                wait_for_sync $C


                puts "DEBUG: Write data to A"
                $A set a_key1 a_val1
                $A set a_key2 a_val2
                wait_for_ofs_sync $C $A

                assert_equal [$C get a_key1] a_val1
                assert_equal [$C get a_key2] a_val2


                puts "DEBUG: B becomes independent (split-brain)"
                $B replicaof no one
                after 100

                $B set b_key1 b_val1
                $B set b_key2 b_val2

                set b_uuid [get_uuid $B]
                puts "DEBUG: B UUID: $b_uuid"


                puts "DEBUG: B reconnects to A"
                $B replicaof $A_host $A_port
                wait_for_sync $B
                after 200


                set b_gaplog_len [$B GTIDX GAPLOG LEN]
                puts "DEBUG: B gaplog length after reconnect: $b_gaplog_len"
                assert {$b_gaplog_len >= 2}


                puts "DEBUG: C becomes independent"
                $C replicaof no one
                after 100

                $C set c_key1 c_val1
                $C set c_key2 c_val2

                set c_uuid [get_uuid $C]
                puts "DEBUG: C UUID: $c_uuid"


                puts "DEBUG: C switches to B (should have no gap)"
                $C replicaof $B_host $B_port
                wait_for_sync $C
                after 200

                puts "DEBUG: C switches to A (skip-level mount, should have gap)"
                $C replicaof $A_host $A_port
                wait_for_sync $C
                after 200


                set c_gaplog_len [$C GTIDX GAPLOG LEN]
                puts "DEBUG: C gaplog length after skip-level switch: $c_gaplog_len"


                assert {$c_gaplog_len >= 2}
            }
        }
    }
}


proc run_extreme_test {maxgap backlog data_multiplier} {

    set data_count [expr {$maxgap * $data_multiplier}]

    puts "DEBUG: Running extreme test with maxgap=$maxgap, backlog=$backlog, data_count=$data_count"

    start_server [subst -nocommands -nobackslashes {
        tags {"gaplog"} overrides {
            gtid-enabled yes
            gtid-gaplog-enabled yes
            gtid-xsync-max-gap $maxgap
        }
    }] {
        set M [srv 0 client]
        set M_host [srv 0 host]
        set M_port [srv 0 port]

        start_server [subst -nocommands -nobackslashes {
            overrides {
                gtid-enabled yes
                gtid-gaplog-enabled yes
                gtid-xsync-max-gap $maxgap
            }
        }] {
            set S [srv 0 client]

            test "Extreme-test (maxgap=$maxgap): Large capacity gaplog" {
                puts "DEBUG: Writing $maxgap baseline keys to master"
                write_n_keys $M "bulk" 1 $maxgap

                $S replicaof $M_host $M_port
                wait_for_sync $S

                $S replicaof no one
                after 200

                puts "DEBUG: Writing 1 gap key to master (slave disconnected)"
                $M set gap_key gap_val

                set slave_write_count $maxgap
                puts "DEBUG: Writing $slave_write_count keys to slave independently"
                write_n_keys $S "slave" 1 $slave_write_count

                puts "DEBUG: Reconnecting slave to master"
                $S replicaof $M_host $M_port
                wait_for_sync $S
                after 1000

                set gaplog_len [$S GTIDX GAPLOG LEN]
                puts "DEBUG: Gaplog length after reconnect: $gaplog_len"

                assert {$gaplog_len > 0}
                assert {$gaplog_len <= $maxgap}
                puts "DEBUG: Verified gaplog has $gaplog_len entries"
            }

            test "Extreme-test (maxgap=$maxgap): Rapid switch stress test" {
                $S GTIDX GAPLOG CLEAR

                puts "DEBUG: Starting 20 rapid switches with gap creation"

                set total_rapid_switches 20
                for {set i 1} {$i <= $total_rapid_switches} {incr i} {

                    $S replicaof no one
                    after 20


                    $M set m_rapid_key${i} m_val${i}

                    $S set s_rapid_key${i} s_val${i}

                    $S replicaof $M_host $M_port
                    wait_for_sync $S
                    after 20
                }

                set gaplog_len [$S GTIDX GAPLOG LEN]
                puts "DEBUG: Gaplog length after $total_rapid_switches rapid switches: $gaplog_len"

                assert {$gaplog_len > 0}
                puts "DEBUG: Verified gaplog has $gaplog_len entries after rapid switches"
            }
        }
    }
}

foreach {maxgap backlog data_multiplier} {
    10000  100mb  2
    10     500mb  2
    50000  1mb   2
} {
    run_extreme_test $maxgap $backlog $data_multiplier
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CFG-001: disabled" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S CONFIG SET gtid-gaplog-enabled no
            $S set off1 v1; $S set off2 v2
            replicaof_xcontinue $S $Mh $Mp
            assert_equal [gaploglen $S] 0; assert_equal [$S get off1] v1
            $S CONFIG SET gtid-gaplog-enabled yes
        }
    }
}
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CFG-002: toggle" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S set on1 v1; $S set on2 v2
            replicaof_xcontinue $S $Mh $Mp; set l1 [gaploglen $S]; assert {$l1 >= 2}
            $S replicaof no one; after 100; $S CONFIG SET gtid-gaplog-enabled no
            $S set off1 v3; $S set off2 v4
            replicaof_xcontinue $S $Mh $Mp; assert_equal [gaploglen $S] $l1
            $S replicaof no one; after 100; $S CONFIG SET gtid-gaplog-enabled yes
            $S set bk1 v5; replicaof_xcontinue $S $Mh $Mp
            assert {[gaploglen $S] > $l1}
        }
    }
}