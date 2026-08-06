


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-001: GAPLOG LEN - empty gaplog returns 0" {
        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }

    test "GAPLOG-CMD-002: GAPLOG CLEAR - clears all entries" {
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"

        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-009: GAPLOG LEN - wrong args count error" {
        catch {r GTIDX GAPLOG LEN "extra-arg"} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-CMD-012: GAPLOG invalid subcommand error" {
        catch {r GTIDX GAPLOG INVALID} err
        assert_match "*subcommand*" $err
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-013: GTIDX HELP contains GAPLOG commands" {
        set help [r GTIDX HELP]
        assert_match "*GAPLOG LEN*" $help
        assert_match "*GAPLOG LIST*" $help
        assert_match "*GAPLOG ALL*" $help
        assert_match "*GAPLOG CLEAR*" $help
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-014: INFO GTID contains gaplog stats" {
        r GTIDX GAPLOG CLEAR
        set info [r INFO gtid]
        assert_match "*gtid_gaplog_entries:0*" $info
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-LIST-001: LIST on empty gaplog returns empty array" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG LIST 0 10]
        assert_equal $result {}
    }

    test "GAPLOG-LIST-002: LIST with negative start index returns error" {
        catch {r GTIDX GAPLOG LIST -1 10} err
        assert_match "*start must*" $err
    }

    test "GAPLOG-LIST-003: LIST with zero count returns error" {
        catch {r GTIDX GAPLOG LIST 0 0} err
        assert_match "*count must*" $err
    }

    test "GAPLOG-LIST-005: LIST with wrong args count returns error" {
        catch {r GTIDX GAPLOG LIST 0} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-LIST-009: LIST rejects non-integer indexes" {
        catch {r GTIDX GAPLOG LIST invalid 1} start_err
        assert_match "*integer*" $start_err
        catch {r GTIDX GAPLOG LIST 0 invalid} count_err
        assert_match "*integer*" $count_err
    }

    test "GAPLOG-LIST-010: LIST accepts maximum page size" {
        assert_equal [r GTIDX GAPLOG LIST 0 100] {}
    }

    test "GAPLOG-LIST-011: LIST rejects extra arguments" {
        catch {r GTIDX GAPLOG LIST 0 1 extra} err
        assert_match "*wrong*" $err
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]


        test "MULTI/EXEC transaction should record to gaplog" {
            $S replicaof $M_host $M_port
            wait_for_sync $S

            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            $S MULTI
            $S set s_key1 s_val1
            $S set s_key2 s_val2
            $S hset s_hash field1 val1 field2 val2
            $S EXEC

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 500

            set info [$S INFO gtid]
            puts "\nSlave GTID Info:"
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_*" $line]} {
                    puts $line
                }
            }

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_key1] s_val1
            assert_equal [$S get s_key2] s_val2
            assert_equal [$S hget s_hash field1] val1
            assert_equal [$S hget s_hash field2] val2
        }
       
   
        test "GAPLOG LIST command - list entries in global order" {
            set info [$S INFO gtid]
            set uuid ""
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_uuid:*" $line]} {
                    set uuid [string range $line 10 end]
                    break
                }
            }
            assert {$uuid != ""}

            set page_count [$S GTIDX GAPLOG LEN]
            assert {$page_count > 0}

            set list_result [$S GTIDX GAPLOG LIST 0 $page_count]
            assert {[llength $list_result] > 0}
            assert_equal [llength $list_result] 1
            set first_entry [lindex $list_result 0]
            assert_equal [llength $first_entry] 2
            assert_equal [lindex $first_entry 0] $uuid
            # inner_array: [gno, keys, gno, keys, ...], len = 2 * entry_count
            set inner [lindex $first_entry 1]
            assert_equal [llength $inner] [expr {$page_count * 2}]
            for {set j 0} {$j < [llength $inner]} {incr j 2} {
                set prev_gno [expr {$j == 0 ? -1 : [lindex $inner [expr {$j - 2}]]}]
                assert {[lindex $inner $j] > $prev_gno}
            }

            if {$page_count >= 2} {
                set second_page [$S GTIDX GAPLOG LIST 1 1]
                assert_equal [llength $second_page] 1
                assert_equal [lindex $second_page 0] [lindex $list_result 1]
            }
        }

        test "GAPLOG-LIST-006: LIST returns {uuid gno {dbid key type subkeys}}" {
            set first_entry [lindex [$S GTIDX GAPLOG LIST 0 1] 0]
            # outer: [uuid, inner_array]
            assert_equal [llength $first_entry] 2
            # uuid
            set entry_uuid [lindex $first_entry 0]
            assert {[string length $entry_uuid] > 0}
            # inner_array: [gno, keys, gno, keys, ...]
            set body [lindex $first_entry 1]
            set first_gno [lindex $body 0]
            assert {[string is integer -strict $first_gno]}
            set keys [lindex $body 1]
            assert {[llength $keys] > 0}
            foreach key_entry $keys {
                assert_equal [llength $key_entry] 4
                set dbid [lindex $key_entry 0]
                set key [lindex $key_entry 1]
                set type [lindex $key_entry 2]
                set subkeys [lindex $key_entry 3]
                assert {$dbid >= 0 && $dbid <= 15}
                assert {[string length $key] > 0}
                assert {[string length $type] > 0}
                assert {[string is list $subkeys]}
            }
        }

        test "GAPLOG-LIST-007: LIST start_idx out of range returns empty array" {
            set gaplog_len [$S GTIDX GAPLOG LEN]
            set result [$S GTIDX GAPLOG LIST $gaplog_len 10]
            assert_equal $result {}
        }

        test "GAPLOG-LIST-008: LIST count exceeds remaining entries - truncated" {
            set gaplog_len [$S GTIDX GAPLOG LEN]
            set start_idx [expr {$gaplog_len - 2}]
            if {$start_idx < 0} { set start_idx 0 }
            set result [$S GTIDX GAPLOG LIST $start_idx 100]
            set expected [expr {$gaplog_len - $start_idx}]
            assert {[llength $result] == $expected}
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]

        test "GAPLOG-ALL-001: ALL returns empty gtidSet when gaplog empty" {
            $S GTIDX GAPLOG CLEAR
            set result [$S GTIDX GAPLOG ALL]
            assert_equal $result "" {}
        }

        test "GAPLOG-ALL-002: ALL returns correct gtidSet encoding" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S set k1 v1; $S set k2 v2; $S set k3 v3
            set su [get_slave_gtid_uuid $S]
            replicaof_xcontinue $S $Mh $Mp
            set all [$S GTIDX GAPLOG ALL]
            assert {[string length $all] > 0}
            assert_match "${su}:*" $all
        }

        test "GAPLOG-ALL-003: ALL gno range covers written gnos" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b2 m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            set gl [gaploglen $S]
            $S set a1 v1; $S set a2 v2; $S set a3 v3
            set su [get_slave_gtid_uuid $S]
            replicaof_xcontinue $S $Mh $Mp
            set gl [gaploglen $S]
            set all [$S GTIDX GAPLOG ALL]
            set pattern "${su}:"
            set idx [string first $pattern $all]
            assert {$idx >= 0}
            set gno_part [string range $all [expr {$idx + [string length $pattern]}] end]
            set gno_end [lindex [split $gno_part "-"] end]
            set gno_end [lindex [split $gno_end ","] 0]
            assert {$gno_end >= $gl}
        }
    }
}


