


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
    test "GAPLOG-CMD-003: GAPLOG RANGE - empty gaplog returns empty array" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "uuid-001" 1 10]
        assert_equal $result {}
    }

    test "GAPLOG-CMD-004: GAPLOG RANGE - non-existent uuid returns empty array" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "non-existent-uuid" 1 10]
        assert_equal $result {}
    }

    test "GAPLOG-CMD-005: GAPLOG RANGE - start > end returns empty" {
        r GTIDX GAPLOG CLEAR
        catch {[r GTIDX GAPLOG RANGE "uuid-001" 5 1]} err
        assert_match "ERR start gno must be <= end gno" $err
    }

    test "GAPLOG-CMD-006: GAPLOG RANGE - start beyond max gno returns empty" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "uuid-001" 10 20]
        assert_equal $result {}
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-007: GAPLOG CLEAR - empty gaplog returns OK" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"
        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }

    test "GAPLOG-CMD-008: GAPLOG CLEAR - RANGE returns empty after clear" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"

        set result [r GTIDX GAPLOG RANGE "uuid-001" 1 10]
        assert_equal $result {}
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-009: GAPLOG LEN - wrong args count error" {
        catch {r GTIDX GAPLOG LEN "extra-arg"} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-CMD-010: GAPLOG RANGE - wrong args count error" {
        catch {r GTIDX GAPLOG RANGE "uuid"} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-CMD-011: GAPLOG RANGE - invalid gno format error" {
        catch {r GTIDX GAPLOG RANGE "uuid-001" "abc" "def"} err
        assert_match "*integer*" $err
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
        assert_match "*GAPLOG RANGE*" $help
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

    test "GAPLOG-LIST-004: LIST with count > 100 returns error" {
        catch {r GTIDX GAPLOG LIST 0 101} err
        assert_match "*count must*" $err
    }

    test "GAPLOG-LIST-005: LIST with wrong args count returns error" {
        catch {r GTIDX GAPLOG LIST 0} err
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

            set gaplog_len [$S GTIDX GAPLOG LEN]
            assert {$gaplog_len > 0}
            puts "gaplog len: $gaplog_len"

            set list_result [$S GTIDX GAPLOG LIST 0 $gaplog_len]
            assert {[llength $list_result] > 0}
            puts "list_result len: [llength $list_result]"

            set first_entry [lindex $list_result 0]
            assert {[llength $first_entry] == 3}
            set entry_uuid [lindex $first_entry 0]
            set entry_gno [lindex $first_entry 1]
            assert_equal $entry_uuid $uuid
            assert {$entry_gno > 0}

            set list_result2 [$S GTIDX GAPLOG LIST 0 1]
            assert {[llength $list_result2] == 1}

            if {$gaplog_len >= 2} {
                set list_result3 [$S GTIDX GAPLOG LIST 1 1]
                assert {[llength $list_result3] == 1}
                set second_entry [lindex $list_result 1]
                set third_entry [lindex $list_result3 0]
                assert_equal [lindex $second_entry 1] [lindex $third_entry 1]
            }
        }

        test "GAPLOG-LIST-006: LIST keys content - verify dbid/type/key/subkeys format" {
            set list_result [$S GTIDX GAPLOG LIST 0 1]
            set first_entry [lindex $list_result 0]
            assert {[llength $first_entry] == 3}

            set keys [lindex $first_entry 2]
            assert {[llength $keys] > 0}

            foreach key_entry $keys {
                assert {[llength $key_entry] == 4}
                set dbid [lindex $key_entry 0]
                set ktype [lindex $key_entry 1]
                set kname [lindex $key_entry 2]
                set subkeys [lindex $key_entry 3]

                assert {$dbid >= 0 && $dbid <= 15}

                assert {[string length $ktype] > 0}

                assert {[string length $kname] > 0}

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

        test "GAPLOG DELETERANGE command - delete single entry" {
            set info [$S INFO gtid]
            set uuid ""
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_uuid:*" $line]} {
                    set uuid [string range $line 10 end]
                    break
                }
            }
            assert {$uuid != ""}

            set gaplog_len [$S GTIDX GAPLOG LEN]
            assert {$gaplog_len > 0}

            set list_result [$S GTIDX GAPLOG LIST 0 1]
            set first_entry [lindex $list_result 0]
            set first_gno [lindex $first_entry 1]
            puts "first_gno: $first_gno"

            set deleted [$S GTIDX GAPLOG DELETERANGE $uuid $first_gno $first_gno]
            assert {$deleted > 0}
            puts "deleted: $deleted"

            set new_len [$S GTIDX GAPLOG LEN]
            assert {$new_len == [expr {$gaplog_len - $deleted}]}
        }

        test "GAPLOG-LIST-009: LIST after DEL - verify deleted entry removed and order preserved" {
            set gaplog_len [$S GTIDX GAPLOG LEN]
            if {$gaplog_len > 0} {
                set list_result [$S GTIDX GAPLOG LIST 0 $gaplog_len]
                assert {[llength $list_result] == $gaplog_len}


                set first_entry [lindex $list_result 0]
                set first_uuid [lindex $first_entry 0]
                set first_gno [lindex $first_entry 1]
                set deleted [$S GTIDX GAPLOG DELETERANGE $first_uuid $first_gno $first_gno]
                assert {$deleted == 1}

                set after_list [$S GTIDX GAPLOG LIST 0 $gaplog_len]
                assert {[llength $after_list] == [expr {$gaplog_len - 1}]}

                if {[llength $list_result] >= 2} {
                    set second_before [lindex $list_result 1]
                    set first_after [lindex $after_list 0]
                    assert_equal [lindex $second_before 0] [lindex $first_after 0]
                    assert_equal [lindex $second_before 1] [lindex $first_after 1]
                }
            }
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CMD-016: DEL+DELETERANGE partial" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 10} {incr i} { $S set "t_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            set mg [lindex [lindex [$S GTIDX GAPLOG LIST 0 1] 0] 1]
            assert_equal [$S GTIDX GAPLOG DELETERANGE $su [expr {$mg+1}] [expr {$mg+1}]] 1
            assert_equal [$S GTIDX GAPLOG DELETERANGE $su [expr {$mg+3}] [expr {$mg+3}]] 1
            assert_equal [llength [$S GTIDX GAPLOG RANGE $su $mg [expr {$mg+4}]]] 6
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CMD-015: RANGE start=end" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S set p1 v1; $S set p2 v2; $S set p3 v3
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            set mg [lindex [lindex [$S GTIDX GAPLOG LIST 0 1] 0] 1]
            set r [$S GTIDX GAPLOG RANGE $su $mg $mg]
            assert {[llength $r] == 2}; assert_equal [lindex $r 0] $mg
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-DEL-001: non-contiguous" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 5} {incr i} { $S set "d_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            set gl [gaploglen $S]; assert {$gl >= 5}
            set mg [lindex [lindex [$S GTIDX GAPLOG LIST 0 1] 0] 1]
            assert_equal [$S GTIDX GAPLOG DELETERANGE $su $mg $mg] 1
            assert_equal [$S GTIDX GAPLOG DELETERANGE $su [expr {$mg+2}] [expr {$mg+2}]] 1
            assert_equal [gaploglen $S] [expr {$gl-2}]
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-DEL-002: uuid isolation" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 5} {incr i} { $S set "iso_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            set mg [lindex [lindex [$S GTIDX GAPLOG LIST 0 1] 0] 1]
            assert_equal [$S GTIDX GAPLOG DELETERANGE "NO_UUID" $mg [expr {$mg+4}]] 0
        }
    }
}