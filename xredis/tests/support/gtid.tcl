source "tests/support/gtid_version.tcl"
proc get_gtid { r } {
    set result [dict create]
    if {[regexp "\r\ngtid_set:(.*?)\r\n" [{*}$r info gtid] _ value]} {
        set uuid_sets [split $value ","]
        foreach uuid_set $uuid_sets {
            set uuid_set [split $uuid_set ":"]
            set uuid [lindex $uuid_set 0]
            set value [lreplace $uuid_set 0 0]
            dict set result $uuid $value
        }
    }
    return $result
}

# example
#   a { A:1 }       b {A:1}        return 0
#   a { A:1,B:1}    b {A:1}        return 1
#   a { A:1}        b {A:1,B:1}    return 2
#   a { A:1 }       b {B:1}        return -1
proc gtid_cmp {a b} {
    set a_size [dict size $a]
    set b_size [dict size $b]
    set min $a
    set max $b
    if {$a_size == $b_size} {
        set result 0
    } elseif {$a_size > $b_size} {
        set result 1
        set min $b
        set max $a
    } else {
        set result 2
    }
    dict for {key value} $min {
        if {[dict get $a $key] != $value} {
            set result -1
            return -1
        }
    }
    return $result
}

proc gtid_set_is_equal {repr1  repr2} {
    if {[join [lsort [split $repr1 ","]] ","] eq [join [lsort [split $repr2 ","]] ","]} {
        set _ 1
    } else {
        set _ 0
    }
}

proc wait_for_gtid_sync {r1 r2} {
    wait_for_condition 500 100 {
        [gtid_set_is_equal [status $r1 gtid_set] [status $r2 gtid_set] ]
    } else {
        puts "[$r1 config get port]"
        puts [$r1 info gtid]

        puts "[$r2 config get port]"
        puts [$r2 info gtid]

        press_enter_to_continue

        fail "gtid didn't sync in time"
    }
}

proc repl_ack_off_aligned {master} {
    set infostr [$master INFO REPLICATION]

    set master_repl_offset [getInfoProperty $infostr master_repl_offset]

    set aligned 1
    set lines [split $infostr "\n"]
    foreach line $lines {
        if {[regexp {slave\d+:ip=.*,port=.*,state=.*,offset=(.*?),lag=.*} $infostr _ repl_ack_off]} {
            if {$master_repl_offset != $repl_ack_off} {
                set aligned 0
            }
        }
    }

    set _ $aligned
}

proc get_gaplog_entries {client} {
    set info [$client INFO gtid]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_gaplog_entries:*" $line]} {
            return [string range $line 20 end]
        }
    }
    return 0
}

proc get_slave_gtid_uuid {client} {
    set seq [$client GTIDX seq gtid.set]
    set parts [split $seq ","]
    if {[llength $parts] >= 2} {
        set uuid_gno [lindex $parts 1]
        set uuid [lindex [split $uuid_gno ":"] 0]
        return $uuid
    } elseif {[llength $parts] == 1} {
        set uuid_gno [lindex $parts 0]
        set uuid [lindex [split $uuid_gno ":"] 0]
        return $uuid
    }
    return ""
}



proc get_info_property {r section line property} {
    set str [$r info $section]
    if {[regexp ".*${line}:\[^\r\n\]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        return $submatch
    }
    return ""
}

proc get_slave_gtid_uuid {client} {
    set info [$client INFO gtid]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_uuid:*" $line]} {
            return [string range $line 10 end]
        }
    }
    return ""
}


proc get_gaplog_entries {client} {
    set len [$client GTIDX GAPLOG LEN]
    return $len
}


proc get_uuid {client} {
    return [get_slave_gtid_uuid $client]
}

proc get_xsync_continue_stat {S} { return [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] }
proc wait_xsync_continue_stat {S o} {
    wait_for_condition 50 100 { [get_xsync_continue_stat $S] > $o } else {
        if {[get_xsync_continue_stat $S] > $o} return
        fail "xcontinue not inc"
    }
}
proc replicaof_xcontinue {S Mh Mp} {
    set o [get_xsync_continue_stat $S]; $S replicaof $Mh $Mp; wait_for_sync $S
    wait_xsync_continue_stat $S $o; after 200
}

proc gaploglen {c} { return [$c GTIDX GAPLOG LEN] }