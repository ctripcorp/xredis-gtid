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
    set info [$client INFO gtid]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_uuid:*" $line]} {
            return [string range $line 10 end]
        }
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
proc gaploglen {c} {
    return [get_gaplog_entries $c]
}
proc get_gaplog_gtidset {client} {
    set info [$client INFO gtid]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_gaplog:*" $line]} {
            set raw [string range $line 12 end]
            if {[string length $raw] >= 2 \
                    && [string index $raw 0] eq "\"" \
                    && [string index $raw end] eq "\""} {
                return [string range $raw 1 end-1]
            }
            return $raw
        }
    }
    return ""
}
proc gaplog_get_key_type {client key} {
    set gaplog_len [gaploglen $client]
    if {$gaplog_len == 0} { return "" }
    set list_result [$client GTIDX GAPLOG LIST 0 $gaplog_len]
    foreach entry $list_result {
        set uuid [lindex $entry 0]
        set body [lindex $entry 1]
        for {set j 0} {$j < [llength $body]} {incr j 2} {
            set keys [lindex $body [expr {$j + 1}]]
            foreach key_entry $keys {
                set kname [lindex $key_entry 1]
                set ktype [lindex $key_entry 2]
                if {$kname == $key} {
                    return $ktype
                }
            }
        }
    }
    return ""
}
proc gaplog_get_key_type_debug {client key} {
    set gaplog_len [gaploglen $client]
    if {$gaplog_len == 0} { puts "gaplog empty"; return "" }
    set list_result [$client GTIDX GAPLOG LIST 0 $gaplog_len]
    puts "gaplog entries: $gaplog_len"
    set idx 0
    foreach entry $list_result {
        set uuid [lindex $entry 0]
        set body [lindex $entry 1]
        puts "entry $idx: uuid=$uuid ngroups=[expr {[llength $body] / 2}]"
        for {set j 0} {$j < [llength $body]} {incr j 2} {
            set gno [lindex $body $j]
            set keys [lindex $body [expr {$j + 1}]]
            puts "  gno=$gno nkeys=[llength $keys]"
            foreach key_entry $keys {
                set kname [lindex $key_entry 1]
                set ktype [lindex $key_entry 2]
                puts "    key: '$kname' type: '$ktype'"
                if {$kname == $key} {
                    puts "    -> MATCHED"
                    return $ktype
                }
            }
        }
        incr idx
    }
    puts "key '$key' not found in any entry"
    return ""
}

proc dbsize_loadsafe {r varname} {
    upvar 1 $varname dbsize
    if {$::swap} {
        return [expr {[catch {{*}$r dbsize} dbsize] == 0}]
    }
    set dbsize [{*}$r dbsize]
    return 1
}

# Commands whose proc rewrites argv before propagate; must stay in sync with
# gtidGetRewriteCmdProcs() in xredis_gtid_adaptation_version_{6,8}x.c
proc gtid_rewrite_cmd_list_common {} {
    return {expire pexpire expireat setex psetex getset \
        blmove brpoplpush blpop brpop bzpopmin bzpopmax geoadd}
}

proc gtid_rewrite_cmd_list_6x {} {
    return [gtid_rewrite_cmd_list_common]
}

proc gtid_rewrite_cmd_list_8x {} {
    return {hexpire hpexpire hexpireat hsetex}
}

proc gtid_redis_major_version {r} {
    if {![info exists ::gtid_redis_major_version]} {
        regexp {redis_version:(\d+)\.(\d+)\.(\d+)} [{*}$r info server] _ major minor patch
        set ::gtid_redis_major_version $major
    }
    return $::gtid_redis_major_version
}

proc gtid_rewrite_cmd_list {} {
    set cmds [gtid_rewrite_cmd_list_6x]
    if {[gtid_redis_major_version r] >= 8} {
        foreach cmd [gtid_rewrite_cmd_list_8x] {
            if {[lsearch -exact $cmds $cmd] < 0} {
                lappend cmds $cmd
            }
        }
    }
    return $cmds
}

proc gtid_rewrite_cmd_build_args {cmd} {
    set now_s [clock seconds]
    set now_ms [clock milliseconds]
    switch -- [string tolower $cmd] {
        expire     { return [list expire k1 1000] }
        pexpire    { return [list pexpire k1 1000] }
        expireat   { return [list expireat k1 [expr {$now_s + 100}]] }
        setex      { return [list setex k1 10 v] }
        psetex     { return [list psetex k1 10000 v] }
        getset     { return [list getset getset_key new] }
        blmove     { return [list blmove src_key dst_key LEFT RIGHT 1] }
        brpoplpush { return [list brpoplpush src_key dst_key 1] }
        blpop      { return [list blpop list_key 1] }
        brpop      { return [list brpop list_key 1] }
        bzpopmin   { return [list bzpopmin zset_key 1] }
        bzpopmax   { return [list bzpopmax zset_key 1] }
        geoadd     { return [list geoadd geo_key 13.36 38.11 palermo] }
        hexpire    { return [list hexpire hash_key 100 FIELDS 1 f1] }
        hpexpire   { return [list hpexpire hash_key 100 FIELDS 1 f1] }
        hexpireat  { return [list hexpireat hash_key [expr {$now_s + 100}] FIELDS 1 f1] }
        hsetex     { return [list hsetex hash_key PX 100 FIELDS 1 f2 v2] }
        default    { return [list $cmd] }
    }
}

proc gtid_seed_rewrite_cmd_keys {r} {
    $r set k1 v
    $r set getset_key v
    $r lpush src_key a
    $r lpush dst_key b
    $r lpush list_key a
    $r zadd zset_key 1 a
    $r hset hash_key f1 v1
}
