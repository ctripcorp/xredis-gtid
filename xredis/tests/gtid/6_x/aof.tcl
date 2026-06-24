if {!$::swap} {

# ---------------------------------------------------------------------------
# Redis 6.x AOF infrastructure: single appendonly.aof file.
# ---------------------------------------------------------------------------

set defaults {appendonly yes appendfilename appendonly.aof gtid-enabled yes auto-aof-rewrite-percentage 0}
set server_path [tmpdir server.aof]
set gtid_module_repl_aof_server_path [tmpdir gtid.module.replicate.aof]

proc server_is_alive {} {
    is_alive [lindex $::servers end]
}

proc append_to_aof {str} {
    upvar fp fp
    puts -nonewline $fp $str
}

proc create_aof {dir basename code} {
    upvar 1 fp fp
    set path "$dir/$basename"
    set fp [open $path w+]
    uplevel 1 $code
    close $fp
}

proc start_server_aof {overrides code} {
    upvar defaults defaults server_path server_path
    set config [concat $defaults $overrides]
    start_server [list overrides $config keep_persistence true] $code
}

proc read_command_arg {fd} {
    set len [::redis::redis_read_line $fd]
    set len [string range $len 1 end]
    set buf [gets $fd]
    while {[string length $buf] != $len} {
        set buf1 [gets $fd]
        set buf "$buf$buf1"
    }
    return $buf
}

proc read_command {fd} {
    set count [::redis::redis_read_line $fd]
    if {$count == 0} {
        return {}
    }
    set count [string range $count 1 end]
    set res {}
    for {set j 0} {$j < $count} {incr j} {
        set arg [read_command_arg $fd]
        if {$j == 0} {set arg [string tolower $arg]}
        lappend res $arg
    }
    return $res
}

proc try_read_aof {file line_count} {
    set try_num 3
    set res {}
    while {$try_num > 0} {
        set fd [open $file]
        set res {}
        set command [read_command $fd]
        while {$command != {}} {
            lappend res $command
            set command [read_command $fd]
        }
        close $fd
        if {[llength $res] == $line_count} {
            break
        }
        set try_num [expr {$try_num - 1}]
        after 1000
    }
    assert_equal [llength $res] $line_count
    return $res
}

proc assert_aof {s patterns} {
    assert_equal [llength $s] [llength $patterns]
    for {set j 0} {$j < [llength $patterns]} {incr j} {
        assert_match [lindex $patterns $j] [lindex $s $j]
    }
}

proc aof_incr_path {dir basename} {
    return "$dir/$basename"
}

test "aof" {
    test "save aof and reload aof" {
        start_server_aof [list dir $server_path aof-load-truncated yes] {
            test "write expire command save to aof" {
                set client [redis [srv host] [srv port] 0 $::tls]
                $client set k1 v ex 1000
                $client set k2 v px 2000
                $client set k3 v
                $client expire k3 1000
                $client set k4 v
                $client pexpire k4 2000
                $client set k v
                set dir [dict get [srv config] dir]
                set res [try_read_aof [aof_incr_path $dir appendonly.aof] 8]
                assert_aof $res {
                    {select *}
                    {gtid * 0 SET k1 v PXAT *}
                    {gtid * 0 SET k2 v PXAT *}
                    {gtid * 0 set k3 v}
                    {gtid * 0 PEXPIREAT k3 *}
                    {gtid * 0 set k4 v}
                    {gtid * 0 PEXPIREAT k4 *}
                    {gtid * 0 set k v}
                }
            }
        }

        start_server_aof [list dir $server_path aof-load-truncated yes] {
            test "restart redis load aof" {
                set dir [dict get [srv config] dir]
                set res [try_read_aof [aof_incr_path $dir appendonly.aof] 8]
                assert_aof $res {
                    {select *}
                    {gtid * 0 SET k1 v PXAT *}
                    {gtid * 0 SET k2 v PXAT *}
                    {gtid * 0 set k3 v}
                    {gtid * 0 PEXPIREAT k3 *}
                    {gtid * 0 set k4 v}
                    {gtid * 0 PEXPIREAT k4 *}
                    {gtid * 0 set k v}
                }
                after 500
                set client [redis [srv host] [srv port] 0 $::tls]
                assert_equal [$client get k] v
            }
        }

        create_aof $server_path appendonly.aof {
            append_to_aof [formatCommand gtid A:1 0 set k1 y]
            append_to_aof [formatCommand set k2 y]
            append_to_aof [formatCommand gtid A:2 0 PEXPIREAT k2 100000]
        }

        start_server_aof [list dir $server_path aof-load-truncated yes] {
            test "Unfinished MULTI: Server should start if load-truncated is yes" {
                assert_equal 1 [server_is_alive]
                set client [redis [srv host] [srv port] 0 $::tls]
                assert_equal [$client get k1] y
                assert_equal [$client get k2] {}
            }
        }

        start_server [list overrides [list dir $server_path appendonly yes appendfilename appendonly.aof2 gtid-enabled yes]] {
            test {Redis should not try to convert DEL into EXPIREAT for EXPIRE -1} {
                r setex k1 10 y
                r set k2 y
                r expire k2 1000
                r set k3 y ex 1000
                r set k5 y
                set dir [dict get [srv 0 config] dir]
                set res [try_read_aof [aof_incr_path $dir appendonly.aof2] 6]
                assert_aof $res {
                    {select *}
                    {gtid * 9 SET k1 y PXAT *}
                    {gtid * 9 set k2 y}
                    {gtid * 9 PEXPIREAT k2 *}
                    {gtid * 9 SET k3 y PXAT *}
                    {gtid * 9 set k5 y}
                }
            }
        }
    }

    set gtid_module_repl_aof_overrides [list \
        dir $gtid_module_repl_aof_server_path \
        appendonly yes \
        appendfilename appendonly.aof \
        auto-aof-rewrite-percentage 0 \
        aof-load-truncated yes \
        appendfsync always \
        save "" \
        gtid-enabled yes]

    start_server [list overrides $gtid_module_repl_aof_overrides keep_persistence true] {
        test {prepare AOF for GTID stale embedded identity regression} {
            set raw [redis [srv host] [srv port] 0 $::tls]
            assert_equal OK [$raw gtid "repro:1" 0 set leak-seed seed]
            set dir [dict get [srv config] dir]
            set commands [try_read_aof [aof_incr_path $dir appendonly.aof] 2]
            $raw close

            assert_equal {select 0} [lindex $commands 0]
            assert_equal {gtid repro:1 0 set leak-seed seed} [lindex $commands 1]
        }
    }

    start_server [list overrides $gtid_module_repl_aof_overrides keep_persistence true] {
        test {GTID AOF reload does not leak stale embedded identity to first plain write} {
            set raw [redis [srv host] [srv port] 0 $::tls]
            assert_equal OK [$raw set leak-fresh value]
            set dir [dict get [srv config] dir]
            set commands [try_read_aof [aof_incr_path $dir appendonly.aof] 4]
            $raw close

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
}
