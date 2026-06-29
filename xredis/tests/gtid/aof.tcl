if {!$::swap} {
set defaults { appendonly {yes} appendfilename {appendonly.aof} gtid-enabled yes}    
proc aof_server_path {} {
    variable p
    if {![info exists p]} { set p [tmpdir server.aof] }
    return $p
}
proc aof_basename {} { return appendonly.aof }

proc aof_start_server {overrides code} {
    upvar defaults defaults srv srv server_path server_path
    set config [concat $defaults $overrides]
    set srv [start_server [list overrides $config ]]
    uplevel 1 $code
    kill_server $srv
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

test "save aof and reload aof" {

    aof_start_server [list dir [aof_server_path] aof-load-truncated yes] {
        test "write expire command save to aof" {
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            $client set k1 v ex 1000
            $client set k2 v px 2000
            $client set k3 v
            $client expire k3 1000
            $client set k4 v
            $client pexpire k4 2000
            $client set k v
            set config [dict get $srv config]
            set dir [dict get $config dir]
            set res [try_read_aof [aof_get_path $dir [aof_basename]] 8]
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

    aof_start_server [list dir [aof_server_path] aof-load-truncated yes] {
        test "restart redis load aof" {
            set dir [dict get [dict get $srv config] dir]
            set res [try_read_aof [aof_get_path $dir [aof_basename]] 8]
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
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            assert_equal [$client get k] v
        }
    }

    aof_create_file [aof_get_path [aof_server_path] [aof_basename]] {
        aof_append [formatCommand gtid A:1 0 set k1 y]
        aof_append [formatCommand set k2 y]
        aof_append [formatCommand gtid A:2 0 PEXPIREAT k2 100000]
    }

    aof_start_server [list dir [aof_server_path] aof-load-truncated yes] {
        test "Unfinished MULTI: Server should start if load-truncated is yes" {
            assert_equal 1 [aof_is_alive $srv]
            set client [redis [dict get $srv host] [dict get $srv port] 0 $::tls]
            assert_equal [$client get k1] y
            assert_equal [$client get k2] {}
        }
    }

    start_server [list overrides [list dir [aof_server_path] \
                                          appendonly yes \
                                          appendfilename appendonly.aof2 \
                                          gtid-enabled yes]] {
        test {Redis should not try to convert DEL into EXPIREAT for EXPIRE -1} {
            r setex k1 10 y
            r set k2 y
            r expire k2 1000
            r set k3 y ex 1000
            r set k4 y
            r gtid A:1 9 PEXPIREAT k4 200000000000
            r set k5 y
            set dir [dict get [srv 0 config] dir]
            set res [try_read_aof [aof_get_path $dir appendonly.aof2] 8]
            assert_aof $res {
                {select *}
                {gtid * 9 SET k1 y PXAT *}
                {gtid * 9 set k2 y}
                {gtid * 9 PEXPIREAT k2 *}
                {gtid * 9 SET k3 y PXAT *}
                {gtid * 9 set k4 y}
                {gtid * 9 PEXPIREAT k4 *}
                {gtid * 9 set k5 y}
            }
        }
    }
}

}