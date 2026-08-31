start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    set master_repl [attach_to_replication_stream]
    set orig_db 0

    start_server {overrides {gtid-enabled yes}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]
        catch {$slave config set repl-rdb-channel no}
        # Init replication link and and repl stream
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

        test "propagte repl: GTID-ENABLED(yes) TX(yes) CMD(write,may-replicate)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master MULTI
            $master GET key
            $master SET key val1
            $master PUBLISH hello world
            $master SET key val2
            $master EXEC

            wait_for_gtid_sync $master $slave

            incr mygno
            set mygtidset "$myuuid:1-$mygno"

            assert_equal [$master GET key] val2
            assert_equal [$slave GET key] val2

            set exec_pattern [format {gtid %s:%s * [Ee][Xx][Ee][Cc]} $myuuid $mygno]
            assert_replication_stream $master_repl [list multi {set key val1} {publish hello world} {set key val2} $exec_pattern]
            assert_replication_stream $slave_repl  [list multi {set key val1} {publish hello world} {set key val2} $exec_pattern]

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: GTID-ENABLED(yes) TX(yes) CMD(gtid) => not allowed" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            set orig_val [$master GET key]

            $master MULTI
            $master PUBLISH hello world
            catch {$master GTID A:1 1 set key valx} err
            assert_match "*gtidCommand not allowed in multi*" $err
            catch {$master EXEC} err
            assert_match "*EXECABORT Transaction discarded because of previous errors*" $err

            assert_replication_stream $master_repl {{}}
            assert_replication_stream $slave_repl  {{}}

            assert_equal [$master GET key] $orig_val
            assert_equal [$slave GET key] $orig_val
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(write)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SET key val3

            wait_for_gtid_sync $master $slave

            incr mygno
            set mygtidset "$myuuid:1-$mygno"

            assert_equal [$master GET key] val3
            assert_equal [$slave GET key] val3

            assert_replication_stream $master_repl [list "gtid $myuuid:$mygno * SET key val3"]
            assert_replication_stream $slave_repl  [list "gtid $myuuid:$mygno * SET key val3"]

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(may-replicate)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master PUBLISH hello world

            wait_for_gtid_sync $master $slave

            # mygno not incrmented
            set mygtidset "$myuuid:1-$mygno"

            assert_replication_stream $master_repl {{publish hello world}}
            assert_replication_stream $slave_repl  {{publish hello world}}

            assert_match  "*$mygtidset*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] -1
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] -1

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] {}
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] {}
        }

        test "propagte repl: GTID-ENABLED(yes) TX(no) CMD(gtid)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master GTID A:1 1 SET key val4; # master changed to db-1

            wait_for_gtid_sync $master $slave

            $master select 1
            $slave select 1
            assert_equal [$master GET key] val4
            assert_equal [$slave GET key] val4

            assert_replication_stream $master_repl [list {select 1} "gtid A:1 * SET key val4"]
            assert_replication_stream $slave_repl  [list {select 1} "gtid A:1 * SET key val4"]

            assert_match  "*A:1*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            # length of *2\r\n$6\r\nselect\r\n$1\r\n0\r\n is 23
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1+23]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:1"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:1"

            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master select $orig_db
            $slave select $orig_db

            $master GTID A:2 0 SET key val5

            wait_for_gtid_sync $master $slave

            assert_equal [$master GET key] val5
            assert_equal [$slave GET key] val5

            assert_replication_stream $master_repl [list "select $orig_db" "gtid A:2 * SET key val5"]
            assert_replication_stream $slave_repl  [list "select $orig_db" "gtid A:2 * SET key val5"]

            assert_match  "*A:1-2*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1+23]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:2"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:2"

        }

        # NOTE: "propagte repl: expire" was removed from this
        # version-agnostic file because 6.x and 8.x have different
        # propagation expectations (SET PX vs PXAT). It is covered in the
        # respective gtid/6_x/gtid.tcl and gtid/8_x/gtid.tcl files.
        #
        # "propagte repl: INCR on master replicates, user GTID+INCR rejected"
        # was removed because INCR does not carry CMD_GTID_NON_DETERMINISM;
        # expecting a user-wrapped GTID INCR to be rejected was incorrect.
    }
}

start_server {tags {"gtid"} overrides {gtid-enabled no}} {
    set master_repl [attach_to_replication_stream]
    if {$::swap} {
        set orig_db 0
    } else {
        set orig_db 9
    }

    start_server {overrides {gtid-enabled no}} {
        set master [srv -1 client]
        set master_host [srv -1 host]
        set master_port [srv -1 port]
        set slave [srv 0 client]

        # Init replication link and and repl stream
        $slave replicaof $master_host $master_port
        wait_for_sync $slave

        set slave_repl [attach_to_replication_stream]

        $master SET key val0
        wait_for_ofs_sync $master $slave
        assert_equal [$master GET key] val0
        assert_equal [$slave GET key] val0

        set myuuid [status $master gtid_uuid]
        set mygno  [status $master gtid_executed_gno_count]

        assert_replication_stream $master_repl [list {select *} "set key val0"]
        assert_replication_stream $slave_repl  [list {select *} "set key val0"]

        test "propagte repl: GTID-ENABLED(no) TX(no) CMD(gtid)" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master GTID A:1 $orig_db SET key val1

            wait_for_ofs_sync $master $slave

            assert_equal [$master GET key] val1
            assert_equal [$slave GET key] val1

            assert_replication_stream $master_repl [list "gtid A:1 * SET key val1"]
            assert_replication_stream $slave_repl  [list "gtid A:1 * SET key val1"]

            assert_match  "*A:1*" [status $master gtid_executed]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 0] [expr $orig_master_reploff+1]
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 0] [expr $orig_slave_reploff+1]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "A:1"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "A:1"
        }

        test "propagte repl: GTID-ENABLED(no) TX(yes) CMD(gtid) => not allowed" {
            set orig_master_reploff [status $master master_repl_offset]
            set orig_slave_reploff  [status $slave  master_repl_offset]
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            set orig_val [$master GET key]

            $master MULTI
            $master PUBLISH hello world
            catch { $master GTID A:1 1 set key valx } err
            assert_match "*gtidCommand not allowed in multi*" $err
            catch { $master EXEC} err
            assert_match "*EXECABORT Transaction discarded because of previous errors*" $err

            assert_replication_stream $master_repl {{}}
            assert_replication_stream $slave_repl  {{}}

            assert_equal [$master GET key] $orig_val
            assert_equal [$slave GET key] $orig_val
        }

    }
}

start_server {tags {"gtid"} overrides {gtid-enabled yes}} {

    test "gtid command with gtid.set-lost ignored" {
        r GTIDX ADD LOST A 1 10
        catch {r GTID A:5 0 set hello world} reply
        assert_match "*gtid command already executed*" $reply
    }

}

start_server {tags {"gtid"} overrides} {
    test "gtid uses jemalloc allocator" {
        assert_equal [s gtid_allocator] [s mem_allocator]
    }
}

start_server {tags {"master"} overrides} {
    test "change gtid-enabled efficient" {
        set repl [attach_to_replication_stream]
        r set k v1
        assert_replication_stream $repl {
            {select *}
            {set k v1}
        }
        assert_equal [r get k] v1

        r config set gtid-enabled yes

        set repl [attach_to_replication_stream]
        r set k v2

        assert_replication_stream $repl {
            {select *}
            {gtid * * set k v2}
        }
        assert_equal [r get k] v2

        r config set gtid-enabled no
        set repl [attach_to_replication_stream]
        r set k v3
        assert_replication_stream $repl {
            {select *}
            {set k v3}
        }
        assert_equal [r get k] v3
    }
}

#closed gtid-enabled, can exec gtid command
start_server {tags {"gtid"} overrides} {
    test "exec gtid command" {
        r gtid A:1 $::target_db set k v
        assert_equal [r get k] v
        assert_equal [dict get [get_gtid r] "A"] "1"
    }
}

# stand-alone redis exec gtid related commands
start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test {COMMANDS} {
        test {GTID SET} {
            r gtid A:1 $::target_db set x foobar
            r get x
        } {foobar}

        test {GTID REPATE SET} {
            catch {r gtid A:1 $::target_db set x foobar} error
            assert_match $error "gtid command already executed, `A:1`, `$::target_db`, `set`,"
        }
        test {SET} {
            r set y foobar
            r get y
        } {foobar}

        test {MULTI} {
            r multi
            r set z foobar
            r gtid A:3 $::target_db exec
            r set z f
            r get z
        } {f}
        test {MULTI} {
            set z_value [r get z]
            r del x
            assert_equal [r get x] {}
            r multi
            r set z foobar1
            catch {r gtid A:3 $::target_db exec} error
            assert_equal $error "gtid command already executed, `A:3`, `$::target_db`, `exec`,"
            assert_equal [r get z] $z_value
            r set x f1
            r get x
        } {f1}
        test "ERR WRONG NUMBER" {
            catch {r gtid A } error
            assert_match "ERR wrong number of arguments for 'gtid' command" $error
        }

    }

    test {INFO GTID} {
        set dicts [dict get [get_gtid r] [status r run_id]]
        set value [lindex $dicts 0]
        assert_equal [string match {1-*} $value] 1
    }
}

# NOTE: The "multi-exec select db" cases (with and without MULTI/EXEC) were
# intentionally removed from this version-agnostic file. Their replication
# stream expectations diverge between 6.x and 8.x (SELECT position relative
# to MULTI, EXEC case sensitivity), so they are covered separately in
# gtid/6_x/gtid.tcl and gtid/8_x/gtid.tcl instead.


# NOTE: The "GTID cross-DB transaction ... chain replication" case below was
# intentionally removed from this version-agnostic file. Its replication
# stream expectations diverge between 6.x and 8.x (SELECT position relative
# to MULTI, EXEC case sensitivity, target db), so it is covered separately in
# gtid/6_x/gtid.tcl and gtid/8_x/gtid.tcl instead.






# Verify that GTID command rejects commands that would rewrite their argv
# (e.g. expire -> PEXPIREAT, setex -> SET PX, incrbyfloat -> SET). Rewriting
# argv inside the gtid command body is unsafe: the rewritten argv is dropped
# on gtidCommand exit (which restores orig_argv), so the unrewritten original
# is written to AOF/replication and breaks master-replica consistency. The
# command list is version-specific and supplied by gtid_rewrite_cmd_list.
start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test {GTID should reject commands that rewrite argv} {
        set rewrite_cmds [gtid_rewrite_cmd_list]
        set gno 1
        set now_seconds [clock seconds]
        set now_ms [clock milliseconds]

        foreach cmd $rewrite_cmds {
            switch -- $cmd {
                "append"        { set args [list $cmd rw_append v] }
                "expire"        { set args [list $cmd k1 1000] }
                "pexpire"       { set args [list $cmd k1 1000] }
                "expireat"      { set args [list $cmd k1 [expr {$now_seconds + 100}]] }
                "hexpireat"     { set args [list $cmd hash_key [expr {$now_seconds + 100}] FIELDS 1 f1] }
                "setex"         { set args [list $cmd k1 10 v] }
                "psetex"        { set args [list $cmd k1 10000 v] }
                "getdel"        { set args [list $cmd rw_getdel k] }
                "getset"        { set args [list $cmd getset_key new] }
                "getex"         { set args [list $cmd getset_key EX 100] }
                "setrange"      { set args [list $cmd rw_setrange 0 v] }
                "incr"          { set args [list $cmd incr_key] }
                "decr"          { set args [list $cmd decr_key] }
                "hexpire"       { set args [list $cmd hash_key 100 f1] }
                "hpexpire"      { set args [list $cmd hash_key 100 f1] }
                "hsetex"        { set args [list $cmd hash_key 100 f2 v2] }
                "hgetdel"       { set args [list $cmd hash_key f1] }
                "hgetex"        { set args [list $cmd hash_key f1 EX 100] }
                "hincrby"       { set args [list $cmd hash_key f1 1] }
                "hincrbyfloat"  { set args [list $cmd hash_key f1 1.1] }
                "incrbyfloat"   { set args [list $cmd getset_key 1.1] }
                "blmove"        { set args [list $cmd src_key dst_key LEFT RIGHT 1] }
                "brpoplpush"    { set args [list $cmd src_key dst_key 1] }
                "blpop"         { set args [list $cmd list_key 1] }
                "brpop"         { set args [list $cmd list_key 1] }
                "blmpop"        { set args [list $cmd 0 1 list_key LEFT] }
                "bzpopmin"      { set args [list $cmd zset_key 1] }
                "bzpopmax"      { set args [list $cmd zset_key 1] }
                "geoadd"        { set args [list $cmd geo_key 13.36 38.11 palermo] }
                "bzmpop"        { set args [list $cmd 0 1 hash_key MIN] }
                "zmpop"         { set args [list $cmd hash_key 2 MIN] }
                "spop"          { set args [list $cmd spop_key] }
                default         { fail "unexpected command $cmd" }
            }
            catch {r gtid A:$gno 0 {*}$args} result
            assert_match {*ERR*} $result
            incr gno
        }
    }
}

start_server {tags {"gtid"} overrides {gtid-enabled yes}} {
    test "GTID rejects commands that rewrite argv" {
        gtid_seed_rewrite_cmd_keys r
        set before_gno [status r gtid_executed_gno_count]
        set before_gtid [status r gtid_set]

        set gno 1
        foreach cmd [gtid_rewrite_cmd_list] {
            set args [gtid_rewrite_cmd_build_args $cmd]
            catch {r gtid "rwtest:$gno" 0 {*}$args} err
            assert_match "*nondeterminism in gtid command*" $err
            incr gno
        }

        assert_equal $before_gno [status r gtid_executed_gno_count]
        assert_equal $before_gtid [status r gtid_set]
    }

    test "GTID accepts canonical PEXPIREAT (no argv rewrite)" {
        r set k_accept v
        set abs_ms 2000000000000
        assert_equal [r gtid canon:1 0 PEXPIREAT k_accept $abs_ms] 1
        assert_match "*canon:1*" [status r gtid_set]
    }

    test "GTID accepts canonical HPEXPIREAT on Redis 8+ (no argv rewrite)" {
        # Avoid bare `return` inside test{} — Redis 6 test.tcl's catch treats
        # TCL_RETURN as an unexpected exception.
        if {[gtid_redis_major_version r] >= 8} {
            r hset hash_canon f1 v1
            set abs_ms 2000000000000
            assert_equal [r gtid canon:2 0 HPEXPIREAT hash_canon $abs_ms FIELDS 1 f1] 1
            assert_match "*canon:1-2*" [status r gtid_set]
        }
    }

    test "GTID rewrite rejection leaves keyspace unchanged (geoadd)" {
        catch {r gtid rw:1 0 geoadd geo_key 13.36 38.11 x} err
        assert_match "*nondeterminism in gtid command*" $err
        assert_equal 0 [r zcard geo_key]
    }
}
