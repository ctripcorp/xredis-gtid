# Redis 6.x-only cases split from gtid.tcl.
# Run explicitly:
#   ./runtest --single gtid/6_x/gtid --clients 1

proc normalize_repl_stream_pattern {pattern} {
    if {[string length $pattern] > 0} {
        set parts [split $pattern]
        if {[llength $parts] > 0} {
            lset parts 0 [string tolower [lindex $parts 0]]
            if {[lindex $parts 0] eq "gtid" && [llength $parts] >= 4} {
                lset parts 3 [string tolower [lindex $parts 3]]
            }
            set pattern [join $parts]
        }
    }
    return $pattern
}

proc normalize_repl_stream_value {value} {
    if {[string length $value] == 0} {
        return $value
    }
    set parts [split $value]
    if {[llength $parts] > 0} {
        lset parts 0 [string tolower [lindex $parts 0]]
        if {[lindex $parts 0] eq "gtid" && [llength $parts] >= 4} {
            lset parts 3 [string tolower [lindex $parts 3]]
        }
        set value [join $parts]
    }
    return $value
}

proc assert_repl_stream_match {pattern value} {
    assert_match [normalize_repl_stream_pattern $pattern] \
        [normalize_repl_stream_value $value]
}

start_server {tags {"gtid" "gtid_6x"} overrides {gtid-enabled yes}} {
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

        set myuuid [status $master gtid_uuid]
        set mygno  [status $master gtid_executed_gno_count]

        assert_replication_stream $master_repl [list "select $::target_db" "gtid $myuuid:$mygno $::target_db SET key val0"]
        assert_replication_stream $slave_repl  [list "select $::target_db" "gtid $myuuid:$mygno $::target_db SET key val0"]

        test "propagte repl: GTID-ENABLED(yes) auto-wrapped alsoPropagate TX maps to one GTID" {
            set sadd_argv [list SADD spop_gtid]
            for {set i 1} {$i <= 3000} {incr i} {
                lappend sadd_argv $i
            }
            $master {*}$sadd_argv
            wait_for_gtid_sync $master $slave

            incr mygno
            assert_replication_stream $master_repl [list "gtid $myuuid:$mygno $::target_db SADD spop_gtid *"]
            assert_replication_stream $slave_repl  [list "gtid $myuuid:$mygno $::target_db SADD spop_gtid *"]

            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]

            $master SPOP spop_gtid 2500
            wait_for_gtid_sync $master $slave

            incr mygno
            assert_equal [$master SCARD spop_gtid] 500
            assert_equal [$slave SCARD spop_gtid] 500

            # 6.x: one SREM per popped member inside MULTI (8.x batches ~3 SREMs).
            foreach repl [list $master_repl $slave_repl] {
                assert_repl_stream_match multi [read_from_replication_stream $repl]
                for {set i 0} {$i < 2500} {incr i} {
                    set cmd [read_from_replication_stream $repl]
                    if {![string match {srem spop_gtid *} $cmd]} {
                        fail "expected srem spop_gtid at index $i before gtid EXEC, got $cmd"
                    }
                }
                assert_repl_stream_match [format {gtid %s:%s %s [Ee][Xx][Ee][Cc]} $myuuid $mygno $::target_db] \
                    [read_from_replication_stream $repl]
            }

            assert_match "*$myuuid:1-$mygno*" [status $master gtid_executed]
            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$mygno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$mygno"
        }

        test "propagte repl: expire" {
            set orig_master_gtidset [status $master gtid_set]
            set orig_slave_gtidset  [status $slave  gtid_set]
            set set_gno [expr {$mygno + 1}]
            set del_gno [expr {$mygno + 2}]

            $master SET key val6 PX 100
            wait_for_condition 100 100 {
                [$master EXISTS key] == 0
            } else {
                fail "key val6 did not expire on master"
            }

            wait_for_gtid_sync $master $slave

            assert_equal [$master EXISTS key] 0
            assert_equal [$slave EXISTS key] 0

            # 6.x replicates PX; 8.x uses PXAT (see gtid_8x.tcl).
            assert_replication_stream $master_repl [list \
                "gtid $myuuid:$set_gno $::target_db SET key val6 PX 100" \
                "gtid $myuuid:$del_gno $::target_db DEL key"]
            assert_replication_stream $slave_repl [list \
                "gtid $myuuid:$set_gno $::target_db SET key val6 PX 100" \
                "gtid $myuuid:$del_gno $::target_db DEL key"]

            assert_equal [lindex [$master GTIDX SEQ LOCATE $orig_master_gtidset] 1] "$myuuid:$set_gno-$del_gno"
            assert_equal [lindex [$slave  GTIDX SEQ LOCATE $orig_slave_gtidset ] 1] "$myuuid:$set_gno-$del_gno"

            incr mygno 2
            assert_match "*$myuuid:1-$mygno*" [status $master gtid_executed]
        }
    }
}

start_server {tags {"gtid" "gtid_6x"} overrides {gtid-enabled yes}} {
    set myuuid [status r gtid_uuid]
    set mygno 0

    tags {memonly} {
        test "multi-exec select db" {
            set repl [attach_to_replication_stream]
            set gno1 [expr {$mygno + 1}]
            set gno2 [expr {$mygno + 2}]
            r set k v
            r select 0
            r set k v

            assert_replication_stream $repl [list \
                "select $::target_db" \
                "gtid $myuuid:$gno1 $::target_db set k v" \
                {select 0} \
                "gtid $myuuid:$gno2 0 set k v"]
            incr mygno 2
            r select $::target_db
        }

        test "multi-exec select db" {
            set repl [attach_to_replication_stream]
            set exec_gno [expr {$mygno + 1}]
            set post_gno [expr {$mygno + 2}]
            r multi
            r set k v
            r select 0
            r set k v
            r exec
            r set k v1

            assert_replication_stream $repl [list \
                "select $::target_db" \
                {multi} \
                {set k v} \
                {select 0} \
                {set k v} \
                [format {gtid %s:%s %s [Ee][Xx][Ee][Cc]} $myuuid $exec_gno $::target_db] \
                "gtid $myuuid:$post_gno 0 set k v1"]
            incr mygno 2
        }
    }
}

start_server {tags {"repl" "gtid_6x"} overrides} {
    set master [srv 0 client]
    $master config set repl-diskless-sync-delay 1
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set gtid-enabled yes
    set myuuid [status $master gtid_uuid]
    set repl [attach_to_replication_stream]
    start_server {tags {"slave"}} {
        set slave [srv 0 client]
        $slave slaveof $master_host $master_port
        wait_for_sync $slave
        set exec_gno [expr {[status $master gtid_executed_gno_count] + 1}]
        $master multi
        $master select 1
        $master select 2
        $master set k v
        $master select 3
        $master set k v1
        $master exec

        assert_replication_stream $repl [list \
            {select 2} \
            {multi} \
            {set k v} \
            {select 3} \
            {set k v1} \
            [format {gtid %s:%s 2 [Ee][Xx][Ee][Cc]} $myuuid $exec_gno]]

        after 1000
        assert_equal [$slave get k] {}
        $slave select 2
        assert_equal [$slave get k] v
        $slave select 3
        assert_equal [$slave get k] v1
    }
}

start_server {tags {"repl" "gtid_6x"} overrides} {
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
            set myuuid [status $master gtid_uuid]
            $master select 0
            $master set gtid-cross-db-seed seed
            wait_for_gtid_sync $master $slave
            set repl [attach_to_replication_stream]
            set exec_gno [expr {[status $master gtid_executed_gno_count] + 1}]

            $master select 9
            $master multi
            $master select 0
            $master set gtid-cross-db-key:1 value-in-db0
            $master set gtid-cross-db-key:2 value-in-db0
            $master exec

            assert_replication_stream $repl [list \
                {multi} \
                {set gtid-cross-db-key:1 value-in-db0} \
                {set gtid-cross-db-key:2 value-in-db0} \
                [format {gtid %s:%s 0 [Ee][Xx][Ee][Cc]} $myuuid $exec_gno]]

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
