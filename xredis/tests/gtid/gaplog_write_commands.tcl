

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-001: String commands - SET/GETEX/GETDEL/APPEND/SETRANGE/INCR/DECR" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            $S set s_str_key1 s_val1
            # SETEX (1)
            $S setex s_str_key2 3600 s_val2
            # PSETEX (1)
            $S psetex s_str_key3 3600000 s_val3
            # SETNX (1) 
            $S setnx s_str_key4 s_val4
            # SET (1) + APPEND (1)
            $S set s_str_key5 "hello"
            $S append s_str_key5 " world"
            # SET (1) + SETRANGE (1)
            $S set s_str_key6 "hello world"
            $S setrange s_str_key6 6 "Redis"
            # SET (1) + INCR (1) + DECR (1) + INCRBY (1) + DECRBY (1) + INCRBYFLOAT (1)
            $S set s_counter 10
            $S incr s_counter
            $S decr s_counter
            $S incrby s_counter 5
            $S decrby s_counter 3
            $S incrbyfloat s_counter 2.5
            # GETSET (1)
            $S getset s_getset_key new_val

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
            assert_equal $gaplog_len 15 "Expected exactly 15 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_str_key1*" $result
            assert_match "*s_str_key2*" $result
            assert_match "*s_str_key3*" $result
            assert_match "*s_str_key4*" $result
            assert_match "*s_str_key5*" $result
            assert_match "*s_str_key6*" $result
            assert_match "*s_counter*" $result
            assert_match "*s_getset_key*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-002: List commands - LPUSH/RPUSH/LPOP/RPOP/LSET/LTRIM/LREM" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # LPUSH (1) + RPUSH (1)
            $S lpush s_list_key1 a b c
            $S rpush s_list_key1 d e f
            # LPUSHX (1) + RPUSHX (1)
            $S lpushx s_list_key1 x
            $S rpushx s_list_key1 y
            # LPOP (1) + RPOP (1)
            $S lpop s_list_key1
            $S rpop s_list_key1
            # LSET (1)
            $S lset s_list_key1 0 new_val
            # LTRIM (1)
            $S ltrim s_list_key1 0 2
            # LREM (1)
            $S lrem s_list_key1 1 a
            # LINSERT (1)
            $S linsert s_list_key1 before b inserted
            # RPOPLPUSH (1)
            $S rpoplpush s_list_key1 s_list_key2
            # LMOVE (1)
            $S lmove s_list_key1 s_list_key2 LEFT RIGHT

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
            assert_equal $gaplog_len 12 "Expected exactly 12 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_list_key1*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-003: Set commands - SADD/SREM/SMOVE/SPOP/SINTERSTORE/SUNIONSTORE/SDIFFSTORE" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # SADD (1)
            $S sadd s_set_key1 a b c d e
            # SREM (1)
            $S srem s_set_key1 a b
            # SMOVE (1)
            $S smove s_set_key1 s_set_key2 c
            # SPOP (1)
            $S spop s_set_key1
            # SADD (1)
            $S sadd s_set_key3 x y z
            # SINTERSTORE (1)
            $S sinterstore s_set_result s_set_key1 s_set_key3
            # SUNIONSTORE (1)
            $S sunionstore s_set_union s_set_key1 s_set_key3
            # SDIFFSTORE (1)
            $S sdiffstore s_set_diff s_set_key1 s_set_key3

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
            assert_equal $gaplog_len 7 "Expected exactly 7 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_set_key1*" $result
            assert_match "*a*" $result
            assert_match "*b*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-004: Sorted Set commands - ZADD/ZREM/ZINCRBY/ZPOPMIN/ZPOPMAX/ZREMRANGEBY*" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # ZADD (1)
            $S zadd s_zset_key1 1 a 2 b 3 c 4 d 5 e
            # ZINCRBY (1)
            $S zincrby s_zset_key1 2.5 a
            # ZREM (1)
            $S zrem s_zset_key1 a
            # ZREMRANGEBYSCORE (1)
            $S zremrangebyscore s_zset_key1 2 3
            # ZREMRANGEBYRANK (1)
            $S zremrangebyrank s_zset_key1 0 0
            # ZADD (1) + ZREMRANGEBYLEX (1)
            $S zadd s_zset_key2 0 a 0 b 0 c 0 d
            $S zremrangebylex s_zset_key2 (b (c
            # ZPOPMIN (1) + ZPOPMAX (1)
            $S zpopmin s_zset_key1
            $S zpopmax s_zset_key1
            # ZADD (1) + ZUNIONSTORE (1) + ZINTERSTORE (1) + ZDIFFSTORE (1)
            $S zadd s_zset_key3 1 x 2 y 3 z
            $S zunionstore s_zset_union 2 s_zset_key1 s_zset_key3
            $S zinterstore s_zset_inter 2 s_zset_key1 s_zset_key3
            $S zdiffstore s_zset_diff 2 s_zset_key1 s_zset_key3
            # ZADD (1) + ZRANGESTORE (1)
            $S zadd s_zset_key4 1 a 2 b 3 c
            $S zrangestore s_zset_range s_zset_key4 0 -1

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
            assert_equal $gaplog_len 11 "Expected exactly 11 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_zset_key1*" $result
            assert_match "*s_zset_key2*" $result
            assert_match "*s_zset_union*" $result
            assert_match "*a*" $result
            assert_match "*b*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-005: Hash commands - HSET/HSETNX/HDEL/HINCRBY/HINCRBYFLOAT" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # HSET (1)
            $S hset s_hash_key1 field1 val1 field2 val2 field3 val3
            # HSETNX (1)
            $S hsetnx s_hash_key1 field4 val4
            # HINCRBY (1)
            $S hincrby s_hash_key1 counter 1
            # HINCRBYFLOAT (1)
            $S hincrbyfloat s_hash_key1 counter 2.5
            # HDEL (1)
            $S hdel s_hash_key1 field1

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
            assert_equal $gaplog_len 5 "Expected exactly 5 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_hash_key1*" $result
            assert_match "*field1*" $result
            assert_match "*field2*" $result
            assert_match "*counter*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-006: Bitmap commands - SETBIT/BITFIELD/BITOP" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # SETBIT
            $S setbit s_bitmap_key 0 1
            $S setbit s_bitmap_key 1 1
            # BITFIELD
            $S bitfield s_bitmap_key2 set u8 0 100 set u8 8 200
            # BITOP
            $S set s_bit1 "\xff"
            $S set s_bit2 "\x0f"
            $S bitop and s_bit_result s_bit1 s_bit2

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
            assert_equal $gaplog_len 6 "Expected exactly 6 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_bitmap_key*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-007: Keyspace commands - DEL/UNLINK/RENAME/RENAMENX/COPY/MOVE/EXPIRE/PEXPIRE" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # DEL
            $S set s_del_key val
            $S del s_del_key
            # UNLINK
            $S set s_unlink_key val
            $S unlink s_unlink_key
            # RENAME
            $S set s_rename_src val
            $S rename s_rename_src s_rename_dst
            # RENAMENX
            $S set s_renamenx_src val
            $S set s_renamenx_dst existing
            $S renamenx s_renamenx_src s_renamenx_dst
            # COPY
            $S set s_copy_src val
            $S copy s_copy_src s_copy_dst
            # EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT
            $S set s_expire_key val
            $S expire s_expire_key 3600
            $S pexpire s_expire_key 3600000
            $S expireat s_expire_key [expr [clock seconds] + 3600]
            $S pexpireat s_expire_key [expr [clock milliseconds] + 3600000]
            # PERSIST
            $S persist s_expire_key

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
            assert_equal $gaplog_len 16 "Expected exactly 16 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_del_key*" $result
            assert_match "*s_rename_src*" $result
            assert_match "*s_copy_src*" $result
            assert_match "*s_expire_key*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-008: HyperLogLog commands - PFADD/PFMERGE" {
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # PFADD
            $S pfadd s_hll_key1 a b c d e
            # PFMERGE
            $S pfadd s_hll_key2 x y z
            $S pfmerge s_hll_result s_hll_key1 s_hll_key2

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
            assert_equal $gaplog_len 3 "Expected exactly 3 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_hll_key1*" $result
            assert_match "*s_hll_result*" $result
        }
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-009: Geo commands - GEOADD/GEOSEARCHSTORE" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # GEOADD
            $S geoadd s_geo_key 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
            # GEOSEARCHSTORE - STORE 
            $S geosearchstore s_geo_result s_geo_key frommember Palermo byradius 200 km

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
            assert_equal $gaplog_len 2 "Expected exactly 2 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_geo_key*" $result
            assert_match "*s_geo_result*" $result
        }
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-010: Stream commands - XADD/XTRIM/XDEL" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # XADD
            set id1 [$S xadd s_stream_key * field1 val1]
            set id2 [$S xadd s_stream_key * field2 val2]
            set id3 [$S xadd s_stream_key * field3 val3]
            # XTRIM
            $S xtrim s_stream_key maxlen 2
            # XDEL
            $S xdel s_stream_key $id3

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
            assert_equal $gaplog_len 5 "Expected exactly 5 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_stream_key*" $result
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-WRITE-011: Multi-key commands - MSET/MSETNX" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            # MSET
            $S mset s_mkey1 val1 s_mkey2 val2 s_mkey3 val3
            # MSETNX
            $S msetnx s_mkey4 val4 s_mkey5 val5

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
            assert_equal $gaplog_len 2 "Expected exactly 2 gaplog entries"

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $gaplog_len]
            assert_match "*s_mkey1*" $result
            assert_match "*s_mkey2*" $result
            assert_match "*s_mkey3*" $result
        }
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-WRITE-012: 300 SET xcontinue" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 300} {incr i} { $S set "mk_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            assert_equal [gaploglen $S] 300; assert_equal [$S get mk_1] v1; assert_equal [$S get mk_300] v300
            assert_equal [$S get m_b] m_v
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-WRITE-013: SET 3 dbs" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S select 0
            for {set i 1} {$i <= 5} {incr i} { $S set "d0_${i}" "v${i}" }
            $S select 1
            for {set i 1} {$i <= 5} {incr i} { $S set "d1_${i}" "v${i}" }
            $S select 2
            for {set i 1} {$i <= 5} {incr i} { $S set "d2_${i}" "v${i}" }
            replicaof_xcontinue $S $Mh $Mp
            assert_equal [gaploglen $S] 15
            set d {}
            for {set i 0} {$i < 15} {incr i} {
                set e [lindex [$S GTIDX GAPLOG LIST $i 1] 0]
                foreach k [lindex $e 2] { lappend d [lindex $k 0] }
            }
            assert_equal [llength $d] 15
            assert_equal [llength [lsearch -all -integer $d 0]] 5
            assert_equal [llength [lsearch -all -integer $d 1]] 5
            assert_equal [llength [lsearch -all -integer $d 2]] 5
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 200}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 200}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CAPACITY-001: trim at maxgap=200" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 150} {incr i} { $S set "c1_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            assert_equal [gaploglen $S] 150
            assert {[llength [$S GTIDX GAPLOG RANGE $su 1 50]] > 0}
        }
    }
}
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 50}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 50}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-CAPACITY-002: trim+LIST" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 100} {incr i} { $S set "c2_${i}" "v${i}" }
            set su [get_uuid $S]
            $S replicaof $Mh $Mp; wait_for_sync $S; after 500
            set gl [gaploglen $S]; assert {$gl <= 50}
            set pg -1
            for {set i 0} {$i < $gl} {incr i} {
                set g [lindex [lindex [$S GTIDX GAPLOG LIST $i 1] 0] 1]
                assert {$g > $pg}; set pg $g
            }
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-WRITE-014: MSETNX conflict" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            assert_equal [$S msetnx nx1 v1 nx2 v2 nx3 v3] 1
            assert_equal [$S msetnx nx1 cx nx4 v4 nx5 v5] 0
            assert_equal [$S get nx1] v1; assert_equal [$S exists nx4] 0
            replicaof_xcontinue $S $Mh $Mp
            assert {[gaploglen $S] >= 1 && [gaploglen $S] <= 2}
            assert_equal [$S get nx1] v1; assert_equal [$S exists nx4] 0
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-WRITE-015: EVAL multi-key" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            $S set ew 0
            set lua {
                redis.call("MSET",KEYS[1],ARGV[1],KEYS[2],ARGV[2],KEYS[3],ARGV[3])
                redis.call("DEL",KEYS[4]);redis.call("SET",KEYS[5],ARGV[5])
                return "OK"
            }
            $S EVAL $lua 5 k1 k2 k3 ew k5 aa bb cc dd ee
            replicaof_xcontinue $S $Mh $Mp
            assert {[gaploglen $S] >= 1}
            assert_equal [$S get k1] aa; assert_equal [$S get k5] ee
            assert_equal [$S exists ew] 0
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 200}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 200}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-LIST-010: LIST count=100" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 150} {incr i} { $S set "lb_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            assert_equal [llength [$S GTIDX GAPLOG LIST 0 100]] 100
            catch {$S GTIDX GAPLOG LIST 0 101} e; assert_match "*count must*" $e
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-LIST-011: LIST start=size empty" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            for {set i 1} {$i <= 5} {incr i} { $S set "le_${i}" "v${i}" }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            set l [gaploglen $S]
            assert_equal [$S GTIDX GAPLOG LIST $l 10] {}
            assert_equal [$S GTIDX GAPLOG LIST [expr {$l+100}] 10] {}
            assert_equal [llength [$S GTIDX GAPLOG LIST [expr {$l-1}] 1]] 1
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-KEY-001: special keys" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            set lk "sl_[string repeat x 1000]"
            $S set $lk vl; $S hset eh "" ev
            set bk "sb_\x00\xff\x01\xfe"
            $S set $bk vb
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            assert {[gaploglen $S] >= 3}
            set rs [join [$S GTIDX GAPLOG LIST 0 3] " "]
            assert_match "*sl_xxxxxxxxxx*" $rs; assert_match "*sb_*" $rs; assert_match "*eh*" $rs
            assert_equal [$S get $lk] vl; assert_equal [$S get $bk] vb
            assert_equal [$S hget eh ""] ev
        }
    }
}
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]; set Mh [srv -1 host]; set Mp [srv -1 port]; set S [srv 0 client]
        test "GAPLOG-KEY-002: oversized values" {
            $S replicaof $Mh $Mp; wait_for_sync $S
            $M set m_b m_v; wait_for_ofs_sync $S $M
            $S replicaof no one; after 100
            set bv [string repeat "x" 10240]
            for {set i 1} {$i <= 100} {incr i} { $S set "bv_${i}" $bv }
            set su [get_uuid $S]; replicaof_xcontinue $S $Mh $Mp
            assert_equal [gaploglen $S] 100
            assert_equal [string length [$S get bv_1]] 10240
            assert_equal [string length [$S get bv_100]] 10240
        }
    }
}