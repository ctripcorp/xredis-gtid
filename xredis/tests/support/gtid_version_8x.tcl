
proc aof_get_defaults {} {
    return [list \
        appendonly yes \
        appendfilename appendonly.aof \
        appenddirname appendonlydir \
        auto-aof-rewrite-percentage 0 \
        gtid-enabled yes \
    ]
}

proc aof_get_path {dir basename} {
    return "$dir/appendonlydir/${basename}.1.incr.aof"
}


proc aof_create_file {filepath code} {
    upvar 1 fp fp
    file mkdir [file dirname $filepath]
    set fp [open $filepath w+]
    uplevel 1 $code
    close $fp
}

proc aof_append {str} {
    upvar fp fp
    puts -nonewline $fp $str
}

proc aof_is_alive {srv} {
    return [is_alive [dict get $srv pid]]
}

proc aof_skip_in_swap {} {
    return 1
}

# 8.x 版本中会做 argv rewrite 的命令列表（gtid 模式下应拒绝）
proc gtid_rewrite_cmd_list {} {
    return {
        setex
        psetex
        getset
        expire
        pexpire
        expireat
        hexpire
        hpexpire
        hexpireat
        hsetex
        blmove
        brpoplpush
        blpop
        brpop
        bzpopmin
        bzpopmax
        geoadd
    }
}

# 在 replication 流中匹配一条带过期时间的 SET 命令。
# 6.x 保留 PX 字面量，8.x 在 propagate 时改写为 PXAT + 绝对时间戳。
# 输入：key、val、px 字面量（PX ms），输出：可用于 assert_replication_stream 的
# 完整命令 list（已含通配符）。
proc gtid_set_px_match {uuid dbid key val px} {
    return "gtid $uuid $dbid SET $key $val PXAT *"
}

# 在 replication 流中匹配 GTID 包装的 EXEC。
# exec_db 是事务执行时的数据库 id（用于 gtid ... exec_db EXEC）。
proc gtid_exec_match {exec_db} {
    return "gtid * $exec_db EXEC"
}


# 构造跨 DB MULTI 事务
# 8.x: {multi} {select X} {body} {gtid X EXEC}  ← multi 在 select 前
#
# 参数：
#   select_db     multi 体内选定的目标数据库 id（用于构造 select 行）
#   body_cmds     multi 体中所有命令的 list（每条已是 {cmd args...} 形式）
#   exec_db       EXEC 行中的 dbid
# 返回：完整的 patterns 列表，可直接传入 assert_replication_stream。
proc gtid_multi_select_patterns {select_db exec_db body_cmds} {
    # 8.x: multi 在 select 前（GTID 适配层通过 dbid=-1 抑制 SELECT，
    #       由 body 中第一条有 dbid 的命令自然带出）
    set patterns [list {multi} [list select $select_db]]
    foreach cmd $body_cmds {
        lappend patterns $cmd
    }
    lappend patterns [gtid_exec_match $exec_db]
    return $patterns
}


proc gtid_multi_one_command_pattern {args} {
    # 6.x/8.x 中单命令 multi 被 GTID 层展开为单条 gtid 命令（去掉 multi/exec），
    # 返回 "gtid * * cmd args..." 作为一个 pattern 字符串
    return [list "gtid * * $args"]
}

# 通过 replicaof 127.0.0.1 0 断开复制，统一调用方式。
# 8.x 版本断开后不检查 master_link_status（8.x 不会断开 slave 连接）。
# client: 执行 replicaof 的客户端
# delay_ms: 断开后的等待时间（毫秒），默认 100
proc replicaof_disconnect {client {delay_ms 100}} {
    $client replicaof 127.0.0.1 0
    after $delay_ms
}
