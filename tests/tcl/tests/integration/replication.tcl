start_server {tags {"repl"}} {
    set A [srv 0 client]
    set A_host "localhost"
    set A_port [srv 0 port]
    start_server {} {
        set B [srv 0 client]
        set B_host [srv 0 host]
        set B_port [srv 0 port]

        test {Set instance A as slave of B} {
            $A slaveof $B_host $B_port
            after 1000

            wait_for_condition 50 100 {
                [lindex [$A role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$A info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }
        }
    }
}

start_server {tags {"repl"}} {
    r set mykey foo
    r set mystring a
    r lpush mylist a b c
    r sadd myset a b c
    r hmset myhash a 1 b 2 c 3
    r zadd myzset 1 a 2 b 3 c

    start_server {} {
        test {Second server should have role master at first} {
            # Can't statify partial replication
            populate 100 "" 10
            s role
        } {master}

        test {The role should immediately be changed to "replica"} {
            r slaveof [srv -1 host] [srv -1 port]
            s role
        } {slave}

        after 3000
        test {Sync should have transferred keys from master} {
            after 100
            assert_equal [r -1 get mykey] [r get mykey]
            assert_equal [r -1 get mystring] [r get mystring]
            assert_equal [r -1 lrange mylist 0 -1] [r lrange mylist 0 -1]
            assert_equal [r -1 hgetall myhash] [r hgetall myhash]
            assert_equal [r -1 zrange myzset 0 -1 WITHSCORES] [r ZRANGE myzset 0 -1 WITHSCORES]
            assert_equal [r -1 smembers myset] [r smembers myset]
        }

        test {The link status should be up} {
            s master_link_status
        } {up}

        test {SET on the master should immediately propagate} {
            r -1 set mykey bar

            wait_for_condition 500 100 {
                [r  0 get mykey] eq {bar}
            } else {
                fail "SET on master did not propagated on replica"
            }
        }

        test {FLUSHALL should replicate} {
            r -1 flushall
            after 100
            r -1 dbsize scan
            r 0 dbsize scan
            after 200
            if {$::valgrind} {after 2000}
            list [r -1 dbsize] [r 0 dbsize]
        } {0 0}

        test {ROLE in master reports master with a slave} {
            set res [r -1 role]
            lassign $res role offset slaves
            assert {$role eq {master}}
            assert {$offset > 0}
            assert {[llength $slaves] == 1}
            lassign [lindex $slaves 0] master_host master_port slave_offset
            assert {$slave_offset <= $offset}
        }

        test {ROLE in slave reports slave in connected state} {
            set res [r role]
            lassign $res role master_host master_port slave_state slave_offset
            assert {$role eq {slave}}
            assert {$slave_state eq {connected}}
        }
    }
}

start_server {tags {"repl"}} {
    set A [srv 0 client]
    populate 100 "" 10

    start_server {} {
        set B [srv 0 client]
        populate 100 "" 10

        start_server {} {
            set C [srv 0 client]
            set C_host [srv 0 host]
            set C_port [srv 0 port]
            populate 50 "" 10

            test {Multi slaves full sync with master at the same time} {
                $A slaveof $C_host $C_port
                $B slaveof $C_host $C_port
                after 1000

                # Wait for finishing full replication
                wait_for_condition 500 100 {
                    [string match {*connected*} [$A role]] &&
                    [string match {*connected*} [$B role]]
                } else {
                     fail "Slaves can't sync with master"
                }
                # Only 2 full sync
                assert_equal 2 [s sync_full]
            }
        }
    }
}

start_server {tags {"repl"} overrides {max-replication-mb 1 rocksdb.compression no
                    rocksdb.write_buffer_size 1 rocksdb.target_file_size_base 1}} {
    # Generate multiple sst files
    populate 1024 "" 10240
    r set a b
    r compact
    after 1000
    # Wait for finishing compaction
    wait_for_condition 100 100 {
        [s is_compacting] eq no
    } else {
        fail "Failed to compact DB"
    }

    start_server {} {
        test {resume broken transfer based files} {
            populate 1026 "" 1
            set dir [lindex [r config get dir] 1]

            # Try to transfer some files, because max-replication-mb 1,
            # so maybe more than 5 files are transfered for sleep 5s
            r slaveof [srv -1 host] [srv -1 port]
            after 5000

            # Restart master server, let slave full sync with master again,
            # because slave already recieved some sst files, so we will skip them.
            restart_server -1 true false
            r -1 config set max-replication-mb 0

            wait_for_condition 50 1000 {
                [log_file_matches $dir/kvrocks.INFO "*skip count: 1*"]
            } else {
                fail "Fail to resume broken transfer based files"
            }
            after 1000

            # Slave loads checkpoint successfully
            assert_equal b [r get a]
        }
    }
}

start_server {tags {"repl"}} {
    # Generate multiple sst files
    populate 1024 "" 1
    r set a b
    r compact
    after 1000

    set master_host [srv 0 host]
    set master_port [srv 0 port]
    set dir [lindex [r config get dir] 1]
    set master_log $dir/kvrocks.INFO

    start_server {} {
        populate 1026 "" 1
        set slave1 [srv 0 client]
        start_server {} {
            set slave2 [srv 0 client]
            populate 1026 "" 1

            test {two slaves share one checkpoint for full replication} {
                $slave1 slaveof $master_host $master_port
                $slave2 slaveof $master_host $master_port

                wait_for_condition 500 100 {
                    [log_file_matches $master_log "*Use current existing checkpoint*"]
                } else {
                    fail "Fail to share one checkpoint"
                }
                after 1000
                assert_equal b [$slave1 get a]
                assert_equal b [$slave2 get a]
            }
        }
    }
}
