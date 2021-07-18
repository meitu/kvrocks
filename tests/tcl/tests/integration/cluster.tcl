source "tests/helpers/crc16_slottable.tcl"

start_server {tags {"disable-cluster"}} {
    test {can't execute cluster command if disabled} {
        catch {[r cluster nodes]} var
        assert_match "*not enabled*" $var
    }
}

start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
    test {CLUSTER KEYSLOT} {
        set slot_table_len [llength $::CRC16_SLOT_TABLE]
        for {set i 0} {$i < $slot_table_len} {incr i} {
            assert_equal $i [r cluster keyslot [lindex $::CRC16_SLOT_TABLE $i]]
        }
    }
}

start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
    set nodeid "07c37dfeb235213a872192d90877d0cd55635b91"
    r clusterx SETNODEID $nodeid

    test {basic function of cluster} {
        # Cluster is not initialized
        catch {[r cluster nodes]} err
        assert_match "*CLUSTERDOWN*not initialized*" $err

        # Set cluster nodes info
        set port [srv port]
        set nodes_str "$nodeid 127.0.0.1 $port master - 0-100"
        r clusterx setnodes $nodes_str 2
        assert_equal 2 [r clusterx version]

        # Get and check cluster nodes info
        set output_nodes [r cluster nodes]
        set fields [split $output_nodes " "]
        assert_equal 9 [llength $fields]
        assert_equal "127.0.0.1:$port@[expr $port + 10000]" [lindex $fields 1]
        assert_equal "myself,master" [lindex $fields 2]
        assert_equal "0-100\n" [lindex $fields 8]

        # Cluster slot command
        set ret [r cluster slots]
        assert_equal ${ret} "{0 100 {127.0.0.1 ${port} ${nodeid}}}"
    }

    test {cluser topology is reset by old version} {
        # Set cluster nodes info
        set port [srv port]
        set nodes_str "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1 $port master - 0-200"
        r clusterx setnodes $nodes_str 1 force
        assert_equal 1 [r clusterx version]

        set output_nodes [r cluster nodes]
        assert_equal "0-200\n" [lindex [split $output_nodes " "] 8]
    }

    test {errors of cluster subcommand} {
        catch {[r cluster no-subcommand]} err
        assert_match "*CLUSTER*" $err 
 
        catch {[r clusterx version a]} err
        assert_match "*CLUSTER*" $err 

        catch {[r cluster nodes a]} err
        assert_match "*CLUSTER*" $err 

        catch {[r clusterx setnodeid a]} err
        assert_match "*CLUSTER*" $err 

        catch {[r clusterx setnodes a]} err
        assert_match "*CLUSTER*" $err 

        catch {[r clusterx setnodes a -1]} err
        assert_match "*Invalid version*" $err 
    }
}

start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
    test {cluster slots and nodes about complex topology} {
        set nodeid "07c37dfeb235213a872192d90877d0cd55635b91"
        set host [srv host]
        set port [srv port]
        set cluster_nodes "$nodeid $host $port master -"
        set cluster_nodes "${cluster_nodes} 0-1 2 4-8191 8192 8193 10000 10002-11002 16381 16382-16383"
        r clusterx setnodes "$cluster_nodes" 1
        r clusterx setnodeid $nodeid
        set ret [r cluster slots]
        assert_equal 5 [llength $ret]

        set slot_1w [lindex $ret 2]
        assert_equal ${slot_1w} "10000 10000 {${host} ${port} ${nodeid}}"

        set ret [r cluster nodes]
        assert_match "*0-2 4-8193 10000 10002-11002 16381-16383*" $ret
    }
}

start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
    set r0 [srv 0 client]
    set node0_host [srv host]
    set node0_port [srv port]
    set node0_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
    start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
        set r1 [srv 0 client]
        set node1_host [srv host]
        set node1_port [srv port]
        set node1_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
        start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
            set r2 [srv 0 client]
            set node2_host [srv host]
            set node2_port [srv port]
            set node2_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx02"
            start_server {tags {"cluster"} overrides {cluster-enabled yes}} {
                set r3 [srv 0 client]
                set node3_host [srv host]
                set node3_port [srv port]
                set node3_id "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx03"
                $r3 slaveof $node2_host $node2_port

                set slot_0_key      "06S"
                set slot_1_key      "Qi"
                set slot_2_key      "5L5"
                set slot_16382_key  "4oi"
                set slot_16383_key  "6ZJ"

                test {requests on non-init-cluster} {
                    catch {[$r0 set $slot_0_key 0]} err
                    assert_match "*CLUSTERDOWN*not served*" $err

                    catch {[$r2 set $slot_16383_key 16383]} err
                    assert_match "*CLUSTERDOWN*not served*" $err
                }

                set cluster_nodes  "$node1_id $node1_host $node1_port master - 0-1 3 5-8191"
                set cluster_nodes  "$cluster_nodes\n$node2_id $node2_host $node2_port master - 8192-16383"
                set cluster_nodes  "$cluster_nodes\n$node3_id $node3_host $node3_port slave $node2_id"

                # node0 doesn't serve any slot, just like a router
                $r0 clusterx setnodes $cluster_nodes 1
                $r1 clusterx setnodes $cluster_nodes 1
                $r2 clusterx setnodes $cluster_nodes 1
                $r3 clusterx setnodes $cluster_nodes 1

                test {cluster info command} {
                    set ret [$r1 cluster info]
                    assert_match "*cluster_state:ok*" $ret
                    assert_match "*cluster_slots_assigned:16382*" $ret
                    assert_match "*cluster_slots_ok:16382*" $ret
                    assert_match "*cluster_known_nodes:3*" $ret
                    assert_match "*cluster_size:2*" $ret
                    assert_match "*cluster_current_epoch:1*" $ret
                    assert_match "*cluster_my_epoch:1*" $ret
                }

                test {MOVED slot ip:port if needed} {
                    # Request node2 that doesn't serve slot 0, we will recieve MOVED
                    catch {[$r2 set $slot_0_key 0]} err
                    assert_match "*MOVED 0*$node1_port*" $err

                    # Request node3 that doesn't serve slot 0, we will recieve MOVED
                    catch {[$r3 get $slot_0_key]} err
                    assert_match "*MOVED 0*$node1_port*" $err
                    
                    # Request node1 that doesn't serve slot 16383, we will recieve MOVED,
                    # and the MOVED node must be master
                    catch {[$r1 get $slot_16383_key]} err
                    assert_match "*MOVED 16383*$node2_port*" $err
                }

                test {requests on cluster are ok} {
                    # Request node1 that serves slot 0, that's ok
                    assert_equal "OK" [$r1 set $slot_0_key 0]

                    # Request node2 that serve slot 16383, that's ok
                    assert_equal "OK" [$r2 set $slot_16383_key 16383]
                    after 200

                    # Request replicas a write command, it is wrong
                    catch {[$r3 set $slot_16383_key 16384]} err
                    assert_match "*MOVED*" $err

                    # Request a read-only command to node3 that serve slot 16383, that's ok
                    assert_equal "16383" [$r3 get $slot_16383_key]
                }

                test {requests non-member of cluster, role is master} {
                    catch {[$r0 set $slot_0_key 0]} err
                    assert_match "*MOVED 0*$node1_port*" $err

                    catch {[$r0 get $slot_16383_key]} err
                    assert_match "*MOVED 16383*$node2_port*" $err
                }

                test {requests non-member of cluster, role is slave} {
                    $r0 slaveof $node1_host $node1_port
                    after 100
                    catch {[$r0 get $slot_16383_key]} err
                    assert_match "*MOVED 16383*$node2_port*" $err
                }

                test {cluster slot is not served } {
                    catch {[$r1 set $slot_2_key 2]} err
                    assert_match "*CLUSTERDOWN*not served*" $err
                }

                test {multiple keys(cross slots) command is wrong} {
                    catch {[$r1 mset $slot_0_key 0 $slot_1_key 1]} err
                    assert_match "*CROSSSLOT*" $err
                }

                test {multiple keys(the same slots) command is right} {
                    $r1 mset $slot_0_key 0 $slot_0_key 1
                } {OK}

                test {cluster MULTI-exec cross slots and in one node } {
                    $r1 multi
                    $r1 set $slot_0_key 0
                    $r1 set $slot_1_key 0
                    $r1 exec
                } {OK OK}

                test {cluster MULTI-exec cross slots but not in one node } {
                    $r1 set $slot_0_key no-multi
                    $r1 multi
                    $r1 set $slot_0_key multi
                    catch {[$r1 set $slot_16383_key 0]} err
                    assert_match "*MOVED*$node2_port*" $err
                    catch {[$r1 exec]} err
                    assert_match "*EXECABORT*" $err
                    assert_match no-multi [$r1 get $slot_0_key]
                }
            }
        }
    }
}
