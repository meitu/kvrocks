start_server {tags {"introspection"}} {
    test {CLIENT LIST} {
        r client list
    } {id=* addr=*:* fd=* name=* age=* idle=* flags=N namespace=* qbuf=* *obuf=* cmd=client*}

    test {MONITOR can log executed commands} {
        set rd [redis_deferring_client]
        $rd monitor
        assert_match {*OK*} [$rd read]
        r set foo bar
        r get foo
        list [$rd read] [$rd read]
    } {*"set" "foo"*"get" "foo"*}

    test {CLIENT GETNAME should return NIL if name is not assigned} {
        r client getname
    } {}

    test {CLIENT LIST shows empty fields for unassigned names} {
        r client list
    } {*name= *}

    test {CLIENT SETNAME does not accept spaces} {
        catch {r client setname "foo bar"} e
        set e
    } {ERR*}

    test {CLIENT SETNAME can assign a name to this connection} {
        assert_equal [r client setname myname] {OK}
        r client list
    } {*name=myname*}

    test {CLIENT SETNAME can change the name of an existing connection} {
        assert_equal [r client setname someothername] {OK}
        r client list
    } {*name=someothername*}

    test {After CLIENT SETNAME, connection can still be closed} {
        set rd [redis_deferring_client]
        $rd client setname foobar
        assert_equal [$rd read] "OK"
        assert_match {*foobar*} [r client list]
        $rd close
        # Now the client should no longer be listed
        wait_for_condition 50 100 {
            [string match {*foobar*} [r client list]] == 0
        } else {
            fail "Client still listed in CLIENT LIST after SETNAME."
        }
    }

    test {DEBUG will freeze server} {
        set rd [redis_deferring_client]
        $rd DEBUG sleep 2.2
        $rd flush
        after 100

        set start_time [clock seconds]
        r set a b
        set time_elapsed [expr {[clock seconds]-$start_time}]
        assert {$time_elapsed >= 2}
    }
}
