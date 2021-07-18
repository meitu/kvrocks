start_server {tags {"command"}} {
    test {kvrocks has 160 commands currently} {
        r command count
    } {160}

    test {acquire GET command info by COMMAND INFO} {
        set e [lindex [r command info get] 0]
        assert_equal [llength $e] 6
        assert_equal [lindex $e 0] get
        assert_equal [lindex $e 1] 2
        assert_equal [lindex $e 2] {readonly}
        assert_equal [lindex $e 3] 1
        assert_equal [lindex $e 4] 1
        assert_equal [lindex $e 5] 1
    }

    test {COMMAND - command entry length check} {
        set e [lindex [r command] 0]
        assert_equal [llength $e] 6
    }

    test {get keys of commands by COMMAND GETKEYS} {
        assert_equal {test} [r command getkeys get test]
        assert_equal {test test2} [r command getkeys mget test test2]
        assert_equal {test} [r command getkeys zadd test 1 m1]
    }
}
