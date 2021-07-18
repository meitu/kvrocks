start_server {tags {"sorted-int"}} {
    test {basic function of sint} {
        assert_equal 1 [r SIADD mysi 1]
        assert_equal 1 [r SIADD mysi 2]
        assert_equal 0 [r SIADD mysi 2]
        assert_equal 5 [r SIADD mysi 3 4 5 123 245]

        assert_equal 7 [r SICARD mysi]
        assert_equal {245 123 5} [r SIREVRANGE mysi 0 3]
        assert_equal {4 3 2} [r SIREVRANGE mysi 0 3 cursor 5]
        assert_equal {245} [r SIRANGE mysi 0 3 cursor 123]
        assert_equal {1 2 3 4} [r SIRANGEBYVALUE mysi 1 (5]
        assert_equal {5 4 3 2} [r SIREVRANGEBYVALUE mysi 5 (1]
        assert_equal {1 0 1} [r SIEXISTS mysi 1 88 2]
        assert_equal {1} [r SIREM mysi 2]
    }
}
