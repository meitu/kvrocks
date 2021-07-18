start_server {tags {"config"} overrides {rename-command "KEYS KEYSNEW"}} {
    test {Rename one command} {
        catch {r KEYS *} e
        assert_error "*invalid command name*" $e
        assert_equal "" [r KEYSNEW *]
    }
}


