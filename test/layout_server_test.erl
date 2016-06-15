
-module(layout_server_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(M, layout_server).

smoke_test() ->
    SUT = a,
    {ok, _Pid} = ?M:start_link(SUT, 1, foo),

    {ok, 1, foo} = ?M:get_layout(SUT),

    ok = ?M:set_layout(SUT, 2, bar),
    {ok, 2, bar} = ?M:get_layout(SUT),

    bad_epoch = ?M:set_layout(SUT, 2, bar),
    bad_epoch = ?M:set_layout(SUT, 2, yo_dawg),
    bad_epoch = ?M:set_layout(SUT, 1, bar),

    ok = ?M:stop(SUT),
    try
        ?M:stop(SUT),
        exit(should_have_failed)
    catch
        _:_ ->
            ok
    end,

    ok.

-endif. % TEST
