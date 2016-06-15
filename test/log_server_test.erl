
-module(log_server_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("layout.hrl").

-define(M, log_server).

smoke_test() ->
    SUT = log_a,
    Layout1 = #layout{epoch=1, upi=[a], repairing=[]},
    {ok, _Pid} = ?M:start_link(SUT, 1, Layout1),

    {ok, 1, Layout1} = ?M:get_layout(SUT),

    Layout2 = #layout{epoch=2, upi=[], repairing=[]},
    ok = ?M:set_layout(SUT, 2, Layout2),
    {ok, 2, Layout2} = ?M:get_layout(SUT),

    bad_epoch = ?M:set_layout(SUT, 2, #layout{epoch=2}),
    bad_epoch = ?M:set_layout(SUT, 1, #layout{epoch=1}),

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
