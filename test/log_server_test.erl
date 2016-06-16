
-module(log_server_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("layout.hrl").

-define(M, log_server).

smoke_test() ->
    SUT = log_a,
    Layout1 = #layout{epoch=1, upi=[a], repairing=[]},
    Val1 = <<"First!">>,
    {ok, _Pid} = ?M:start_link(SUT, 1, Layout1, [{1, Val1}]),

    {ok, 1, Layout1} = ?M:get_layout(SUT),

    Layout2 = #layout{epoch=2, upi=[], repairing=[]},
    ok = ?M:set_layout(SUT, 2, Layout2),
    {ok, 2, Layout2} = ?M:get_layout(SUT),

    {bad_epoch, 2} = ?M:set_layout(SUT, 2, #layout{epoch=2}),
    {bad_epoch, 2} = ?M:set_layout(SUT, 1, #layout{epoch=1}),

    Blob = <<"Hello, world!">>,
    Is = [2, 3, 4],
    [ok = ?M:write(SUT, 2, I, Blob) || I <- Is],
    {ok, Val1} = ?M:read(SUT, 2, 1),
    [{ok, Blob} = ?M:read(SUT, 2, I) || I <- Is],

    written     = ?M:write(SUT, 2, 1, <<"try to clobber">>),
    not_written = ?M:read( SUT, 2, 99),

    %% Force a wedge and watch the reactions
    wedged = ?M:read(SUT, 3, 1),
    wedged = ?M:write(SUT, 4, 99, <<"bad epoch at unwritten index">>),
    ok             = ?M:set_layout(SUT, 5, #layout{epoch=5}),
    {bad_epoch, 5} = ?M:set_layout(SUT, 5, #layout{epoch=5}),
    {ok, Val1} = ?M:read(SUT, 5, 1),

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
