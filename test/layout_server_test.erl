
-module(layout_server_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(M, layout_server).

smoke_test() ->
    SUT = layout_server,
    {ok, Pid} = ?M:start_link(SUT, 1, foo),

    try
        {ok, 1, foo} = ?M:read(Pid),

        ok = ?M:write(Pid, 2, bar),
        {ok, 2, bar} = ?M:read(Pid),

        bad_epoch = ?M:write(Pid, 2, bar),
        bad_epoch = ?M:write(Pid, 2, yo_dawg),
        bad_epoch = ?M:write(Pid, 1, bar),

        ok = ?M:stop(Pid),
        try
            ?M:stop(Pid),
            exit(should_have_failed)
        catch
            _:_ ->
                ok
        end
    after
        catch ?M:stop(Pid)
    end,

    ok.

-endif. % TEST
