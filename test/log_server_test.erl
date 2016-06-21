
-module(log_server_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("layout.hrl").

-define(M, log_server).

foo_test() ->
    foo.

log_smoke_test() ->
    SUT = a,
    Layout1 = #layout{epoch=1, upi=[a], repairing=[]},
    Val1 = <<"First!">>,
    {ok, PidA} = ?M:start_link(SUT, 1, Layout1, [{1, Val1}]),

    try
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

        %% Test the repair override flag
        Val10a = <<"first val">>,
        Val10b = <<"second val">>,
        ok           = ?M:write(SUT, 5, 10, Val10a),
        written      = ?M:write(SUT, 5, 10, Val10a),
        {ok, Val10a} = ?M:read( SUT, 5, 10),
        ok           = ?M:write(SUT, 5, 10, Val10b, magic_repair_abracadabra),
        {ok, Val10b} = ?M:read( SUT, 5, 10),
        error        = ?M:write(SUT, 5, 10, Val10b, any_other_atom),

        ok = ?M:stop(SUT),
        try
            ?M:stop(SUT),
            exit(should_have_failed)
        catch
            _:_ ->
                ok
        end
    after
        catch ?M:stop(PidA)
    end,

    ok.

client_smoke_test() ->
    SUT = a,
    Layout1 = #layout{epoch=1, upi=[a], repairing=[]},
    Layout2 = #layout{epoch=2, upi=[a], repairing=[]},
    Val1 = <<"First!">>,
    {ok, Pid_a} = ?M:start_link(SUT, 2, Layout2, [{1, Val1}]),
    {ok, Pid_layout} = layout_server:start_link(layout_server, 2, Layout2),

    try
        {{ok, Val1}, LayoutB}  = log_client:read(1, Layout1),

        {{ok, Val1}, LayoutC}  = log_client:read(1, LayoutB),
        {not_written, LayoutD} = log_client:read(7, LayoutC),

        Val2 = <<"My second page">>,
        {ok, LayoutE}          = log_client:write(2, Val2, LayoutD),
        {{ok, Val2}, _LayoutF} = log_client:read( 2, LayoutE),

        happy
    after
        catch ?M:stop(Pid_a),
        catch layout_server:stop(Pid_layout)
    end,
    ok.

%% /usr/local/src/Concuerror/concuerror --pz ./.eunit -m log_server_test -t conc_write1_test

conc_write1_test() ->
    Layout1 = #layout{epoch=1, upi=[a],   repairing=[]}, % TODO repairing=[b]
    Layout2 = #layout{epoch=2, upi=[a,b], repairing=[]},
    {ok, Pid_a} = ?M:start_link(a, 1, Layout1, []),
    {ok, Pid_b} = ?M:start_link(b, 1, Layout1, []),
    Logs = [a, b],
    {ok, Pid_layout} = layout_server:start_link(layout_server, 1, Layout1),

    Val_a = <<"A version">>,
    Val_b = <<"Version B">>,
    Parent = self(),
    F = fun(Name, Idx, Val) ->
                register(Name, self()),
                {Res, _Layout2} = log_client:write(Idx, Val, Layout1),
                Parent ! {done, self(), Res}
        end,
    try
        Writes = [{client_1, 1, Val_a}, {client_2, 1, Val_b}],

        W_pids = [spawn(fun() ->
                                F(Name, Idx, Val)
                        end) || {Name, Idx, Val} <- Writes ],
        L_pid = spawn(fun() ->
                              ok = layout_server:write(layout_server,2,Layout2),
                              [ok = ?M:set_layout(Log, 2, Layout2) ||
                                  Log <- Logs],
                              Parent ! {done, self(), ok}
                      end),

        W_results = [receive
                         {done, Pid, Res} ->
                             Res
                     end || Pid <- W_pids],
        L_result = receive {done, L_pid, Res} -> Res end,

        %% Sanity checking
        W_expected = [ok, written, starved],    % any # of these is ok
        [] = lists:usort(W_results) -- W_expected, % nothing unexpected
        ok = L_result,

        %% The system under test is stable.  If we do a read-repair of
        %% all indexes, and then we issue a blanket read to all log
        %% servers at all indexes.  For each index, the result of
        %% {ok,V} or not_written must all be equal.
        %%
        Idxs = lists:usort([Idx || {_Log, Idx, _Val} <- Writes]),
        [{ok, _LO} = log_client:read_repair(Idx, Layout2) || Idx <- Idxs],

        [begin
             R_res = [log_server:read(Log, 2, Idx) || Log <- Logs],
             case lists:usort(R_res) of
                 [not_written] ->          ok;
                 [{ok, _Unanimous_val}] -> ok
             end
         end || Idx <- Idxs],

        ok
    after
        catch ?M:stop(Pid_a),
        catch ?M:stop(Pid_b),
        catch layout_server:stop(Pid_layout),
        ok
    end,
    ok.

%% OK, let's set this up using the scenario that I'd put on the
%% whiteboard earlier this week.  Chain starts at length 2:
%%
%%    Head a = Val_a, Tail b = unwritten
%%
%% Next chain config:
%%
%%     Head-of-heads c (repairing), Head a, Tail b
%%
%% Final chain config:
%%
%%     Head c, Middle a, Tail b
%%
%% Let's try the "try 5" version of the repair scheme.

conc_write_repair3_test() ->
    exit(todo).

-endif. % TEST
