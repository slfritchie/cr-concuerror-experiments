
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
        OOS = chain_out_of_service,
        {ok, Val1, LayoutB}    = log_client:read(1, Layout1),

        {ok, Val1, LayoutC}    = log_client:read(1, LayoutB),
        {not_written, LayoutD} = log_client:read(7, LayoutC),

        Val2 = <<"My second page">>,
        {ok, LayoutE}       = log_client:write(2, Val2, LayoutD),
        {ok, Val2, LayoutF} = log_client:read( 2, LayoutE),

        happy
    after
        catch ?M:stop(Pid_a),
        catch layout_server:stop(Pid_layout)
    end,
    ok.

%% /usr/local/src/Concuerror/concuerror --pz ./.eunit -m log_server_test -t conc_write1_test
%% 108 interleavings, no errors

conc_write1_test() ->
    SUT = a,
    Layout1 = #layout{epoch=1, upi=[a], repairing=[]},
    {ok, Pid_a} = ?M:start_link(SUT, 1, Layout1, []),
    %% {ok, Pid_layout} = layout_server:start_link(layout_server, 2, Layout2),

    Val_a = <<"A version">>,
    Val_b = <<"Version B">>,
    Parent = self(),
    F = fun(Name, Idx, Val) ->
                register(Name, self()),
                {ok, _Layout2} = log_client:write(Idx, Val, Layout1),
                Parent ! done
        end,
    try
        Writes = [{client_1, 1, Val_a}, {client_2, 2, Val_b}, {client_3, 3, Val_b}],
        %% Writes = [{client_1, 1, Val_a}, {client_2, 2, Val_b}],
        Pids = [spawn(fun() ->
                              F(Name, Idx, Val)
                      end) || {Name, Idx, Val} <- Writes ],

        [receive done -> ok end || _ <- Pids]
    after
        catch ?M:stop(Pid_a),
        %% catch layout_server:stop(Pid_layout)
        ok
    end,
    ok.
    

-endif. % TEST
