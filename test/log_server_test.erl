
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
        ok           = ?M:write_clobber(SUT, 5, 10, Val10b),
        {ok, Val10b} = ?M:read( SUT, 5, 10),

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

split_role_test() ->
    Layout1 = #layout{epoch=1, upi=[a], repairing=[b]},
    Val1 = <<"First">>,
    Val2 = <<"Invalid, not equal to Val1">>,
    {ok, PidA} = ?M:start_link(a, 0, Layout1, []),
    {ok, PidB} = ?M:start_link(b, 0, Layout1, []),
    Logs = [a, b],
    [ok = ?M:set_layout(L, 1, Layout1) || L <- Logs],

    try
        %% Single log unit testing

        not_written = ?M:read(a, 1, 1),
        ok          = ?M:write_during_repair(a, 1, 1, Val1),
        written     = ?M:write_during_repair(a, 1, 1, Val1),

        {ok, Val1}  = ?M:read_during_repair(a, 1, 1),
        not_written = ?M:read(              a, 1, 1),

        ok      = ?M:write(              a, 1, 1, Val1),
        written = ?M:write(              a, 1, 1, Val1),

        ok        = ?M:write_during_repair(a, 1, 2, Val1),
        bad_value = ?M:write(              a, 1, 2, Val2),

        %% Using either read function, value should exist
        {ok, Val1}  = ?M:read_during_repair(a, 1, 1),
        {ok, Val1}  = ?M:read(a, 1, 1),

        %% Let's try a chain.  Does log_client:write() do the right thing?

        Layout4 = #layout{epoch=4, upi=[a], repairing=[b]},
        [ok = log_server:set_layout(L, 4, Layout4) || L <- Logs],

        Val4 = <<"Val written during repair, clobbers earlier partial write">>,
        ok = ?M:write(b, 4, 10, <<"partial write value">>),

        {ok,_} = log_client:write(10, Val4, Layout4),
        [{L, {ok, Val4}} = {L, ?M:read(L, 4, 10)} || L <- [a,b] ],
        {ok, Val4} = ?M:read_during_repair(a, 4, 10),

        ok
    after
        [catch ?M:stop(P) || P <- [PidA, PidB]]
    end.

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

%% Concuerror test case "no repair".  The types of processes involved are:
%%
%%   a). The log_server processes, Pid_a and Pid_b, representing server
%%   a and b, respectively.  They are basic key-value (KV) stores, plus
%%   an additional property that each key is writable only once (i.e., a
%%   write-once register).  These servers are our cluster, with a single
%%   chain configuration for Chain Replication.
%%
%%   b). The layout_server process, which stores the Chain Replication
%%   configuration for our two node cluster.  In the event that a client
%%   discovers that the cluster configuration is changing, the client
%%   will query the layout_server for the updated cluster config.
%%
%%   c). Two client process, client_1 and client_2, whose PIDs are
%%   stored in the W_pids list.  Each process will try to write a single
%%   value to the cluster using the same key, 1, but will store
%%   different values.  Because the log_server KV stores are write-once,
%%   we expect that exactly one client will succeed writing to key=1.
%%
%%   d). The layout-changing process, L_pid.  The cluster is started
%%   with an initial configuration layout of epoch=1 and a Chain
%%   Replication chain of a single server, [a].  L_pid changes the
%%   cluster configuration by first writing a layout with epoch=2, the
%%   (UPI) chain=[a,b] to the layout server and then to each of the
%%   log_server KV stores.
%%
%% While a client process attempts to write to key=1, the cluster layout
%% may or may not change, depending on race timing.  Both clients start
%% using the epoch=1 layout where the chain is only [a].  If the faster
%% client is successful writing to server 'a' before the layout change
%% is finished, then server 'b' will remain unwritten: the cluster
%% config change is slower than the fast client.
%%
%% However, if a client is slow, and the cluster config change is in
%% progress, a server return {bad_epoch,CurrentEpoch} to the client.
%% This may happen any time after the server has processed the
%% set_layout() call, which tells the server to ignore epoch 1 and only
%% provide service to client requests that contain epoch=2.
%%
%% When a client receives {bad_epoch,...}, the client must query the
%% layout_server to get the new cluster config.  The client may then
%% resume its write protocol by resending the write op to the
%% appropriate server and appropriate epoch number.  If a client needs
%% to retry after more than 4 layout changes, it may abort and return
%% 'starved'.  (See the ?MAX_LAYOUTS macro use in log_client.erl.)
%%
%% The faster client should return 'ok'.  The slower client should
%% return 'written', because the faster client won the race to write to
%% the head of the chain.  (We use a trick of Erlang pattern matching
%% and list removal with the '--' operator to detect & crash when
%% clients report two 'ok' or two 'written' responses.)
%%
%% NOTE: The layout that we use for this test is *not* correct for
%%       testing any of the repair techniques that we wish to test.
%%       Specifically, the 'repairing' record field is unused.  Here, we
%%       aren't attempting any real repair.  We will use 'repairing'
%%       field in later tests which try to verify actual repair
%%       algorithms.
%%
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
        %% %% TODO: broken by chain repair @ length=1
        %% Idxs = lists:usort([Idx || {_Log, Idx, _Val} <- Writes]),
        %% [{ok, _LO} = log_client:read_repair(Idx, Layout2) || Idx <- Idxs],
        %% [begin
        %%      R_res = [log_server:read(Log, 2, Idx) || Log <- Logs],
        %%      case lists:usort(R_res) of
        %%          [not_written] ->          ok;
        %%          [{ok, _Unanimous_val}] -> ok
        %%      end
        %%  end || Idx <- Idxs],

        ok
    after
        catch ?M:stop(Pid_a),
        catch ?M:stop(Pid_b),
        catch layout_server:stop(Pid_layout),
        ok
    end,
    ok.

%% Concuerror test case "2 to 3".  The types of processes involved are:
%%
%%   a). The log_server processes, Pid_a, Pid_b, and Pid_c.
%%
%%   b). The layout_server process.
%%
%%   c). Two writer client processes, Wa_pid and Wb_pid.  Both are
%%       attempting to write to key=1 but are using different values.
%%       Both clients start using the epoch=1 layout but will detect and
%%       switch to new layouts if they are slow enough to get caught in
%%       the middle of cluster change.
%%
%%   d). A reader client process, R_pid, which attempts to read key=1
%%       from the cluster twice in a row.  Like the writers, the reader
%%       starts with epoch=1 layout but may switch new layouts if it is
%%       too slow.
%%
%%   e). The layout-changing process, L_pid.  This process is simulating
%%       both the cluster config change and also sync'ing/repairing data
%%       on the server that is being added to the chain.
%%
%%           1). Change layout to epoch=2, UPI chain=[a,b], repairing=[c].
%%           2). Read the value of key=1 directly from server 'b' and
%%           write that value (if it was written) directly to server 'c'.
%%           3). Change layout to epoch=3, UPI chain=[a,c,b], repairing=[].
%%
%% Layout epoch=1 summary:
%%
%%    Head a, Tail b.
%%    Key=1 is unwritten on both a & b.
%%
%% Layout epoch=2 summary:
%%
%%     Head a, Middle/repairing c, Tail b
%%     Key=1 value may be changing
%%
%% Layout epoch=3 summary:
%%
%%     Head a, Middle c, Tail b.
%%     Key=1 is written to some unanimous value on all servers by the end
%%     of the test.

conc_write_repair3_2to3_test() ->
    Val_a = <<"A version">>,
    Val_b = <<"Version B">>,
    Layout1 = #layout{epoch=1, upi=[a,b],   repairing=[]},
    Layout2 = #layout{epoch=2, upi=[a,b],   repairing=[c]},
    Layout3 = #layout{epoch=3, upi=[a,c,b], repairing=[]},
    {ok, _Pid_a} = ?M:start_link(a, 1, Layout1, []),
    {ok, _Pid_b} = ?M:start_link(b, 1, Layout1, []),
    {ok, _Pid_c} = ?M:start_link(c, 1, Layout1, []),
    Logs = [a, b, c],
    {ok, Pid_layout} = layout_server:start_link(layout_server, 1, Layout1),

    Parent = self(),
    try
        Wa_pid = spawn(fun() ->
                              {Res, _} = log_client:write(1, Val_a, Layout1),
                              Parent ! {done, self(), Res}
                        end),
        Wb_pid = spawn(fun() ->
                              {Res, _} = log_client:write(1, Val_b, Layout1),
                              Parent ! {done, self(), Res}
                        end),
        R_pid = spawn(fun() ->
                              {Res1, _} = log_client:read(1, Layout1),
                              {Res2, _} = log_client:read(1, Layout1),
                              Parent ! {done, self(), {Res1,Res2}}
                        end),
        L_pid = spawn(fun() ->
                              ok = layout_server:write(layout_server,2,Layout2),
                              [ok = ?M:set_layout(Log, 2, Layout2) ||
                                  Log <- Logs],

                              %% HACK: hard-code read-repair for this case
                              case ?M:read(b, 2, 1) of
                                  {ok, V_repair} ->
                                      case ?M:write_clobber(c, 2, 1, V_repair) of
                                          ok      -> ok;
                                          written -> ok;
                                          starved -> exit(oi_todo_yo1)
                                      end;
                                  not_written ->
                                      ok;
                                  starved ->
                                      exit(oi_todo_yo2)
                              end,

                              ok = layout_server:write(layout_server,3,Layout3),
                              [ok = ?M:set_layout(Log, 3, Layout3) ||
                                  Log <- Logs],
                              Parent ! {done, self(), ok}
                      end),

        Wa_result = receive
                       {done, Wa_pid, Res_a} ->
                           Res_a
                   end,
        Wb_result = receive
                       {done, Wb_pid, Res_b} ->
                           Res_b
                   end,
        R_result = receive
                       {done, R_pid, Res_y} ->
                           Res_y
                   end,
        L_result = receive {done, L_pid, Res_z} -> Res_z end,

        %% Sanity checking
        true = write_result_is_ok(Wa_result),
        true = write_result_is_ok(Wb_result),
        if Wa_result == ok, Wb_result == ok -> error(write_once_violation);
           true                             -> ok
        end,

        ok = L_result,

        %% TODO: simply by collapsing not_written & starved to same thing.
        case R_result of
            {not_written,not_written} -> ok;
            {starved,    not_written} -> ok;
            {not_written,starved}     -> ok;
            {not_written,{ok,_}}      -> ok;
            {{ok,Same},  {ok,Same}}   -> ok;
            {starved,    {ok,_}}      -> ok;
            {{ok,_},     starved}     -> ok;
            {starved,    starved}     -> ok
        end,

        Idxs = [1],
        [begin
             R_res = [log_server:read(Log, 3, Idx) || Log <- Layout3#layout.upi],
             case R_res of
                 [not_written,not_written,not_written] -> ok;
                 [{ok,_U_val},not_written,not_written] -> ok;
                 [{ok, U_val},{ok, U_val},not_written] -> ok;
                 [{ok, U_val},{ok, U_val},{ok, U_val}] -> ok
             end
         end || Idx <- Idxs],

        ok
    after
        [catch ?M:stop(P) || P <- Logs],
        catch layout_server:stop(Pid_layout),
        ok
    end,
    ok.

%% Concuerror test case "1 to 2".  The types of processes involved are:
%%
%%   a). The log_server processes, Pid_a and Pid_b.
%%
%%   b). The layout_server process.
%%
%%   c). Two writer client processes, Wa_pid and Wb_pid.  Same as the
%%       "2 to 3" test case.
%%
%%   d). A reader client process, R_pid, which attempts to read key=1
%%       from the cluster twice in a row.  Same as the "2 to 3" test
%%       case.
%%
%%   e). The layout-changing process, L_pid.  Similar to the "2 to 3"
%%       test case but not the same.  We wish to put the repairing node
%%       in the middle of a healthy chain.  However, our healthy chain
%%       is only length 1.  See summary below for more detail.

%%
%% Layout epoch=1 summary:
%%
%%    Head a, tail a (the normal Chain Replication state for length=1)
%%    Key=1 is unwritten on a
%%
%% Layout epoch=2 summary:
%%
%%     Head a (special head repair role), Middle/repairing b, Tail a
%%     Key=1 value may be changing
%%
%% We take a novel approach: split server 'a' into two virtual servers
%% with different roles, "head repair role" and "tail role".  The
%% physical store of server a is split into virtual stores, so that
%% write of key=1 to the head repair role's store does *not* affect the
%% tail role's store.
%%
%% Layout epoch=3 summary:
%%
%%     Head a, Tail b
%%     Key=1 is written to some unanimous value on both servers by the end
%%     of the test.

conc_write_repair3_1to2_test() ->
    Val_a = <<"A version">>,
    Val_b = <<"Version B">>,
    Layout1 = #layout{epoch=1, upi=[a],   repairing=[]},
    Layout2 = #layout{epoch=2, upi=[a],   repairing=[b]},
    Layout3 = #layout{epoch=3, upi=[a,b], repairing=[]},
    {ok, _Pid_a} = ?M:start_link(a, 1, Layout1, []),
    {ok, _Pid_b} = ?M:start_link(b, 1, Layout1, []),
    Logs = [a, b],
    {ok, Pid_layout} = layout_server:start_link(layout_server, 1, Layout1),

    Parent = self(),
    try
        Wa_pid = spawn(fun() ->
                              {Res, _} = log_client:write(1, Val_a, Layout1),
                              Parent ! {done, self(), Res}
                        end),
        Wb_pid = spawn(fun() ->
                              {Res, _} = log_client:write(1, Val_b, Layout1),
                              Parent ! {done, self(), Res}
                        end),
        R_pid = spawn(fun() ->
                              {Res1, _} = log_client:read(1, Layout1),
                              {Res2, _} = log_client:read(1, Layout1),
                              Parent ! {done, self(), {Res1,Res2}}
                        end),
        L_pid = spawn(fun() ->
                              ok = layout_server:write(layout_server,2,Layout2),
                              [ok = ?M:set_layout(Log, 2, Layout2) ||
                                  Log <- Logs],

                              %% HACK: hard-code read-repair for this case
                              case ?M:read(a, 2, 1) of
                                  {ok, V_repair} ->
                                      case ?M:write_clobber(b, 2, 1, V_repair) of
                                          ok      -> ok;
                                          written -> ok;
                                          starved -> exit(oi_todo_yo1)
                                      end;
                                  not_written ->
                                      ok;
                                  starved ->
                                      exit(oi_todo_yo2)
                              end,

                              ok = layout_server:write(layout_server,3,Layout3),
                              [ok = ?M:set_layout(Log, 3, Layout3) ||
                                  Log <- Logs],
                              Parent ! {done, self(), ok}
                      end),

        Wa_result = receive
                       {done, Wa_pid, Res_a} ->
                           Res_a
                   end,
        Wb_result = receive
                       {done, Wb_pid, Res_b} ->
                           Res_b
                   end,
        R_result = receive
                       {done, R_pid, Res_y} ->
                           Res_y
                   end,
        L_result = receive {done, L_pid, Res_z} -> Res_z end,

        %% Sanity checking
        true = write_result_is_ok(Wa_result),
        true = write_result_is_ok(Wb_result),
        if Wa_result == ok, Wb_result == ok -> error(write_once_violation);
           true                             -> ok
        end,

        ok = L_result,

        %% TODO: simply by collapsing not_written & starved to same thing.
        case R_result of
            {not_written,not_written} -> ok;
            {starved,    not_written} -> ok;
            {not_written,starved}     -> ok;
            {not_written,{ok,_}}      -> ok;
            {{ok,Same},  {ok,Same}}   -> ok;
            {starved,    {ok,_}}      -> ok;
            {{ok,_},     starved}     -> ok;
            {starved,    starved}     -> ok
        end,

        Idxs = [1],
        [begin
             R_res = [log_server:read(Log, 3, Idx) || Log <- Layout3#layout.upi],
             case R_res of
                 [not_written,not_written] -> ok;
                 [{ok,_U_val},not_written] -> ok;
                 [{ok, U_val},{ok, U_val}] -> ok
             end
         end || Idx <- Idxs],

        ok
    after
        [catch ?M:stop(P) || P <- Logs],
        catch layout_server:stop(Pid_layout),
        ok
    end,
    ok.

write_result_is_ok(Result) ->
    W_expected = [ok, written, starved],    % any # of these is ok
    lists:member(Result, W_expected).

-endif. % TEST
