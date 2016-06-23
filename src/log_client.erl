
-module(log_client).
-export([read/2,
         %% %% TODO: broken by chain repair @ length=1
         %% read_repair/2,
         write/3]).
-include("layout.hrl").

-define(LAYOUT_SERVER, layout_server). % Naming convention clarity.
-define(MAX_LAYOUTS, 4).               % Max # of layout churn loops

-define(LOG(X), put(foo, [{X,?LINE}|get(foo)])).

read(Idx, Layout) ->
    read(Idx, ?MAX_LAYOUTS, Layout).

read(_Idx, 0, Layout) ->
    {starved, Layout};
read(_Idx, _MaxLayouts, #layout{upi=[]}) ->
    {error, chain_out_of_service, fixme};
read(Idx, MaxLayouts, #layout{epoch=Epoch, upi=UPI} = Layout) ->
    case log_server:read(lists:last(UPI), Epoch, Idx) of
        {ok, Val} ->
            {{ok, Val}, Layout};
        not_written ->
            {not_written, Layout};
        wedged ->
            {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
            read(Idx, MaxLayouts - 1, NewLayout);
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
            read(Idx, MaxLayouts - 1, NewLayout)
    end.

make_write_order(#layout{upi=UPI, repairing=[]}) ->
    [{L, false} || L <- UPI];
make_write_order(#layout{upi=[_], repairing=[]}) ->
    exit(todo_future_work);
make_write_order(#layout{upi=[Solo], repairing=Repairing}) ->
    [{Solo, true}] ++ [{L, false} || L <- Repairing] ++ [{Solo, false}];
make_write_order(#layout{upi=UPI, repairing=Repairing}) ->
    Tail = lists:last(UPI),
    Prefix = UPI -- [Tail],
    Ls = Prefix ++ Repairing ++ [Tail],
    [{L, false} || L <- Ls].

write(_Idx, _Val, #layout{upi=[]}) ->
    {error, chain_out_of_service, fixme};
write(Idx, Val, Layout) ->
    put(foo, []),
    WriteLogs = make_write_order(Layout),
    ?LOG(Layout),
    write(Idx, Val, WriteLogs, [], ?MAX_LAYOUTS, false, Layout).

write(_Idx, _Val, [], _Done, _MaxLayouts, _Repair_p, Layout) ->
    ?LOG(done),
    {ok, Layout};
write(_Idx, _Val, _L, _Done, 0 = _MaxLayouts, _Repair_p, Layout) ->
    %% An infinite loop really irritates Concuerror.  Break out, check
    %% for sanity in some other way.
    ?LOG(starved),
    {starved, Layout};
write(Idx, Val, [{Log, HdRepSpecial_p}|Rest], Done, MaxLayouts, Repair_p,
      #layout{epoch=Epoch, upi=UPI, repairing=Repairing} = Layout) ->
    %% TODO: Change API for magic write to avoid verbosity here.
    W = case {lists:member(Log, Repairing), HdRepSpecial_p} of
            {true, _} ->
                fun() ->
                        ?LOG({write_repair,Log,Layout,Val}),
                        log_server:write_clobber(Log, Epoch, Idx, Val)
                end;
            {false, false} ->
                fun() ->
                        ?LOG({write,Log,Layout,Val}),
                        log_server:write(Log, Epoch, Idx, Val)
                end;
            {false, true} ->
                fun() ->
                        ?LOG({write,head_repair_special,Log,Layout,Val}),
                        log_server:write_during_repair(Log, Epoch, Idx, Val)
                end
        end,
    case W() of
        ok ->
            ?LOG(ok),
            write(Idx, Val, Rest, [Log|Done],
                  MaxLayouts, false, Layout);
        written ->
            if Done == [] ->
                    ?LOG(written),
                    {written, Layout};
               true ->
                    R = if hd(UPI) == Log, Repairing /= [] ->
                                fun() -> log_server:read_during_repair(
                                           Log, Epoch, Idx)
                                end;
                           true ->
                                fun() -> log_server:read(Log, Epoch, Idx)
                                end
                        end,
                    case R() of
                        {ok, WrittenVal} when WrittenVal == Val ->
                            ?LOG(same),
                            write(Idx, Val, Rest, [Log|Done],
                                  MaxLayouts, Repair_p, Layout);
                        {ok, _Else} ->
                            ?LOG({else, _Else}),
                            {{todo_fixme2, log, Log, done, Done, epoch, Layout#layout.epoch, upi, Layout#layout.upi, repairing, Layout#layout.repairing, val, Val, max_l, MaxLayouts, rep, Repair_p, {log_a, catch log_server:read(a, Epoch, Idx)}, {log_b, catch log_server:read(b, Epoch, Idx)}, {log_c, catch log_server:read(c, Epoch, Idx), lists:reverse(get(foo))}}, Layout};
                        wedged ->
                            new_layout_retry_write(Idx, Val, Done,
                                                   MaxLayouts, Repair_p);
                        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
                            new_layout_retry_write(Idx, Val, Done,
                                                   MaxLayouts, Repair_p)
                    end
            end;
        wedged ->
            new_layout_retry_write(Idx, Val, Done,
                                   MaxLayouts, Repair_p);
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            new_layout_retry_write(Idx, Val, Done,
                                    MaxLayouts, Repair_p)
    end.

new_layout_retry_write(Idx, Val, Done, MaxLayouts, Repair_p) ->
    {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
    WriteLogs = make_write_order(NewLayout),
    write(Idx, Val, WriteLogs, Done, MaxLayouts - 1, Repair_p, NewLayout).

%% TODO: broken by chain repair @ length=1
%%
%% read_repair(Idx, #layout{epoch=Epoch,
%%                          upi=[Head|Rest]=UPI} = Layout) ->
%%     case log_server:read(lists:last(UPI), Epoch, Idx) of
%%         {ok, _Val} ->
%%             {ok, Layout};
%%         not_written ->
%%             case log_server:read(Head, Epoch, Idx) of
%%                 not_written ->
%%                     {not_written, Layout};
%%                 {ok, Val} ->
%%                     write(Idx, Val, Rest, [Head], ?MAX_LAYOUTS, true, Layout)
%%             end
%%     end.
