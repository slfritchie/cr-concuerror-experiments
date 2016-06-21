
-module(log_client).
-export([read/2, read_repair/2, write/3]).
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

%% write_ignore_written(Idx, Val, Done,
%%                      #layout{upi=UPI, repairing=Repairing} = Layout) ->
%%     write(Idx, Val, UPI -- Done, Repairing, Done, 3, true, Layout).

write(_Idx, _Val, #layout{upi=[]}) ->
    {error, chain_out_of_service, fixme};
write(Idx, Val, #layout{upi=UPI, repairing=Repairing} = Layout) ->
    put(foo, []),
    WriteLogs = Repairing ++ UPI,
    ?LOG(Layout),
    write(Idx, Val, WriteLogs, Repairing, [], [], ?MAX_LAYOUTS, false, Layout).

write(_Idx, _Val, [], _Repairing, _Done, _DoneRepairing,
      _MaxLayouts, _Repair_p, Layout) ->
    ?LOG(done),
    {ok, Layout};
write(_Idx, _Val, _L, _R, _Done, _DoneRepairing,
      0 = _MaxLayouts, _Repair_p, Layout) ->
    %% An infinite loop really irritates Concuerror.  Break out, check
    %% for sanity in some other way.
    ?LOG(starved),
    {starved, Layout};
write(Idx, Val, [Log|Rest], Repairing, Done, DoneRepairing,
      MaxLayouts, Repair_p, #layout{epoch=Epoch} = Layout) ->
    %% TODO: Change API for magic write to avoid verbosity here.
    W = if Repair_p ->
                fun() ->
                        ?LOG({write_repair,Layout,Val}),
                        log_server:write(Log, Epoch, Idx, Val, magic_repair_abracadabra)
                end;
           true ->
                fun() ->
                        ?LOG({write,Log,Layout,Val}),
                        log_server:write(Log, Epoch, Idx, Val)
                end
        end,
    DoneRepairing2 = case lists:member(Log, Repairing) of
                         true  -> [Log|DoneRepairing];
                         false -> DoneRepairing
                     end,
    case W() of
        ok ->
            %% We unconditionally change the value of Repair_p for all
            %% subsequent writes: if we are doing read repair, then we
            %% have just encountered our first unwritten value.  To to
            %% help expose bugs, we want any write further down the chain
            %% for this read-repair to fail if 'written' is found.
            ?LOG(ok),
            write(Idx, Val, Rest, Repairing, [Log|Done], DoneRepairing2,
                  MaxLayouts, false, Layout);
        written ->
            if Done == [] ->
                    ?LOG(written),
                    {written, Layout};
               true ->
                    case log_server:read(Log, Epoch, Idx) of
                        {ok, WrittenVal} when WrittenVal == Val ->
                            ?LOG(same),
                            write(Idx, Val, Rest, Repairing, [Log|Done],
                                  DoneRepairing2,
                                  MaxLayouts, Repair_p, Layout);
                        {ok, _Else} ->
                            ?LOG({else, _Else}),
                            AllDoneWereRepairs_p =
                                ordsets:is_subset(
                                  ordsets:from_list(Done),
                                  ordsets:from_list(DoneRepairing)),
                            CurrentIsRepairing_p = lists:member(Log, Repairing),
                            if Done == [] ->
                                    {written, Layout};
                               AllDoneWereRepairs_p ->
                                    %% This is the case where the head-of-heads
                                    %% (node under repair) has been written
                                    %% successfully a race in prior layout had
                                    %% written to the head successfully.
                                    %% We tell this writer that it loses.
                                    {written, Layout};
                               CurrentIsRepairing_p ->
                                    %% Current log unit is under repair.
                                    %% Our write failed because of a partial
                                    %% write is present at this unit.  The
                                    %% repair process fill fix the partial
                                    %% write, so we can safely continue on
                                    %% with the rest of the chain.
                                    write(Idx, Val, Rest, Repairing,
                                          Done, DoneRepairing,
                                          MaxLayouts, Repair_p, Layout);
                               true ->
                                    {starved,Layout}  % TODO Experiment! Just return 'starved' for now to see how the rest of the sanity checks react (replace exception commented below)
                                    %% {{todo_fixme2, log, Log, done, Done, done_repairing, DoneRepairing, epoch, Layout#layout.epoch, upi, Layout#layout.upi, repairing, Layout#layout.repairing, val, Val, max_l, MaxLayouts, rep, Repair_p, {log_a, catch log_server:read(a, Epoch, Idx)}, {log_b, catch log_server:read(b, Epoch, Idx)}, {log_c, catch log_server:read(c, Epoch, Idx), lists:reverse(get(foo))}}, Layout}
                            end;
                        wedged ->
                            new_layout_retry_write(Idx, Val, Done, DoneRepairing,
                                                   MaxLayouts, Repair_p);
                        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
                            new_layout_retry_write(Idx, Val, Done, DoneRepairing,
                                                   MaxLayouts, Repair_p)
                    end
            end;
        wedged ->
            new_layout_retry_write(Idx, Val, Done, DoneRepairing,
                                   MaxLayouts, Repair_p);
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            new_layout_retry_write(Idx, Val, Done, DoneRepairing,
                                    MaxLayouts, Repair_p)
    end.

new_layout_retry_write(Idx, Val, Done, DoneRepairing, MaxLayouts, Repair_p) ->
    {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
    Repairing = NewLayout#layout.repairing,
    WriteLogs = Repairing ++ NewLayout#layout.upi,
    write(Idx, Val, WriteLogs, Repairing,
          Done, DoneRepairing, MaxLayouts - 1, Repair_p, NewLayout).

read_repair(Idx, #layout{epoch=Epoch,
                         upi=[Head|Rest]=UPI, repairing=Repairing} = Layout) ->
    case log_server:read(lists:last(UPI), Epoch, Idx) of
        {ok, _Val} ->
            {ok, Layout};
        not_written ->
            case log_server:read(Head, Epoch, Idx) of
                not_written ->
                    {not_written, Layout};
                {ok, Val} ->
                    write(Idx, Val, Rest, Repairing, [Head], [],
                          ?MAX_LAYOUTS, true, Layout)
            end
    end.
