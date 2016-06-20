
-module(log_client).
-export([read/2, read_repair/2, write/3]).
-include("layout.hrl").

-define(LAYOUT_SERVER, layout_server). % Naming convention clarity.
-define(MAX_LAYOUTS, 2).               % Max # of layout churn loops

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
    WriteLogs = UPI ++ Repairing,
    write(Idx, Val, WriteLogs, ['unused!'], [], ?MAX_LAYOUTS, false, Layout).

write(_Idx, _Val, [], _Repairing, _Done, _MaxLayouts, _Repair_p, Layout) ->
    {ok, Layout};
write(_Idx, _Val, _L, _R, _Done, 0 = _MaxLayouts, _Repair_p, Layout) ->
    %% An infinite loop really irritates Concuerror.  Break out, check
    %% for sanity in some other way.
    {starved, Layout};
write(Idx, Val, [Log|Rest], Repairing, Done, MaxLayouts, Repair_p,
      #layout{epoch=Epoch} = Layout) ->
    %% TODO: Change API for magic write to avoid verbosity here.
    W = if Repair_p ->
                fun() ->
                        log_server:write(Log, Epoch, Idx, Val, magic_repair_abracadabra)
                end;
           true ->
                fun() ->
                        log_server:write(Log, Epoch, Idx, Val)
                end
        end,
    case W() of
        ok ->
            %% We unconditionally change the value of Repair_p for all
            %% subsequent writes: if we are doing read repair, then we
            %% have just encountered our first unwritten value.  To to
            %% help expose bugs, we want any write further down the chain
            %% for this read-repair to fail if 'written' is found.
            write(Idx, Val, Rest, Repairing, [Log|Done],
                  MaxLayouts, false, Layout);
        written ->
            if Done == [] ->
                    {written, Layout};
               true ->
                    case log_server:read(Log, Epoch, Idx) of
                        {ok, WrittenVal} when WrittenVal == Val ->
                            write(Idx, Val, Rest, Repairing, [Log|Done],
                                  MaxLayouts, Repair_p, Layout);
                        {ok, _Else} ->
                            if Done == [] ->
                                    {written, Layout};
                               true ->
                                    {todo_fixme, Layout}
                            end;
                        wedged ->
                            new_layout_retry_write(Idx, Val, Done,
                                                   MaxLayouts, Repair_p);
                        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
                            new_layout_retry_write(Idx, Val, Done,
                                                   MaxLayouts, Repair_p)
                    end
            end;
        wedged ->
            new_layout_retry_write(Idx, Val, Done, MaxLayouts, Repair_p);
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            new_layout_retry_write(Idx, Val, Done, MaxLayouts, Repair_p)
    end.

new_layout_retry_write(Idx, Val, Done, MaxLayouts, Repair_p) ->
    {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
    WriteLogs = NewLayout#layout.upi ++ NewLayout#layout.repairing,
    write(Idx, Val, WriteLogs, ['unused!'],
          Done, MaxLayouts - 1, Repair_p, NewLayout).

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
                    write(Idx, Val, Rest, Repairing, [Head],
                          ?MAX_LAYOUTS, true, Layout)
            end
    end.
