
-module(log_client).
-export([read/2, write/3]).
-include("layout.hrl").

-define(LAYOUT_SERVER, layout_server). % Naming convention clarity.

read(_Idx, #layout{upi=[]}) ->
    {error, chain_out_of_service, fixme};
read(Idx, #layout{epoch=Epoch, upi=UPI} = Layout) ->
    case log_server:read(lists:last(UPI), Epoch, Idx) of
        {ok, Val} ->
            {ok, Val, Layout};
        not_written ->
            {not_written, Layout};
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
            read(Idx, NewLayout)
    end.

write(_Idx, _Val, #layout{upi=[]}) ->
    {error, chain_out_of_service, fixme};
write(Idx, Val, #layout{upi=UPI, repairing=Repairing} = Layout) ->
    write(Idx, Val, UPI, Repairing, [], 3, Layout).

%% TODO: we assume Repairing list is always [].  FIXME.
write(_Idx, _Val, [], [], _Done, _MaxLayouts, Layout) ->
    {ok, Layout};
write(_Idx, _Val, _L, _R, _Done, 0 = _MaxLayouts, Layout) ->
    %% An infinite loop really irritates Concuerror.  Break out, check
    %% for sanity in some other way.
    {starved, Layout};
write(Idx, Val, [Log|Rest], Repairing, Done, MaxLayouts,
      #layout{epoch=Epoch} = Layout) ->
    case log_server:write(Log, Epoch, Idx, Val) of
        ok ->
            write(Idx, Val, Rest, Repairing, [Log|Done], MaxLayouts, Layout);
        written ->
            if Done == [] ->
                    {written, Layout};
               true ->
                    {derp_partial_write_wtf, Layout}
            end;
        wedged ->
            new_layout_retry_write(Idx, Val, Done, MaxLayouts);
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            new_layout_retry_write(Idx, Val, Done, MaxLayouts)
    end.

new_layout_retry_write(Idx, Val, Done, MaxLayouts) ->
    {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
    write(Idx, Val,
          NewLayout#layout.upi -- Done,
          NewLayout#layout.repairing -- Done,
          Done, MaxLayouts - 1, NewLayout).
