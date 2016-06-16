
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
    write(Idx, Val, UPI, Repairing, [], Layout).

%% TODO: we assume Repairing list is always [].  FIXME.
write(_Idx, _Val, [], [], _Done, Layout) ->
    {ok, Layout};
write(Idx, Val, [Log|Rest], Repairing, Done, #layout{epoch=Epoch} = Layout) ->
    case log_server:write(Log, Epoch, Idx, Val) of
        ok ->
            write(Idx, Val, Rest, Repairing, [Log|Done], Layout);
        written ->
            {derp_todo_fixme, Layout};
        {bad_epoch, NewEpoch} when Epoch < NewEpoch ->
            {ok, _NewEpoch, NewLayout} = layout_server:read(?LAYOUT_SERVER),
            write(Idx, Val,
                  NewLayout#layout.upi -- Done,
                  NewLayout#layout.repairing -- Done,
                  Done, NewLayout)
    end.
