
-module(log_client).
-export([read/2]).
-include("layout.hrl").

-define(LAYOUT_SERVER, layout_server). % Naming convention clarity.

read(_Idx, #layout{upi=[]}) ->
    {error, chain_out_of_service};
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
