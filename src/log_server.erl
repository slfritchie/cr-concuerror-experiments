
-module(log_server).

-behaviour(gen_server).

-export([start_link/3,
         set_layout/3,                          % used by chain manager thingie
         get_layout/1,                          % unit test/debugging only
         stop/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("layout.hrl").

-record(state, {
          name   :: atom(),
          epoch  :: non_neg_integer(),
          layout :: term()
         }).

start_link(Name, InitialEpoch, InitialLayout)
  when is_integer(InitialEpoch), InitialEpoch >= 0,
       is_record(InitialLayout, layout) ->
    gen_server:start_link({local, Name},
                          ?MODULE, [Name, InitialEpoch, InitialLayout], []).

get_layout(Name) ->
    gen_server:call(Name, get_layout, infinity).

set_layout(Name, Epoch, Layout)
  when is_integer(Epoch), Epoch >= 0,
       is_record(Layout, layout) ->
    gen_server:call(Name, {set_layout, Epoch, Layout}, infinity).

stop(Name) ->
    gen_server:call(Name, stop, infinity).

%%%%%%%%%%%%%%%%%%%%%%

init([Name, InitialEpoch, InitialLayout]) ->
    {ok, #state{name=Name,
                epoch=InitialEpoch,
                layout=InitialLayout}}.

handle_call(get_layout, _From,
            #state{epoch=Epoch, layout=Layout} = S) ->
    {reply, {ok, Epoch, Layout}, S};
handle_call({set_layout, NewEpoch, NewLayout}, _From,
            #state{epoch=Epoch} = S) ->
    if is_integer(NewEpoch), NewEpoch > Epoch ->
            {reply, ok, S#state{epoch=NewEpoch, layout=NewLayout}};
       true ->
            {reply, bad_epoch, S}
    end;
handle_call(stop, _From, S) ->
    {stop, normal, ok, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%
