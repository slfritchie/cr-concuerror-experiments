
-module(layout_server).

-behaviour(gen_server).

%% API
-export([start_link/3, read/1, write/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          name   :: atom(),
          epoch  :: non_neg_integer(),
          layout :: term()
         }).

start_link(Name, InitialEpoch, InitialLayout) when InitialEpoch >= 0 ->
    gen_server:start_link({local, Name},
                          ?MODULE, [Name, InitialEpoch, InitialLayout], []).

read(Name) ->
    gen_server:call(Name, read, infinity).

write(Name, Epoch, Layout) ->
    gen_server:call(Name, {write, Epoch, Layout}, infinity).

stop(Name) ->
    gen_server:call(Name, stop, infinity).

%%%%%%%%%%%%%%%%%%%%%%

init([Name, InitialEpoch, InitialLayout]) ->
    {ok, #state{name=Name,
                epoch=InitialEpoch,
                layout=InitialLayout}}.

handle_call(read, _From,
            #state{epoch=Epoch, layout=Layout} = S) ->
    {reply, {ok, Epoch, Layout}, S};
handle_call({write, NewEpoch, NewLayout}, _From,
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
