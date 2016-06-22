
-module(log_server).

-behaviour(gen_server).

-export([start_link/4,
         set_layout/3,                          % used by chain manager thingie
         get_layout/1,                          % unit test/debugging only
         stop/1,
         write/4,
         write_clobber/4,                       % repair API only, obscurity!
         write_during_repair/4,
         read/3,
         read_during_repair/3
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("layout.hrl").

-record(state, {
          name           :: atom(),
          epoch          :: non_neg_integer(),
          layout         :: 'undefined' | term(),
          am_split=false :: boolean(),
          store          :: orddict:orddict(),
          h_store        :: orddict:orddict()
         }).

start_link(Name, InitialEpoch, InitialLayout, InitialStore)
  when is_integer(InitialEpoch), InitialEpoch >= 0,
       is_record(InitialLayout, layout), is_list(InitialStore) ->
    gen_server:start_link({local, Name}, ?MODULE,
                         [Name, InitialEpoch, InitialLayout, InitialStore], []).

get_layout(Name) ->
    gen_server:call(Name, get_layout, infinity).

set_layout(Name, Epoch, Layout)
  when is_integer(Epoch), Epoch >= 0,
       is_record(Layout, layout), Epoch == Layout#layout.epoch->
    gen_server:call(Name, {set_layout, Epoch, Layout}, infinity).

stop(Name) ->
    gen_server:call(Name, stop, infinity).

write(Name, Epoch, Idx, Val) ->
    write(Name, Epoch, Idx, Val, false, false).

write_clobber(Name, Epoch, Idx, Val) ->
    write(Name, Epoch, Idx, Val, true, false).

write_during_repair(Name, Epoch, Idx, Val) ->
    write(Name, Epoch, Idx, Val, false, true).

write(Name, Epoch, Idx, Val, Repair_p, HeadRepair_p) ->
    gen_server:call(Name, {write, Epoch, Idx, Val, Repair_p, HeadRepair_p}, infinity).

read(Name, Epoch, Idx) when is_integer(Epoch), is_integer(Idx) ->
    read(Name, Epoch, Idx, false).

read_during_repair(Name, Epoch, Idx) when is_integer(Epoch), is_integer(Idx) ->
    read(Name, Epoch, Idx, true).

read(Name, Epoch, Idx, Repair_p)  when is_integer(Epoch), is_integer(Idx) ->
    gen_server:call(Name, {read, Epoch, Idx, Repair_p}, infinity).

%%%%%%%%%%%%%%%%%%%%%%

init([Name, InitialEpoch, InitialLayout, InitialStore]) ->
    {ok, #state{name=Name,
                epoch=InitialEpoch,
                layout=InitialLayout,
                store=orddict:from_list(InitialStore)}}.

handle_call(get_layout, _From,
            #state{epoch=Epoch, layout=Layout} = S) ->
    {reply, {ok, Epoch, Layout}, S};

handle_call({set_layout, NewEpoch, NewLayout}, _From,
            #state{epoch=Epoch} = S) ->
    if NewEpoch > Epoch ->
            {reply, ok, S#state{epoch=NewEpoch, layout=NewLayout}};
       true ->
            {reply, {bad_epoch, Epoch}, S}
    end;

handle_call(stop, _From, S) ->
    {stop, normal, ok, S};

handle_call({write, Epoch, _Idx, _Val, _Rep, _HdRep}, _From,
            #state{epoch=MyEpoch} = S)
  when Epoch < MyEpoch ->
    {reply, {bad_epoch, MyEpoch}, S};

handle_call({write, Epoch, _Idx, _Val, _Rep, _HdRep}, _From,
            #state{epoch=MyEpoch, layout=Layout} = S)
  when Epoch > MyEpoch orelse Layout == undefined ->
    {reply, wedged, S#state{layout=undefined}};
handle_call({write, _Epoch, Idx, Val, Repair_p, _HeadRepair_p}, _From,
            #state{store=D} = S) ->
    case {orddict:is_key(Idx, D), Repair_p} of
        {true, false} ->
            {reply, written, S};
        _ ->
            {reply, ok, S#state{store=orddict:store(Idx, Val, D)}}
    end;

handle_call({read, Epoch, _Idx, _Repair_p}, _From, #state{epoch=MyEpoch} = S)
  when Epoch < MyEpoch ->
    {reply, {bad_epoch, MyEpoch}, S};

handle_call({read, Epoch, _Idx, _Repair_p}, _From, #state{epoch=MyEpoch,
                                                          layout=Layout} = S)
  when Epoch > MyEpoch orelse Layout == undefined ->
    {reply, wedged, S#state{layout=undefined}};
handle_call({read, _Epoch, Idx, _Repair_p}, _From, #state{store=D} = S) ->
    case orddict:find(Idx, D) of
        error ->
            {reply, not_written, S};
        {ok, Val} ->
            {reply, {ok, Val}, S}
    end.

%%%%%%%%%%%%%%%%%%%%%%

handle_cast(_Msg, S) ->
    io:format(user, "Unknown cast ~p\n", [_Msg]),
    {noreply, S}.

handle_info(_Info, S) ->
    io:format(user, "Unknown msg ~p\n", [_Info]),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%
