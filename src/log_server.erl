
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

write(Name, Epoch, Idx, Val, Repair_p, HdRepSpecial_p) ->
    gen_server:call(Name, {write, Epoch, Idx, Val, Repair_p, HdRepSpecial_p}, infinity).

read(Name, Epoch, Idx) when is_integer(Epoch), is_integer(Idx) ->
    read(Name, Epoch, Idx, false).

read_during_repair(Name, Epoch, Idx) when is_integer(Epoch), is_integer(Idx) ->
    read(Name, Epoch, Idx, true).

read(Name, Epoch, Idx, HdRepSpecial_p)  when is_integer(Epoch), is_integer(Idx) ->
    gen_server:call(Name, {read, Epoch, Idx, HdRepSpecial_p}, infinity).

%%%%%%%%%%%%%%%%%%%%%%

init([Name, InitialEpoch, InitialLayout, InitialStore]) ->
    {ok, #state{name=Name,
                epoch=InitialEpoch,
                layout=InitialLayout,
                store=orddict:from_list(InitialStore),
                h_store=orddict:new()}}.

handle_call(get_layout, _From,
            #state{epoch=Epoch, layout=Layout} = S) ->
    {reply, {ok, Epoch, Layout}, S};

handle_call({set_layout, NewEpoch, NewLayout}, _From,
            #state{name=Name, epoch=Epoch} = S) ->
    #layout{upi=UPI, repairing=Repairing} = NewLayout,
    if NewEpoch > Epoch ->
            AmSplit_p = UPI /= [] andalso Name == hd(UPI) andalso
                        Name == lists:last(UPI) andalso
                        Repairing /= [],
            {reply, ok, S#state{epoch=NewEpoch, layout=NewLayout,
                                am_split=AmSplit_p, h_store=orddict:new()}};
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
handle_call({write, _Epoch, Idx, Val, Repair_p, HdRepSpecial_p}=QQ, _From,
            #state{am_split=AmSplit_p, store=D, h_store=HD} = S) ->
    %% TODO: This logic is icky.  But the unit test split_role_test() works.
    %%       ^_^
    if HdRepSpecial_p == false ->
            HeadRepairHeadRole_and_HeadRoleHasKey =
                AmSplit_p andalso orddict:is_key(Idx, HD),
            case {orddict:is_key(Idx, D), Repair_p, HeadRepairHeadRole_and_HeadRoleHasKey} of
                {true, false, false} ->
                    {reply, written, S};
                _ ->
                    case orddict:find(Idx, HD) of
                        {ok, OtherVal} when OtherVal /= Val ->
                            {reply, bad_value, S};
                        _ ->
                            D2 = orddict:store(Idx, Val, D),
                            HD2 = orddict:erase(Idx, HD),
                            {reply, ok, S#state{store=D2, h_store=HD2}}
                    end
            end;
       true ->
            %% Sanity! HdRepSpecial_p=true is only valid when sent to the head,
            %% but Repair_p repair clobber should only ever be sent to middle.
            Repair_p = false,
            case {orddict:find(Idx, HD), orddict:is_key(Idx, D)} of
                {{ok,_},  _} ->
                    %% Written during this repair period
                    {reply, written, S};
                {error, true} ->
                    %% Written before this repair period started
                    {reply, written, S};
                {error, false} ->
                    D2 = orddict:store(Idx, Val, D),
                    HD2 = orddict:store(Idx, Val, HD),
                    {reply, ok, S#state{store=D2, h_store=HD2}}
            end
    end;

handle_call({read, Epoch, _Idx, _HdRepSpecial_p}, _From,
            #state{epoch=MyEpoch}=S)
  when Epoch < MyEpoch ->
    {reply, {bad_epoch, MyEpoch}, S};

handle_call({read, Epoch, _Idx, _HdRepSpecial_p}, _From,
            #state{epoch=MyEpoch, layout=Layout}=S)
  when Epoch > MyEpoch orelse Layout == undefined ->
    {reply, wedged, S#state{layout=undefined}};
handle_call({read, _Epoch, Idx, HdRepSpecial_p}, _From,
            #state{store=D,h_store=HD} = S) ->
    case orddict:find(Idx, D) of
        error ->
            {reply, not_written, S};
        {ok, Val} ->
            case {HdRepSpecial_p, orddict:is_key(Idx, HD)} of
                {false, true} ->
                    %% HdRepSpecial_p=false means we are in a tail role.
                    %% Idx was written during split role repair time and
                    %% has not yet been written to the tail.
                    {reply, not_written, S};
                _ ->
                    {reply, {ok, Val}, S}
            end
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
