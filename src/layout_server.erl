
-module(layout_server).

%% API
-export([start_link/3, read/1, write/3, stop/1]).

-record(state, {
          name   :: atom(),
          epoch  :: non_neg_integer(),
          layout :: term()
         }).

start_link(Name, InitialEpoch, InitialLayout) when InitialEpoch >= 0 ->
    Pid = spawn_link(fun() ->
                             server_loop(Name, InitialEpoch, InitialLayout)
                     end),
    {ok, Pid}.

read(Name) ->
    g_call(Name, read, infinity).

write(Name, Epoch, Layout) ->
    g_call(Name, {write, Epoch, Layout}, infinity).

stop(Name) ->
    g_call(Name, stop, 100).

%%%%%%%%%%%%%%%%%%%%%%

g_call(Pid, Term, Timeout) ->
    Ref = make_ref(),
    Pid ! {g_call, Ref, Term, self()},
    receive
        {g_reply, Ref, Reply} ->
            Reply
    after Timeout ->
            exit(timeout)
    end.

g_reply(Pid, Ref, Reply) ->
    Pid ! {g_reply, Ref, Reply}.

server_loop(Name, InitialEpoch, InitialLayout) ->
    server_loop(
      #state{name=Name,
             epoch=InitialEpoch,
             layout=InitialLayout}).

server_loop(S) ->
    receive
        {g_call, Ref, read, From} ->
            #state{epoch=Epoch, layout=Layout} = S,
            g_reply(From, Ref, {ok, Epoch, Layout}),
            server_loop(S);
        {g_call, Ref, {write, NewEpoch, NewLayout}, From} ->
            #state{epoch=Epoch} = S,
            if is_integer(NewEpoch), NewEpoch > Epoch ->
                    g_reply(From, Ref, ok),
                    server_loop(S#state{epoch=NewEpoch, layout=NewLayout});
               true ->
                    g_reply(From, Ref, bad_epoch),
                    server_loop(S)
            end;
        {g_call, Ref, stop, From} ->
            g_reply(From, Ref, ok),
            exit(normal)
    end.
