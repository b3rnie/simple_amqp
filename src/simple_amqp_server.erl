%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp server
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_server).

%%%_* Exports ==========================================================
-export([ start/1
        , start_link/1
        , stop/0
        , cleanup/0
        , cmd/2
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("amqp_client/include/amqp_client.hrl").

%%%_* Macros ===========================================================
-define(tick, 1000).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { channels %% dict clientpid  -> channel
           , clients  %% dict channelpid -> clientpid
           , connection %% {pid, ref}
           , brokers
           }).

-record(e, { pid
           , ref
           }).

%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop()         -> cast(stop).
cleanup()      -> call(cleanup).
cmd(Cmd, Args) -> call({cmd, Cmd, Args}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ets:new(?MODULE, [named_table, ordered_set]),
  {ok, #s{brokers  = proplists:get_value(brokers, Args)}, 0}.

handle_call({cmd, unsubscribe, Args}, {Pid, _} = From, S) ->
  case select({Pid, client, '_'}) of
    [ChannelPid] ->
      simple_amqp_channel:cmd(ChannelPid, From, unsubscribe, Args),
      {noreply, S, next_tick(S)};
    [] ->
      {reply, {error, not_subscribed}, S, next_tick(S)}
  end;

handle_call({cmd, Cmd, Args}, {Pid, _} = From, S0) ->
  case maybe_new(Pid, S0) of
    {ok, {ChannelPid, S}} ->
      simple_amqp_channel:cmd(ChannelPid, From, Cmd, Args),
      {noreply, S, next_tick(S)};
    {error, Rsn} ->
      {reply, {error, Rsn}, S0, next_tick(S)}
  end

handle_call(cleanup, {Pid, _} = _From, S0) ->
  case select({Pid, client, '_'}) of
    [_ChannelPid] -> {reply, ok, delete(Pid, S0), next_tick(S)};
    []            -> {reply, ok, S0,              next_tick(S)}
  end.

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info(timeout, #s{ connection = undefined
                       , brokers    = [{Type, Conf} | Brokers]} = S0) ->
  case amqp_connection:start(params(Type, Conf)) of
    {ok, Pid} ->
      S = S0#s{ connection = {Pid, erlang:monitor(process, Pid)}
              , brokers    = Brokers ++ [{Type, Conf}]},
      {noreply, S, next_tick(S)};
    {error, Rsn}        ->
      error_logger:info_msg("Connect failed (~p,~p): ~p ~n",
                            [?MODULE, {Type, Conf}, Rsn]),
      {noreply, S0, next_tick(S0)}
  end.

handle_info(timeout, #s{connection = {_Pid, _Ref}} = S) ->
  {noreply, S, next_tick(S)};

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{connection = {Pid, Ref}} = S) ->
  error_logger:info_msg("Connection died (~p): ~p~n", [?MODULE, Rsn]),
  erlang:demonitor(Ref, [flush]),
  {noreply, S#s{connection = undefined}, 0};

handle_info({'DOWN', _Ref, process, _Pid, _Rsn} = Down, S) ->
  case select({Pid, '_', Ref}) of
    [{Pid, channel, Ref}] ->
      error_logger:info_msg("Channel died (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, delete(ChannelPid, S), next_tick(S)};
    [{Pid, client, Ref}] ->
      error_logger:info_msg("Client died (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, delete(ChannelPid, S), next_tick(S)};
    error ->
      error_logger:info_msg("monitored process died, "
                            "investigate! (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, S, next_tick(S)}
  end;

handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Rsn, #s{ connection_pid       = ConnectionPid
                  , connection_ref       = ConnectionRef
                  , clientpid_to_channel = CDict}) ->
  dict:fold(fun(_ClientPid, #channel{ pid = Pid
                                    , ref = Ref}, '_') ->
                erlang:demonitor(Ref, [flush]),
                simple_amqp_channel:stop(Pid)
            end, '_', CDict),
  erlang:demonitor(ConnectionRef, [flush]),
  ok = amqp_connection:close(ConnectionPid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, S) ->
  case dict:find(ClientPid, S#s.channels) of
    {ok, #e{pid = ChannelPid}} ->
      {ok, {ChannelPid, S}};
    error when S#s.connection /= undefined ->
      {ConnectionPid, _ConnectionRef} = S#s.connection,
      {ok, ChannelPid}  = simple_amqp_channel:start(
                            [ {connection_pid, ConnectionPid}
                            , {client_pid,     ClientPid}]),
      Channels = dict:store(ClientPid,  e(ChannelPid), S#s.channels),
      Clients  = dict:store(ChannelPid, e(ClientPid),  S#s.clients),
      {ok, {ChannelPid, S#s{ channels = Channels
                           , clients  = Clients}}};
    error ->
      {error, no_connection}
  end.

delete(ClientPid, Ops, S) ->
  #e{pid = ChannelPid,
     ref = ChannelRef} = dict:fetch(ClientPid, S#s.channels),
  #e{pid = ClientPid,
     ref = ClientRef} = dict:fetch(ChannelPid, S#s.clients),
  erlang:demonitor(ChannelRef, [flush]),
  erlang:demonitor(ClientRef,  [flush]),
  [simple_amqp_channel:stop(ChannelPid) || lists:member(stop, Ops)],
  S#s{ channels = dict:erase(ClientPid,  S#s.channels)
     , clients  = dict:erase(ChannelPid, S#s.clients)}.

next_tick(_S) ->
  ?tick.

e(Pid) ->
  #e{ pid = Pid
    , erlang:monitor(process, Pid)}.

params(direct, Args) ->
  F = fun(K) -> proplists:get_value(K, Args) end,
  #amqp_params_direct{ username          = F(username)
                     , virtual_host      = F(virtual_host)
                     , node              = F(node)
                     };

params(network, Args) ->
  F = fun(K) -> proplists:get_value(K, Args) end,
  #amqp_params_network{ username     = F(username)
                      , password     = F(password)
                      , virtual_host = F(virtual_host)
                      , host         = F(host)
                      , port         = F(port)
                      }.

call(Args) -> gen_server:call(?MODULE, Args).
cast(Args) -> gen_server:cast(?MODULE, Args).
%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
