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
-record(s, { connection %% {pid, ref}
           , brokers    %% [{type, conf}]
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
  ets:new(?MODULE, [named_table, ordered_set, private]),
  {ok, #s{brokers  = proplists:get_value(brokers, Args)}, 0}.

handle_call({cmd, unsubscribe, Args}, {Pid, _} = From, S) ->
  case ets_select(Pid, '_', client) of
    [{{Pid, _Ref}, client, ChannelPid}] ->
      simple_amqp_channel:cmd(ChannelPid, unsubscribe, Args, From),
      {noreply, S, next_tick(S)};
    [] ->
      {reply, {error, not_subscribed}, S, next_tick(S)}
  end;

handle_call({cmd, Cmd, Args}, {Pid, _} = From, S) ->
  case maybe_new(Pid, S#s.connection) of
    {ok, ChannelPid} ->
      simple_amqp_channel:cmd(ChannelPid, Cmd, Args, From),
      {noreply, S, next_tick(S)};
    {error, _Rsn} = E ->
      {reply, E, S, next_tick(S)}
  end;

handle_call(cleanup, {Pid, _} = _From, S) ->
  maybe_delete(Pid, [{stop_channel, true}]),
  {reply, ok, S, next_tick(S)}.

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info(timeout, #s{ connection = undefined
                       , brokers    = [{Type, Conf} | Brokers]} = S0) ->
  error_logger:info_msg("trying to connect (~p)~n", [?MODULE]),
  case amqp_connection:start(params(Type, Conf)) of
    {ok, Pid} ->
      %% xxx better logging
      error_logger:info_msg("connect successful (~p): ~p~n",
                            [?MODULE, Pid]),
      S = S0#s{ connection = {Pid, erlang:monitor(process, Pid)}
              , brokers    = Brokers ++ [{Type, Conf}]},
      {noreply, S, next_tick(S)};
    {error, Rsn} ->
      error_logger:error_msg("connect failed (~p): ~p~n",
                             [?MODULE, Rsn]),
      {noreply, S0, next_tick(S0)}
  end;

handle_info(timeout, #s{connection = {_Pid, _Ref}} = S) ->
  {noreply, S, next_tick(S)};

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{connection = {Pid, Ref}} = S) ->
  error_logger:info_msg("connection died (~p): ~p~n", [?MODULE, Rsn]),
  erlang:demonitor(Ref, [flush]),
  {noreply, S#s{connection = undefined}, 0};

handle_info({'DOWN', Ref, process, Pid, Rsn}, S) ->
  case ets_select(Pid, Ref, '_') of
    [{{Pid, Ref}, channel, ClientPid}] ->
      error_logger:info_msg("channel died (~p): ~p~n", [?MODULE, Rsn]),
      maybe_delete(ClientPid, [{stop_channel, false}]),
      {noreply, S, next_tick(S)};
    [{{Pid, Ref}, client, _ChannelPid}] ->
      error_logger:info_msg("client died (~p): ~p~n", [?MODULE, Rsn]),
      maybe_delete(Pid, [{stop_channel, true}]),
      {noreply, S, next_tick(S)};
    [] ->
      error_logger:info_msg("weird down message, investigate (~p): "
                            "~p~n", [?MODULE, Rsn]),
      {noreply, S, next_tick(S)}
  end;

handle_info(Info, S) ->
  error_logger:info_msg("weird info message, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

terminate(_Rsn, S) ->
  lists:foreach(fun({{Pid, Ref}, Type, _}) ->
                    erlang:demonitor(Ref, [flush]),
                    [simple_amqp_channel:stop(Pid) || Type == channel]
                end,
                ets_select('_', '_', '_')),
  case S#s.connection of
    {Pid, Ref} ->
      erlang:demonitor(Ref, [flush]),
      ok = amqp_connection:close(Pid);
    undefined -> ok
  end.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, Connection) ->
  ct:pal("XXX: ~p~n", [ets_select(ClientPid, '_', client)]),
  case ets_select(ClientPid, '_', client) of
    [{{ClientPid, _Ref}, client, ChannelPid}] ->
      {ok, ChannelPid};
    [] when Connection /= undefined ->
      {ConnectionPid, _ConnectionRef} = Connection,
      case simple_amqp_channel:start(
             [ {connection_pid, ConnectionPid}
             , {client_pid,     ClientPid}]) of
        {ok, ChannelPid} ->
          ets_insert(ClientPid,  client,  ChannelPid),
          ets_insert(ChannelPid, channel, ClientPid),
          {ok, ChannelPid};
        {error, Rsn} ->
          {error, Rsn}
      end;
    [] when Connection == undefined ->
      {error, no_connection}
  end.

maybe_delete(Pid, Ops) ->
  case ets_select(Pid, '_', client) of
    [{{Pid, Ref}, client, ChannelPid}] ->
      [{{ChannelPid, ChannelRef}, channel, Pid}] =
        ets_select(ChannelPid, '_', channel),
      ets_delete(Pid, Ref),
      ets_delete(ChannelPid, ChannelRef),
      [simple_amqp_channel:stop(ChannelPid) ||
        proplists:get_value(stop_channel, Ops)];
    [] -> ok
  end.

next_tick(_S) ->
  ?tick.

ets_select(Pid, Ref, Type) ->
  ets:select(?MODULE, [{{{Pid, Ref}, Type, '_'}, [], ['$_']}]).

ets_insert(Pid, Type, Pid2) ->
  ets:insert(?MODULE, {{Pid, erlang:monitor(process, Pid)}, Type, Pid2}).

ets_delete(Pid, Ref) ->
  erlang:demonitor(Ref, [flush]),
  ets:delete(?MODULE, {Pid, Ref}).

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
