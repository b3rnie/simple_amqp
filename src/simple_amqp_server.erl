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
  ets:new(?MODULE, [named_table, ordered_set]),
  {ok, #s{brokers  = proplists:get_value(brokers, Args)}, 0}.

handle_call({cmd, unsubscribe, Args}, {Pid, _} = From, S) ->
  case select(Pid, '_', client) of
    [{{Pid, _Ref}, client, ChannelPid}] ->
      simple_amqp_channel:cmd(ChannelPid, From, unsubscribe, Args),
      {noreply, S, next_tick(S)};
    [] ->
      {reply, {error, not_subscribed}, S, next_tick(S)}
  end;

handle_call({cmd, Cmd, Args}, {Pid, _} = From, S) ->
  case maybe_new(Pid, S) of
    {ok, ChannelPid} ->
      simple_amqp_channel:cmd(ChannelPid, From, Cmd, Args),
      {noreply, S, next_tick(S)};
    {error, Rsn} ->
      {reply, {error, Rsn}, S, next_tick(S)}
  end;

handle_call(cleanup, {Pid, _} = _From, S) ->
  case select(Pid, '_', client) of
    [{{Pid, _Ref}, client, _ChannelPid}] ->
      {reply, ok, delete(Pid, [stop]), next_tick(S)};
    [] ->
      {reply, ok, S, next_tick(S)}
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
  end;

handle_info(timeout, #s{connection = {_Pid, _Ref}} = S) ->
  {noreply, S, next_tick(S)};

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{connection = {Pid, Ref}} = S) ->
  error_logger:info_msg("Connection died (~p): ~p~n", [?MODULE, Rsn]),
  erlang:demonitor(Ref, [flush]),
  {noreply, S#s{connection = undefined}, 0};

handle_info({'DOWN', Ref, process, Pid, Rsn}, S) ->
  case select(Pid, Ref, '_') of
    [{{Pid, Ref}, channel, ClientPid}] ->
      error_logger:info_msg("Channel died (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, delete(ClientPid, []), next_tick(S)};
    [{{Pid, Ref}, client, _ChannelPid}] ->
      error_logger:info_msg("Client died (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, delete(Pid, [stop]), next_tick(S)};
    [] ->
      error_logger:info_msg("monitored process died, "
                            "investigate! (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, S, next_tick(S)}
  end;

handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Rsn, S) ->
  lists:foreach(fun({{Pid, Ref}, Type, _}) ->
                    erlang:demonitor(Ref, [flush]),
                    [simple_amqp_channel:stop(Pid) || Type == channel]
                end,
                select('_', '_', '_')),
  case S#s.connection of
    {Pid, Ref} ->
      erlang:demonitor(Ref, [flush]),
      ok = amqp_connection:close(Pid);
    undefined -> ok
  end.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, S) ->
  case select(ClientPid, '_', client) of
    [{{ClientPid, _Ref}, client, ChannelPid}] ->
      {ok, ChannelPid};
    [] when S#s.connection /= undefined ->
      {ConnectionPid, _ConnectionRef} = S#s.connection,
      {ok, ChannelPid}  = simple_amqp_channel:start(
                            [ {connection_pid, ConnectionPid}
                            , {client_pid,     ClientPid}]),
      ets:insert(?MODULE, {{ClientPid,
                            erlang:monitor(process, ClientPid)},
                           client, ChannelPid}),
      ets:insert(?MODULE, {{ChannelPid,
                            erlang:monitor(process, ChannelPid)},
                           channel, ClientPid}),
      {ok, ChannelPid};
    [] when S#s.connection == undefined ->
      {error, no_connection}
  end.

delete(ClientPid, Ops) ->
  [{{ClientPid, ClientRef}, client, ChannelPid}] =
    select(ClientPid, '_', client),
  [{{ChannelPid, ChannelRef}, channel, ClientPid}] =
    select(ChannelPid, '_', channel),
  ets:delete(?MODULE, {ClientPid, ClientRef}),
  ets:delete(?MODULE, {ChannelPid, ChannelRef}),
  erlang:demonitor(ChannelRef, [flush]),
  erlang:demonitor(ClientRef,  [flush]),
  [simple_amqp_channel:stop(ChannelPid) || lists:member(stop, Ops)],
  ok.

next_tick(_S) ->
  ?tick.

select(Pid, Ref, Type) ->
  ets:select(?MODULE, [{{{Pid, Ref}, Type, '_'}, [], ['$_']}]).

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
