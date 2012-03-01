%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp server
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_server).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/0
        , cleanup/0
        , cmd/2
        , add_connection/1
        , open_connections/0
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
-define(connections, 2).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { connection_pid
           , connections
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop()              -> cast(stop).
cleanup()           -> call(cleanup).
cmd(Cmd, Args)      -> call({cmd, Cmd, Args}).
add_connection(Pid) -> cast({add_connection, Pid}).
open_connections()  -> call(open_connections).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ets:new(?MODULE, [named_table, ordered_set, private]),
  case simple_amqp_connection:start_link(Args) of
    {ok, Pid} ->
      simple_amqp_connection:connect(Pid, ?connections),
      {ok, #s{ connection_pid = Pid
             , connections    = []}};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call({cmd, unsubscribe, Args}, {Pid, _} = From, S) ->
  case ets_select(Pid, '_', client) of
    [{{Pid, _Ref}, client, ChannelPid}] ->
      simple_amqp_channel:cmd(ChannelPid, unsubscribe, Args, From),
      {noreply, S};
    [] ->
      {reply, {error, not_subscribed}, S}
  end;

handle_call({cmd, Cmd, Args}, {Pid, _} = From, S) ->
  case maybe_new(Pid, S#s.connections) of
    {ok, {ChannelPid, Connections}} ->
      simple_amqp_channel:cmd(ChannelPid, Cmd, Args, From),
      {noreply, S#s{connections = Connections}};
    {error, Rsn} ->
      {reply, {error, Rsn}, S}
  end;

handle_call(open_connections, _From, S) ->
  {reply, length(S#s.connections), S};

handle_call(cleanup, {Pid, _} = _From, S) ->
  case ets_select(Pid, '_', client) of
    [{{Pid, Ref}, client, _ChannelPid}] ->
      ok = try_delete(Pid, Ref, client);
    [] ->
      ok
  end,
  {reply, ok, S}.

handle_cast({add_connection, Pid}, S) ->
  false = lists:member(Pid, S#s.connections),
  ets_insert(Pid, connection, S#s.connection_pid),
  {noreply, S#s{connections = [Pid | S#s.connections]}};

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info({'DOWN', Ref, process, Pid, Rsn}, S) ->
  case ets_select(Pid, Ref, '_') of
    [{{Pid, Ref}, channel, _ClientPid}] ->
      error_logger:info_msg("channel died (~p): ~p~n", [?MODULE, Rsn]),
      ok = try_delete(Pid, Ref, channel),
      {noreply, S};
    [{{Pid, Ref}, client, ChannelPid}] ->
      error_logger:info_msg("client died (~p): ~p~n", [?MODULE, Rsn]),
      ok = try_delete(Pid, Ref, client),
      simple_amqp_channel:stop(ChannelPid),
      {noreply, S};
    [{{Pid, Ref}, connection, _ConnectionPid}] ->
      error_logger:info_msg("connection died (~p): ~p~n",
                            [?MODULE, Rsn]),
      ok = try_delete(Pid, Ref, connection),
      simple_amqp_connection:connect(S#s.connection_pid, 1),
      {noreply, S#s{connections = S#s.connections -- [Pid]}};
    [] ->
      error_logger:info_msg("weird down message, investigate (~p): "
                            "~p~n", [?MODULE, Rsn]),
      {noreply, S}
  end;

handle_info(Info, S) ->
  error_logger:info_msg("weird info message, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

terminate(_Rsn, S) ->
  error_logger:info_msg("shutting down (~p)~n", [?MODULE]),
  lists:foreach(fun({{Pid, Ref}, channel, ClientPid}) ->
                    simple_amqp_channel:stop(Pid),
                    receive {'DOWN', Ref, process, Pid, _Rsn} -> ok
                    end,
                    ok = try_delete(Pid, Ref, channel)
                end,
                ets_select('_', '_', channel)),

  lists:foreach(fun({{Pid, Ref}, connection, _}) ->
                    ok = try_delete(Pid, Ref, connection),
                    amqp_connection:close(Pid)
                end,
                ets_select('_', '_', connection)),
  simple_amqp_connection:stop(S#s.connection_pid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, Connections) ->
  case ets_select(ClientPid, '_', client) of
    [{{ClientPid, _Ref}, client, ChannelPid}] ->
      {ok, {ChannelPid, Connections}};
    [] when Connections /= [] ->
      {ok, ChannelPid} = simple_amqp_channel:start(
                           [ {connection_pid, hd(Connections)}
                           , {client_pid,     ClientPid}]),
      simple_amqp_channel:open(ChannelPid),
      ets_insert(ClientPid,  client,  ChannelPid),
      ets_insert(ChannelPid, channel, ClientPid),
      {ok, {ChannelPid, tl(Connections) ++ [hd(Connections)]}};
    [] when Connections == [] ->
      {error, no_connections}
  end.

try_delete(Pid, Ref, Type) ->
  case ets_select(Pid, Ref, Type) of
    [{{Pid, Ref}, Type, OtherPid}]
      when Type == channel;
           Type == client ->
      OtherType = other(Type),
      [{{OtherPid, OtherRef}, OtherType, Pid}] =
        ets_select(OtherPid, '_', OtherType),
      ets_delete(Pid, Ref),
      ets_delete(OtherPid, OtherRef),
      ok;
    [{{Pid, Ref}, connection, _}] ->
      ets_delete(Pid, Ref),
      ok;
    [] ->
      {error, no_pid}
  end.

other(channel) -> client;
other(client)  -> channel.

ets_select(Pid, Ref, Type) ->
  ets:select(?MODULE, [{{{Pid, Ref}, Type, '_'}, [], ['$_']}]).

ets_insert(Pid, Type, Pid2) ->
  ets:insert(?MODULE, {{Pid, erlang:monitor(process, Pid)}, Type, Pid2}).

ets_delete(Pid, Ref) ->
  erlang:demonitor(Ref, [flush]),
  ets:delete(?MODULE, {Pid, Ref}).

call(Args) -> gen_server:call(?MODULE, Args).
cast(Args) -> gen_server:cast(?MODULE, Args).
%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
ets_test() ->
  ets:new(?MODULE, [named_table, ordered_set, private]),
  [] = ets_select(foo, bar, baz),
  Pid = self(),
  ets_insert(Pid, client,  Pid),
  ets_insert(Pid, channel, Pid),
  ets_insert(Pid, connection, Pid),
  [{{Pid, Ref1}, client, Pid}]     = ets_select(Pid, '_', client),
  [{{Pid, Ref2}, channel, Pid}]    = ets_select(Pid, '_', channel),
  [{{Pid, Ref3}, connection, Pid}] = ets_select(Pid, '_', connection),
  ok = try_delete(Pid, Ref1, client),
  {error, no_pid} = try_delete(Pid, Ref1, client),
  ok = try_delete(Pid, Ref3, connection),
  ets:delete(?MODULE).
-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
