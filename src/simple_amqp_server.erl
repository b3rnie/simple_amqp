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
        , add_connection/1
        , del_connection/1
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
-record(s, { connection_handlers
           , connections
           }).

%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop()              -> cast(stop).
cleanup()           -> call(cleanup).
cmd(Cmd, Args)      -> call({cmd, Cmd, Args}).
add_connection(Pid) -> cast({add_connection, Pid}).
del_connection(Pid) -> cast({del_connection, Pid}).
open_connections()  -> call(open_connections).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ets:new(?MODULE, [named_table, ordered_set, private]),
  Handlers = setup_connections(?connections, Args),
  {ok, #s{ connection_handlers = Handlers
         , connections         = []}}.

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
    {error, _Rsn} = E ->
      {reply, E, S}
  end;

handle_call(open_connections, _From, S) ->
  {reply, length(S#s.connections), S};

handle_call(cleanup, {Pid, _} = _From, S) ->
  maybe_delete(Pid, [{stop_channel, true}]),
  {reply, ok, S}.

handle_cast({add_connection, Pid}, S) ->
  false = lists:member(Pid, S#s.connections),
  {noreply, S#s{connections = [Pid | S#s.connections]}};

handle_cast({del_connection, Pid}, S) ->
  true = lists:member(Pid, S#s.connections),
  {noreply, S#s{connections = S#s.connections -- [Pid]}};

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info({'DOWN', Ref, process, Pid, Rsn}, S) ->
  case ets_select(Pid, Ref, '_') of
    [{{Pid, Ref}, channel, ClientPid}] ->
      error_logger:info_msg("channel died (~p): ~p~n", [?MODULE, Rsn]),
      maybe_delete(ClientPid, [{stop_channel, false}]),
      {noreply, S};
    [{{Pid, Ref}, client, _ChannelPid}] ->
      error_logger:info_msg("client died (~p): ~p~n", [?MODULE, Rsn]),
      maybe_delete(Pid, [{stop_channel, true}]),
      {noreply, S};
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
  error_logger:info_msg("shutting down (~p): ~p~n",
                        [?MODULE, S]),
  lists:foreach(fun({{Pid, Ref}, Type, _}) ->
                    erlang:demonitor(Ref, [flush]),
                    [simple_amqp_channel:stop(Pid) || Type == channel]
                end,
                ets_select('_', '_', '_')),
  lists:foreach(fun(Pid) ->
                    simple_amqp_connection:stop(Pid)
                end, S#s.connection_handlers).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
setup_connections(0, _Args) -> [];
setup_connections(N,  Args) ->
  {ok, Pid} = simple_amqp_connection:start_link(Args),
  [Pid | setup_connections(N-1, Args)].

maybe_new(ClientPid, Connections) ->
  case ets_select(ClientPid, '_', client) of
    [{{ClientPid, _Ref}, client, ChannelPid}] ->
      {ok, {ChannelPid, Connections}};
    [] when Connections /= [] ->
      case simple_amqp_channel:start(
             [ {connection_pid, hd(Connections)}
             , {client_pid,     ClientPid}]) of
        {ok, ChannelPid} ->
          ets_insert(ClientPid,  client,  ChannelPid),
          ets_insert(ChannelPid, channel, ClientPid),
          {ok, {ChannelPid, tl(Connections) ++ [hd(Connections)]}};
        {error, Rsn} ->
          {error, Rsn}
      end;
    [] when Connections == [] ->
      {error, no_connections}
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

ets_select(Pid, Ref, Type) ->
  ets:select(?MODULE, [{{{Pid, Ref}, Type, '_'}, [], ['$_']}]).

ets_insert(Pid, Type, Pid2) ->
  ets:insert(?MODULE, {{Pid, erlang:monitor(process, Pid)}, Type, Pid2}).

ets_delete(Pid, Ref) ->
  erlang:demonitor(Ref, [flush]),
  ets:delete(?MODULE, {Pid, Ref}).


call(Args) -> gen_server:call(?MODULE, Args).
cast(Args) -> gen_server:cast(?MODULE, Args).
%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
