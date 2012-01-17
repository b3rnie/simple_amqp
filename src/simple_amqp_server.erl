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

        , subscribe/3
        , unsubscribe/3
        , publish/5
        , exchange_declare/3
        , exchange_delete/3
        , queue_declare/3
        , queue_delete/3
        , bind/4
        , unbind/4
        , cleanup/1
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

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { channels           %% orddict()
           , pid2clients        %% orddict()
           , connection_pid     %% pid()
           , connection_monitor %% monitor()
           }).

-record(channel, { pid     %% pid()
                 , monitor %% monitor()
                 }).
%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
  call(stop).

subscribe(Pid, Queue, Ops) ->
  call({subscribe, Pid, Queue, Ops}).

unsubscribe(Pid, Queue, Ops) ->
  call({unsubscribe, Pid, Queue, Ops}).

publish(Pid, Exchange, RoutingKey, Payload, Ops) ->
  call({publish, Pid, Exchange, RoutingKey, Payload, Ops}).

exchange_declare(Pid, Exchange, Ops) ->
  call({exchange_declare, Pid, Exchange, Ops}).

exchange_delete(Pid, Exchange, Ops) ->
  call({exchange_delete, Pid, Exchange, Ops}).

queue_declare(Pid, Queue, Ops) ->
  call({queue_declare, Pid, Queue, Ops}).

queue_delete(Pid, Queue, Ops) ->
  call({queue_delete, Pid, Queue, Ops}).

bind(Pid, Queue, Exchange, RoutingKey) ->
  call({bind, Pid, Queue, Exchange, RoutingKey}).

unbind(Pid, Queue, Exchange, RoutingKey) ->
  call({unbind, Pid, Queue, Exchange, RoutingKey}).

cleanup(Pid) ->
  call({cleanup, Pid}).

call(Args) -> gen_server:call(?MODULE, Args).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  case connect(Args) of
    {ok, ConnectionPid} ->
      Monitor = erlang:monitor(process, ConnectionPid),
      {ok, #s{ channels           = orddict:new()
             , pid2clients        = orddict:new()
             , connection_pid     = ConnectionPid
             , connection_monitor = Monitor
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call({subscribe, Pid, Queue, Ops}, From, S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:subscribe(CPid, From, Queue, Ops),
  {noreply, S};

handle_call({unsubscribe, Pid, Queue, Ops}, From,
            #s{channels = Channels} = S) ->
  case orddict:find(Pid, Channels) of
    {ok, #channel{pid = CPid}} ->
      simple_amqp_channel:unsubscribe(CPid, From, Queue, Ops),
      {noreply, S};
    error ->
      {reply, {error, no_subscription}, S}
  end;

handle_call({publish, Pid, Exchange, RoutingKey, Payload, Ops}, From, S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:publish(CPid, From, Exchange, RoutingKey, Payload, Ops),
  {noreply, S};

handle_call({Method, Pid, Exchange, Ops}, From, S0)
  when Method == exchange_declare;
       Method == exchange_delete ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:Method(CPid, From, Exchange, Ops),
  {noreply, S};

handle_call({Method, Pid, Queue, Ops}, From, S0)
  when Method == queue_declare;
       Method == queue_delete ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:Method(CPid, From, Queue, Ops),
  {noreply, S};

handle_call({Method, Pid, Queue, Exchange, RoutingKey}, From, S0)
  when Method == bind;
       Method == unbind ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:Method(CPid, From, Queue, Exchange, RoutingKey),
  {noreply, S};

handle_call({cleanup, Pid}, _From, S0) ->
  S = maybe_delete(Pid, S0),
  {reply, ok, S}.

handle_cast(stop, #s{} = S) ->
  {stop, normal, S}.

handle_info({'DOWN', CMon, process, CPid, Rsn},
            #s{ connection_pid     = CPid
              , connection_monitor = CMon
              } = S) ->
  {stop, Rsn, S};

handle_info({'DOWN', Mon, process, Pid, Rsn},
            #s{pid2clients = Pid2Clients} = S0) ->
  error_logger:info_msg("Channel died (~p): ~p~n", [?MODULE, Rsn]),
  S = maybe_delete(orddict:fetch(Pid, Pid2Clients), S0),
  {noreply, S};

handle_info(Info, S) ->
  io:format("WERID INFO: ~p~n", [Info]),
  {noreply, S}.

terminate(_Rsn, #s{ connection_pid = ConnectionPid
                  , channels       = Channels}) ->
  orddict:fold(fun({CPid, #channel{}}, _) ->
                   simple_amqp_channel:stop(CPid)
               end, '_', Channels),
  ok = amqp_connection:close(ConnectionPid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, #s{ connection_pid = ConnectionPid
                       , channels       = Channels0
                       , pid2clients    = Pid2Clients0} = S0) ->
  case orddict:find(ClientPid, Channels0) of
    {ok, #channel{pid = CPid}} -> {CPid, S0};
    error                      ->
      Args = orddict:from_list([ {connection_pid, ConnectionPid}
                               , {client_pid,     ClientPid}
                               ]),
      {ok, CPid}  = simple_amqp_channel:start(Args),
      CMon        = erlang:monitor(process, CPid),
      Channel     = #channel{ pid     = CPid
                            , monitor = CMon},
      Channels    = orddict:store(ClientPid, Channel, Channels0),
      Pid2Clients = orddict:store(CPid, ClientPid, Pid2Clients0),
      {CPid, S0#s{ channels    = Channels
                 , pid2clients = Pid2Clients}}
  end.

maybe_delete(ClientPid, #s{ channels    = Channels
                          , pid2clients = Pid2Clients} = S) ->
  case orddict:find(ClientPid, Channels) of
    {ok, #channel{ pid     = ChannelPid
                 , monitor = ChannelMonitor}} ->
      erlang:demonitor(ChannelMonitor, [flush]),
      simple_amqp_channel:stop(ChannelPid),
      S#s{ channels    = orddict:erase(ClientPid, Channels)
         , pid2clients = orddict:erase(ChannelPid, Pid2Clients)};
    error -> S
  end.

connect(Args) ->
  do_connect(orddict:fetch(brokers, Args)).

do_connect([]) -> {error, no_working_brokers};
do_connect([{Type, Conf}|T]) ->
  Params = params(Type, Conf),
  case amqp_connection:start(Params) of
    {ok, ConnectionPid} -> {ok, ConnectionPid};
    {error, Rsn}        ->
      error_logger:info_msg("Connect failed (~p,~p): ~p ~n",
                            [?MODULE, Params, Rsn]),
      do_connect(T)
  end.

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


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
