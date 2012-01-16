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

        , subscribe/2
        , unsubscribe/2
        , publish/4
        , declare_exchange/2
        , delete_exchange/2
        , declare_queue/2
        , delete_queue/2
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

subscribe(Pid, Queue) ->
  call({subscribe, Pid, Queue}).

unsubscribe(Pid, Queue) ->
  call({unsubscribe, Pid, Queue}).

publish(Pid, Exchange, RoutingKey, Payload) ->
  call({publish, Pid, Exchange, RoutingKey, Payload}).

declare_exchange(Pid, Exchange) ->
  call({declare_exchange, Pid, Exchange}).

delete_exchange(Pid, Exchange) ->
  call({delete_exchange, Pid, Exchange}).

declare_queue(Pid, Queue) ->
  call({declare_queue, Pid, Queue}).

delete_queue(Pid, Queue) ->
  call({delete_queue, Pid, Queue}).

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
             , connection_pid     = ConnectionPid
             , connection_monitor = Monitor
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call({subscribe, Pid, Queue}, From, #s{} = S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:subscribe(CPid, From, Queue),
  {noreply, S};

handle_call({unsubscribe, Pid, Queue}, From,
            #s{channels = Channels} = S) ->
  case orddict:find(Pid, Channels) of
    {ok, #channel{pid = CPid}} ->
      simple_amqp_channel:unsubscribe(CPid, From, Queue),
      {noreply, S};
    error ->
      {reply, {error, no_subscription}, S}
  end;

handle_call({publish, Pid, Exchange, RoutingKey, Payload}, From, S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:publish(CPid, From, Exchange, RoutingKey, Payload),
  {noreply, S};

handle_call({Method, Pid, Exchange}, From, S0)
  when Method == declare_exchange;
       Method == delete_exchange ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:Method(CPid, From, Exchange),
  {noreply, S};

handle_call({Method, Pid, Queue}, From, S0)
  when Method == declare_queue;
       Method == delete_queue ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:Method(CPid, From, Queue),
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

handle_info({'DOWN', _Mon, process, _Pid, Rsn}, S) ->
  %% no reason to shut everything down here.
  %% but do this for now
  {stop, Rsn, S};

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
                       , channels       = Channels0} = S0) ->
  case orddict:find(ClientPid, Channels0) of
    {ok, #channel{pid = CPid}} -> {CPid, S0};
    error                      ->
      Args = orddict:from_list([ {connection_pid, ConnectionPid}
                               , {client_pid,     ClientPid}
                               ]),
      {ok, CPid} = simple_amqp_channel:spawn(Args),
      CMon       = erlang:monitor(process, CPid),
      Channel    = #channel{ pid     = CPid
                           , monitor = CMon},
      Channels   = orddict:store(ClientPid, Channel, Channels0),
      {CPid, S0#s{channels = Channels}}
  end.

maybe_delete(Pid, #s{channels = Channels} = S) ->
  case orddict:find(Pid, Channels) of
    {ok, #channel{ pid     = ChannelPid
                 , monitor = ChannelMonitor}} ->
      erlang:demonitor(ChannelMonitor, [flush]),
      simple_amqp_channel:stop(ChannelPid),
      S#s{channels = orddict:erase(Pid, Channels)};
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
