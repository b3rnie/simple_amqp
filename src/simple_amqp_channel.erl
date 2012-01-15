%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp channel
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_channel).

%%%_* Exports ==========================================================
-export([ start/1
        , start_link/1
        , stop/1
        , subscribe/3
        , unsubscribe/3
        , publish/5
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
-record(s, { client_pid
           , client_monitor
           , channel_monitor
           , channel_pid
           , subscriptions %% {queue, consumer_tag}
           }).

%%%_ * API -------------------------------------------------------------
start(Args)      -> gen_server:start(?MODULE, Args, []).
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop(Pid)        -> gen_server:cast(Pid, stop).

subscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {subscribe, From, Queue}).

unsubscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {unsubscribe, From, Queue}).

publish(Pid, From, Exchange, RoutingKey, Msg) ->
  gen_server:cast(Pid, {publish, From, Exchange, RoutingKey, Msg}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  Connection = orddict:fetch(connection, Args),
  case amqp_connection:open_channel(Connection) of
    {ok, ChannelPid} ->
      ClientPid = orddict:fetch(client_pid, Args),
      {ok, #s{ client_pid      = ClientPid
             , client_monitor  = erlang:monitor(process, ClientPid)
             , channel_pid     = ChannelPid
             , channel_monitor = erlang:monitor(process, ChannelPid)
             , subscriptions   = []
             }}
  end.

handle_cast(stop, S) ->
  {stop, normal, S};

handle_cast({subscribe, From, Queue},
            #s{subscriptions = Subscriptions} = S) ->
  case lists:keymember(Queue, 1, Subscriptions) of
    true ->
      gen_server:reply(From, {error, already_subscribed}),
      {noreply, S};
    false ->
      amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
      #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:call(Channel,
                          #'basic.consume'{ queue = Queue
                                          , consumer_tag = <<"foo">>
                                          }),
      gen_server:reply(From, {ok, self()}),
      {noreply, S#s{subscriptions = [{Queue,Tag} | Subscriptions]}}
  end;

handle_cast({unsubscribe, From, Queue},
            #s{ channel_pid   = ChannelPid
              , subscriptions = Subscriptions0} = S) ->
  case lists:keytake(Queue, 1, Subscriptions0) of
    {value, {Queue, ConsumerTag}, Subscriptions} ->
      amqp_channel:call(ChannelPid,
                        #'basic.cancel'{consumer_tag = ConsumerTag}),
      gen_server:reply(From, ok),
      {noreply, S#s{subscriptions = Subscriptions}};
    false ->
      gen_server:reply(From, {error, not_subscribed}),
      {noreply, S}
  end;

handle_cast({publish, From, Exchange, RoutingKey, PayLoad},
            #s{channel = Channel} = S) ->
  Method =
    #'basic.publish'{ exchange    = Exchange
                    , routing_key = RoutingKey
                    , mandatory   = true
                    , immediate   = true
                    },
  Props = #'P_basic'{delivery_mode = 2}, %% 1 not persistent
                                         %% 2 persistent
  Msg = #amqp_msg{ payload = Payload
                 , props   = Props
                 },
  amqp_channel:call(Channel, Method, Msg),
  gen_server:reply(From, ok),
  {noreply, S};

handle_info(#'basic.consume_ok'{}, S) ->
  {noreply, S};

handle_info(#'basic.cancel_ok'{}, S) ->
  {noreply, S};

handle_info({#'basic.deliver'{}, Content}, S) ->
  {noreply, S};

handle_info(#'basic.raturn'{ reply_text = <<"unroutable">>
                           , exchange = Exchange}, S) ->
  %% Do something with the returned message
  {noreply, S}

handle_info({ack, Tag}, #s{channel_pid = ChannelPid} = S) ->
  amqp_channel:cast(ChannelPid, #'basic.ack'{delivery_tag = Tag}),
  {noreply, S};

handle_info({'DOWN', ChannelRef, process, ChannelPid, Rsn},
            #s{ channel_pid     = ChannelPid
              , channel_monitor = ChannelRef} = S) ->
  simple_amqp_server:cleanup(S#s.client_pid),
  {noreply, S};

handle_info({'DOWN', ClientRef, process, ClientPid, Rsn},
            #s{ client_pid     = ClientPid
              , client_monitor = ClientRef} = S) ->
  simple_amqp_server:cleanup(ClientPid),
  {noreply, S};


handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, #s{ channel_pid     = ChannelPid
                     , channel_monitor = ChannelRef
                     , client_pid      = ClientPid
                     , client_monitor  = ClientRef}) ->
  erlang:demonitor(ChannelRef, [flush]),
  erlang:demonitor(ClientRef,  [flush]),
  amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
  amqp_channel:close(Channel),
  ok.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
