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
        , declare_exchange/3
        , delete_exchange/3
        , declare_queue/3
        , delete_queue/3
        , bind/5
        , unbind/5
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
-record(s, { client_pid      %% pid()
           , client_monitor  %% monitor()
           , channel_pid     %% pid()
           , channel_monitor %% monitor()
           , subs            %% orddict()
           }).

-record(sub, { state %% {setup, From} open, {close, From}
             }).

%%%_ * API -------------------------------------------------------------
start(Args)      -> gen_server:start(?MODULE, Args, []).
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).

stop(Pid)        ->
  cast(Pid, stop).

subscribe(Pid, From, Queue) ->
  cast(Pid, {subscribe, From, Queue}).

unsubscribe(Pid, From, Queue) ->
  cast(Pid, {unsubscribe, From, Queue}).

publish(Pid, From, Exchange, RoutingKey, Payload) ->
  cast(Pid, {publish, From, Exchange, RoutingKey, Payload}).

declare_exchange(Pid, From, Exchange) ->
  cast(Pid, {declare_exchange, From, Exchange}).

delete_exchange(Pid, From, Exchange) ->
  cast(Pid, {delete_exchange, From, Exchange}).

declare_queue(Pid, From, Queue) ->
  cast(Pid, {declare_queue, From, Queue}).

delete_queue(Pid, From, Queue) ->
  cast(Pid, {delete_queue, From, Queue}).

bind(Pid, From, Queue, Exchange, RoutingKey) ->
  cast(Pid, {bind, From, Queue, Exchange, RoutingKey}).

unbind(Pid, From, Queue, Exchange, RoutingKey) ->
  cast(Pid, {unbind, From, Queue, Exchange, RoutingKey}).

cast(Pid, Args) -> gen_server:cast(Pid, Args).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ConnectionPid = orddict:fetch(connection_pid, Args),
  case amqp_connection:open_channel(ConnectionPid) of
    {ok, ChannelPid} ->
      ClientPid = orddict:fetch(client_pid, Args),
      {ok, #s{ client_pid      = ClientPid
             , client_monitor  = erlang:monitor(process, ClientPid)
             , channel_pid     = ChannelPid
             , channel_monitor = erlang:monitor(process, ChannelPid)
             , subs            = orddict:new()
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(stop, S) ->
  {stop, normal, S};

handle_cast({subscribe, From, Queue},
            #s{ channel_pid = ChannelPid
              , subs        = Subs0} = S) ->
  case orddict:find(Queue, Subs0) of
    {ok, #sub{state = open}} ->
      %% gen_server:reply(From, {error, already_subscribed}),
      gen_server:reply(From, {ok, self()}),
      {noreply, S};
    {ok, #sub{state = {setup, _From}}} ->
      gen_server:reply(From, {error, setup_in_progress}),
      {noreply, S};
    {ok, #sub{state = {close, _From}}} ->
      gen_server:reply(From, {error, close_in_progress}),
      {noreply, S};
    error ->
      Qos = #'basic.qos'{prefetch_count = 1},
      amqp_channel:call(ChannelPid, Qos),
      Consume = #'basic.consume'{ queue        = Queue
                                , consumer_tag = Queue
                                },
      #'basic.consume_ok'{consumer_tag = Queue} =
        amqp_channel:call(ChannelPid, Consume),
      Sub  = #sub{state = {setup, From}},
      Subs = orddict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}}
  end;

handle_cast({unsubscribe, From, Queue},
            #s{ channel_pid   = ChannelPid
              , subs          = Subs0} = S) ->
  case orddict:fetch(Queue, Subs0) of
    #sub{state = {close, _From}} ->
      gen_server:reply(From, {error, close_in_progress}),
      {noreply, S};
    #sub{state = {setup, _From}} ->
      gen_server:reply(From, {error, setup_in_progress}),
      {noreply, S};
    #sub{state = open} = Sub ->
      Cancel = #'basic.cancel'{consumer_tag = Queue},
      amqp_channel:call(ChannelPid, Cancel),
      Subs = orddict:store(Queue, Sub#sub{state = {close, From}}, Subs0),
      {noreply, S#s{subs = Subs}};
    error ->
      gen_server:reply(From, {error, not_subscribed}),
      {noreply, S}
  end;

handle_cast({publish, From, Exchange, RoutingKey, Payload},
            #s{channel_pid = ChannelPid} = S) ->
  Publish = #'basic.publish'{ exchange    = Exchange
                            , routing_key = RoutingKey
                            , mandatory   = true
                            , immediate   = true
                            },
  Props = #'P_basic'{delivery_mode = 2}, %% 1 not persistent
                                         %% 2 persistent
  Msg = #amqp_msg{ payload = Payload
                 , props   = Props
                 },
  amqp_channel:call(ChannelPid, Publish, Msg),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({declare_exchange, From, Exchange},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'exchange.declare'{exchange = Exchange},
  #'exchange.declare_ok'{} = amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({delete_exchange, From, Exchange},
            #s{channel_pid = ChannelPid} = S) ->
  Delete = #'exchange.delete'{exchange = Exchange},
  #'exchange.delete_ok'{} = amqp_channel:call(ChannelPid, Delete),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({declare_queue, From, Queue},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'queue.declare'{queue = Queue},
  #'queue.declare_ok'{} = amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({delete_queue, From, Queue},
            #s{channel_pid = ChannelPid} = S) ->
  Delete = #'queue.delete'{queue = Queue},
  #'queue.delete_ok'{} = amqp_channel:call(ChannelPid, Delete),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({bind, From, Queue, Exchange, RoutingKey},
            #s{channel_pid = ChannelPid} = S) ->
  Binding = #'queue.bind'{ queue       = Queue
                         , exchange    = Exchange
                         , routing_key = RoutingKey},
  #'queue.bind_ok'{} = amqp_channel:call(ChannelPid, Binding),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({unbind, From, Queue, Exchange, RoutingKey},
            #s{channel_pid = ChannelPid} = S) ->
  Binding = #'queue.unbind'{ queue       = Queue
                           , exchange    = Exchange
                           , routing_key = RoutingKey},
  #'queue.unbind_ok'{} = amqp_channel:call(ChannelPid, Binding),
  gen_server:reply(From, ok),
  {noreply, S}.

handle_info(#'basic.consume_ok'{consumer_tag = Queue},
            #s{subs = Subs0} = S) ->
  #sub{state = {setup, From}} = Sub = orddict:fetch(Queue, Subs0),
  gen_server:reply(From, {ok, self()}),
  Subs = orddict:store(Queue, Sub#sub{state = open}, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info(#'basic.cancel_ok'{consumer_tag = Queue},
            #s{subs = Subs0} = S) ->
  #sub{state = {close, From}} = orddict:fetch(Queue, Subs0),
  gen_server:reply(From, ok),
  Subs = orddict:erase(Queue, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info({#'basic.deliver'{delivery_tag = Queue}, Payload},
            #s{client_pid = ClientPid} = S) ->
  ClientPid ! {{msg, self()}, Queue, Payload},
  {noreply, S};

handle_info({#'basic.return'{ reply_text = <<"unroutable">>
                            , exchange   = Exchange}, Payload}, _S) ->
  %% slightly drastic for now.
  {stop, {unroutable, Exchange, Payload}};

handle_info({ack, Tag}, #s{channel_pid = ChannelPid} = S) ->
  Ack = #'basic.ack'{delivery_tag = Tag},
  amqp_channel:cast(ChannelPid, Ack),
  {noreply, S};

handle_info({'DOWN', ChannelRef, process, ChannelPid, Rsn},
            #s{ channel_pid     = ChannelPid
              , channel_monitor = ChannelRef} = S) ->
  %%simple_amqp_server:cleanup(S#s.client_pid),
  %%{noreply, S};
  {stop, Rsn, S};

handle_info({'DOWN', ClientRef, process, ClientPid, _Rsn},
            #s{ client_pid     = ClientPid
              , client_monitor = ClientRef} = S) ->
  error_logger:info_msg("Channel died: ~p~n", [_Rsn]),
  ok = simple_amqp_server:cleanup(ClientPid),
  {noreply, S};

handle_info(Info, S) ->
  io:format("WEIRD INFO: ~p~n", [Info]),
  {noreply, S}.

terminate(_Reason, #s{ channel_pid     = ChannelPid
                     , channel_monitor = ChannelMonitor
                     , client_monitor  = ClientMonitor}) ->
  erlang:demonitor(ChannelMonitor, [flush]),
  erlang:demonitor(ClientMonitor,  [flush]),
  ok = amqp_channel:close(ChannelPid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
