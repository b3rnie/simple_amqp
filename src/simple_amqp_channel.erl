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
        , subscribe/4
        , unsubscribe/4
        , publish/6
        , exchange_declare/4
        , exchange_delete/4
        , queue_declare/4
        , queue_delete/4
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
-define(amqp_dbg(Fmt,Args), io:format(Fmt,Args)).

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

stop(Pid) ->
  cast(Pid, stop).

subscribe(Pid, From, Queue, Ops) ->
  cast(Pid, {subscribe, From, Queue, Ops}).

unsubscribe(Pid, From, Queue, Ops) ->
  cast(Pid, {unsubscribe, From, Queue, Ops}).

publish(Pid, From, Exchange, RoutingKey, Payload, Ops) ->
  cast(Pid, {publish, From, Exchange, RoutingKey, Payload, Ops}).

exchange_declare(Pid, From, Exchange, Ops) ->
  cast(Pid, {exchange_declare, From, Exchange, Ops}).

exchange_delete(Pid, From, Exchange, Ops) ->
  cast(Pid, {exchange_delete, From, Exchange, Ops}).

queue_declare(Pid, From, Queue, Ops) ->
  cast(Pid, {queue_declare, From, Queue, Ops}).

queue_delete(Pid, From, Queue, Ops) ->
  cast(Pid, {queue_delete, From, Queue, Ops}).

bind(Pid, From, Queue, Exchange, RoutingKey) ->
  cast(Pid, {bind, From, Queue, Exchange, RoutingKey}).

unbind(Pid, From, Queue, Exchange, RoutingKey) ->
  cast(Pid, {unbind, From, Queue, Exchange, RoutingKey}).

cast(Pid, Args) ->
  gen_server:cast(Pid, Args).

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

handle_cast({subscribe, From, Queue, Ops},
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
      #'basic.qos_ok'{} = amqp_channel:call(ChannelPid, Qos),
      Consume = #'basic.consume'{
         queue        = Queue
       , consumer_tag = Queue
       , no_ack       = ops(no_ack, Ops, false)
       , exclusive    = ops(exclusive, Ops, false)
       },
      #'basic.consume_ok'{consumer_tag = Queue} =
        amqp_channel:call(ChannelPid, Consume),
      Sub  = #sub{state = {setup, From}},
      Subs = orddict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}}
  end;

handle_cast({unsubscribe, From, Queue, _Ops},
            #s{ channel_pid   = ChannelPid
              , subs          = Subs0} = S) ->
  case orddict:find(Queue, Subs0) of
    {ok, #sub{state = {close, _From}}} ->
      gen_server:reply(From, {error, close_in_progress}),
      {noreply, S};
    {ok, #sub{state = {setup, _From}}} ->
      gen_server:reply(From, {error, setup_in_progress}),
      {noreply, S};
    {ok, #sub{state = open} = Sub0} ->
      Cancel = #'basic.cancel'{consumer_tag = Queue},
      amqp_channel:call(ChannelPid, Cancel),
      Sub = Sub0#sub{state = {close, From}},
      Subs = orddict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}};
    error ->
      gen_server:reply(From, {error, not_subscribed}),
      {noreply, S}
  end;

handle_cast({publish, From, Exchange, RoutingKey, Payload, Ops},
            #s{channel_pid = ChannelPid} = S) ->
  Publish = #'basic.publish'{
     exchange    = Exchange
   , routing_key = RoutingKey
   , mandatory   = ops(mandatory, Ops, false) %%true
   , immediate   = ops(immediate, Ops, false) %%true
   },

  Props = #'P_basic'{delivery_mode = 2}, %% 1 not persistent
                                         %% 2 persistent
  Msg = #amqp_msg{ payload = Payload
                 , props   = Props
                 },
  amqp_channel:call(ChannelPid, Publish, Msg),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({exchange_declare, From, Exchange, Ops},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'exchange.declare'{
     exchange    = Exchange
   , type        = ops(type, Ops, <<"direct">>)
   , auto_delete = ops(auto_delete, Ops, false)
   },
  #'exchange.declare_ok'{} = amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({exchange_delete, From, Exchange, _Ops},
            #s{channel_pid = ChannelPid} = S) ->
  Delete = #'exchange.delete'{exchange = Exchange},
  #'exchange.delete_ok'{} = amqp_channel:call(ChannelPid, Delete),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({queue_declare, From, Queue0, Ops},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'queue.declare'{
     queue       = Queue0
   , exclusive   = ops(exclusive, Ops, false)
   , durable     = ops(durable, Ops, false)
   , auto_delete = ops(auto_delete, Ops, false)
   },
  #'queue.declare_ok'{queue = Queue} =
    amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, {ok, Queue}),
  {noreply, S};

handle_cast({queue_delete, From, Queue, _Ops},
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
  ?amqp_dbg("basic.consume_ok (consumer_tag = ~p)~n", [Queue]),
  #sub{state = {setup, From}} = Sub = orddict:fetch(Queue, Subs0),
  gen_server:reply(From, {ok, self()}),
  Subs = orddict:store(Queue, Sub#sub{state = open}, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info(#'basic.cancel_ok'{consumer_tag = Queue},
            #s{subs = Subs0} = S) ->
  ?amqp_dbg("basic.cancel_ok (consumer_tag = ~p)~n", [Queue]),
  #sub{state = {close, From}} = orddict:fetch(Queue, Subs0),
  gen_server:reply(From, ok),
  Subs = orddict:erase(Queue, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info({#'basic.deliver'{ consumer_tag = ConsumerTag
                             , delivery_tag = DeliveryTag
                             , exchange     = Exchange
                             , routing_key  = RoutingKey}, Payload},
            #s{client_pid = ClientPid} = S) ->
  ?amqp_dbg("basic.deliver~n"
            "(consumer_tag = ~p)~n"
            "(delivery_tag = ~p)~n"
            "(exchange     = ~p)~n"
            "(routing_key  = ~p)~n",
            [ConsumerTag, DeliveryTag, Exchange, RoutingKey]),
  ClientPid ! {msg, self(), DeliveryTag, RoutingKey, Payload},
  {noreply, S};

handle_info({#'basic.return'{ reply_text = <<"unroutable">> = Text
                            , exchange   = Exchange}, Payload}, _S) ->
  ?amqp_dbg("basic.return~n"
            "(reply_text = ~p)~n"
            "(exchange   = ~p)~n",
            [Text, Exchange]),
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
ops(K,Ops,Def) -> proplists:get_value(K,Ops,Def).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
