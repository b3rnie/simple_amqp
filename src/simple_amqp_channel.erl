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
        , cmd/4
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
-define(amqp_dbg(From, Args), _ = {From, Args}).
%%-define(amqp_dbg(Fmt,Args), io:format(Fmt,Args)).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { client_pid  %% pid()
           , client_ref  %% monitor()
           , channel_pid %% pid()
           , channel_ref %% monitor()
           , subs        %% orddict()
           }).

-record(sub, { state %% {setup, From} open, {close, From}
             }).

%%%_ * API -------------------------------------------------------------
start(Args)      -> gen_server:start(?MODULE, Args, []).
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).

stop(Pid)                 -> cast(Pid, stop).
cmd(Pid, From, Cmd, Args) -> cast(Pid, {cmd, Cmd, Args, From}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ConnectionPid = proplists:get_value(connection_pid, Args),
  case amqp_connection:open_channel(ConnectionPid) of
    {ok, ChannelPid} ->
      ClientPid = proplists:get_value(client_pid, Args),
      {ok, #s{ client_pid  = ClientPid
             , client_ref  = erlang:monitor(process, ClientPid)
             , channel_pid = ChannelPid
             , channel_ref = erlang:monitor(process, ChannelPid)
             , subs        = dict:new()
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(stop, S) ->
  {stop, normal, S};

handle_cast({cmd, subscribe, [Queue, Ops], From},
            #s{ channel_pid = ChannelPid
              , subs        = Subs0} = S) ->
  case dict:find(Queue, Subs0) of
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
      Subs = dict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}}
  end;

handle_cast({cmd, unsubscribe, [Queue, _Ops], From},
            #s{ channel_pid   = ChannelPid
              , subs          = Subs0} = S) ->
  case dict:find(Queue, Subs0) of
    {ok, #sub{state = {close, _From}}} ->
      gen_server:reply(From, {error, close_in_progress}),
      {noreply, S};
    {ok, #sub{state = {setup, _From}}} ->
      gen_server:reply(From, {error, setup_in_progress}),
      {noreply, S};
    {ok, #sub{state = open} = Sub0} ->
      Cancel = #'basic.cancel'{consumer_tag = Queue},
      #'basic.cancel_ok'{} = amqp_channel:call(ChannelPid, Cancel),
      Sub = Sub0#sub{state = {close, From}},
      Subs = dict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}};
    error ->
      gen_server:reply(From, {error, not_subscribed}),
      {noreply, S}
  end;

handle_cast({cmd, publish, [Exchange, RoutingKey, Payload, Ops], From},
            #s{channel_pid = ChannelPid} = S) ->
  Publish = #'basic.publish'{
     exchange    = Exchange
   , routing_key = RoutingKey
   , mandatory   = ops(mandatory, Ops, false) %%true
   , immediate   = ops(immediate, Ops, false) %%true
   },

  MsgId = ops(message_id, Ops, 0),

  Props = #'P_basic'{ delivery_mode = 2  %% 1 not persistent
                                         %% 2 persistent
                    , message_id = MsgId
                    }, 

  Msg = #amqp_msg{ payload = Payload
                 , props   = Props
                 },
  ok = amqp_channel:cast(ChannelPid, Publish, Msg),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({cmd, exchange_declare, [Exchange, Ops], From},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'exchange.declare'{
     exchange    = Exchange
   , ticket      = ops(ticket,      Ops, 0)
   , type        = ops(type,        Ops, <<"direct">>)
   , passive     = ops(passive,     Ops, false)
   , durable     = ops(durable,     Ops, false)
   , auto_delete = ops(auto_delete, Ops, false)
   , internal    = ops(internal,    Ops, false)
   , nowait      = ops(nowait,      Ops, false)
   , arguments   = ops(arguments,   Ops, [])
   },
  #'exchange.declare_ok'{} = amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({cmd, exchange_delete, [Exchange, Ops], From},
            #s{channel_pid = ChannelPid} = S) ->
  Delete = #'exchange.delete'{
     exchange  = Exchange
   , ticket    = ops(ticket, Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , nowait    = ops(nowait, Ops, false)
   },
  #'exchange.delete_ok'{} = amqp_channel:call(ChannelPid, Delete),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({cmd, queue_declare, [Queue0, Ops], From},
            #s{channel_pid = ChannelPid} = S) ->
  Declare = #'queue.declare'{
     queue       = Queue0
   , ticket      = ops(ticket,      Ops, 0)
   , passive     = ops(passive,     Ops, false)
   , exclusive   = ops(exclusive,   Ops, false)
   , durable     = ops(durable,     Ops, false)
   , auto_delete = ops(auto_delete, Ops, false)
   , nowait      = ops(nowait,      Ops, false)
   , arguments   = ops(arguments,   Ops, [])
   },
  #'queue.declare_ok'{queue = Queue} =
    amqp_channel:call(ChannelPid, Declare),
  gen_server:reply(From, {ok, Queue}),
  {noreply, S};

handle_cast({cmd, queue_delete, [Queue, Ops], From},
            #s{channel_pid = ChannelPid} = S) ->
  Delete = #'queue.delete'{
     queue = Queue
   , ticket    = ops(ticket, Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , if_empty  = ops(if_empty,  Ops, false)
   , nowait    = ops(nowait,    Ops, false)
   },
  #'queue.delete_ok'{message_count = _MessageCount} =
    amqp_channel:call(ChannelPid, Delete),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({cmd, bind, [Queue, Exchange, RoutingKey], From},
            #s{channel_pid = ChannelPid} = S) ->
  Binding = #'queue.bind'{ queue       = Queue
                         , exchange    = Exchange
                         , routing_key = RoutingKey},
  #'queue.bind_ok'{} = amqp_channel:call(ChannelPid, Binding),
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({cmd, unbind, [Queue, Exchange, RoutingKey], From},
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
  #sub{state = {setup, From}} = Sub = dict:fetch(Queue, Subs0),
  gen_server:reply(From, {ok, self()}),
  Subs = dict:store(Queue, Sub#sub{state = open}, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info(#'basic.cancel_ok'{consumer_tag = Queue},
            #s{subs = Subs0} = S) ->
  ?amqp_dbg("basic.cancel_ok (consumer_tag = ~p)~n", [Queue]),
  #sub{state = {close, From}} = dict:fetch(Queue, Subs0),
  gen_server:reply(From, ok),
  Subs = dict:erase(Queue, Subs0),
  {noreply, S#s{subs = Subs}};

handle_info({#'basic.deliver'{ consumer_tag = ConsumerTag
                             , delivery_tag = DeliveryTag
                             , exchange     = Exchange
                             , routing_key  = RoutingKey},
             #amqp_msg{ payload = Payload
                      , props = #'P_basic'{ message_id = MsgId }
                      }
            },
            #s{client_pid = ClientPid} = S) ->
  ?amqp_dbg("basic.deliver~n"
            "(consumer_tag = ~p)~n"
            "(delivery_tag = ~p)~n"
            "(exchange     = ~p)~n"
            "(routing_key  = ~p)~n"
            "(message_id   = ~p)~n",
            [ConsumerTag, DeliveryTag, Exchange, RoutingKey, MsgId]),
  ClientPid ! {msg, self(), DeliveryTag, RoutingKey, MsgId, Payload},
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
              , channel_ref = ChannelRef} = S) ->
  error_logger:info_msg("Channel died: ~p~n", [Rsn]),

  %%simple_amqp_server:cleanup(S#s.client_pid),
  %%{noreply, S};
  {stop, Rsn, S};

handle_info({'DOWN', ClientRef, process, ClientPid, Rsn},
            #s{ client_pid     = ClientPid
              , client_ref = ClientRef} = S) ->
  error_logger:info_msg("Client died: ~p~n", [Rsn]),
  %%ok = simple_amqp_server:cleanup(ClientPid),
  {stop, Rsn, S};

handle_info(Info, S) ->
  io:format("WEIRD INFO: ~p~n", [Info]),
  {noreply, S}.

terminate(_Reason, #s{ channel_pid = ChannelPid
                     , channel_ref = ChannelRef
                     , client_ref  = ClientRef}) ->
  erlang:demonitor(ChannelRef, [flush]),
  erlang:demonitor(ClientRef,  [flush]),
  ok = amqp_channel:close(ChannelPid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
ops(K,Ops,Def) -> proplists:get_value(K,Ops,Def).

cast(Pid, Args) -> gen_server:cast(Pid, Args).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
