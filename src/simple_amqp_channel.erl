%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp channel
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_channel).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start/1
        , open/1
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
-include_lib("simple_amqp/include/simple_amqp.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { connection_pid
           , client_pid %% pid()
           , channel_pid
           , channel_ref
           , subs       %% dict()
           , pubs       %% dict()
           , seen_pubno
           , next_pubno
           }).

-record(sub, { state %% {setup, From} open, {close, From}
             }).

%%%_ * API -------------------------------------------------------------
start(Args)               -> gen_server:start(?MODULE, Args, []).
open(Pid)                 -> cast(Pid, open).
stop(Pid)                 -> cast(Pid, stop).
cmd(Pid, Cmd, Args, From) -> cast(Pid, {cmd, Cmd, Args, From}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ConnectionPid = proplists:get_value(connection_pid, Args),
  ClientPid     = proplists:get_value(client_pid, Args),
  {ok, #s{ connection_pid = ConnectionPid
         , client_pid     = ClientPid
         , subs           = dict:new()
         , pubs           = []
         , next_pubno     = 1
         , seen_pubno     = 0
         }}.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(open, #s{connection_pid = ConnectionPid} = S) ->
  case amqp_connection:open_channel(ConnectionPid) of
    {ok, ChannelPid} ->
      #'confirm.select_ok'{} =
        amqp_channel:call(ChannelPid, #'confirm.select'{}),
      ok = amqp_channel:register_confirm_handler(ChannelPid, self()),
      {noreply, S#s{ channel_pid = ChannelPid
                   , channel_ref = erlang:monitor(process, ChannelPid)}};
    {error, Rsn} ->
      {stop, Rsn, S}
  end;

handle_cast({cmd, Cmd, Args, From}, S0) ->
  S = safe_do_cmd(Cmd, Args, From, S0),
  {noreply, S};

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info(#'basic.consume_ok'{consumer_tag = Queue}, S) ->
  error_logger:info_msg("basic consume (~p): tag = ~p~n",
                        [?MODULE, Queue]),
  #sub{state = {setup, From}} = Sub = dict:fetch(Queue, S#s.subs),
  Subs = dict:store(Queue, Sub#sub{state = open}, S#s.subs),
  gen_server:reply(From, {ok, self()}),
  {noreply, S#s{subs = Subs}};

handle_info(#'basic.cancel_ok'{consumer_tag = Queue}, S) ->
  error_logger:info_msg("basic.cancel_ok (~p): ~p~n",
                        [?MODULE, Queue]),
  #sub{state = {close, From}} = dict:fetch(Queue, S#s.subs),
  gen_server:reply(From, ok),
  {noreply, S#s{subs = dict:erase(Queue, S#s.subs)}};

handle_info({#'basic.deliver'{ consumer_tag = ConsumerTag
                             , delivery_tag = DeliveryTag
                             , exchange     = Exchange
                             , routing_key  = RoutingKey},
             #amqp_msg{ payload = Payload
                      , props   = Props}}, S) ->
  error_logger:info_msg("basic deliver (~p): ~p~n",
                        [?MODULE, ConsumerTag]),

  #'P_basic'{ reply_to       = To
            , correlation_id = Id
            , message_id     = MsgId} = Props,
  %% xxx figure out something nicer
  Dlv = #simple_amqp_deliver{ pid            = self()
                            , consumer_tag   = ConsumerTag
                            , delivery_tag   = DeliveryTag
                            , exchange       = Exchange
                            , routing_key    = RoutingKey
                            , payload        = Payload
                            , reply_to       = To
                            , correlation_id = Id
                            , message_id     = MsgId
                            },
  S#s.client_pid ! Dlv,
  {noreply, S};

handle_info({#'basic.return'{ reply_text = <<"unroutable">>
                            , exchange   = Exchange}, Payload}, S) ->
  error_logger:error_msg("unroutable (~p): ~p", [?MODULE, Exchange]),
  %% slightly drastic for now.
  {stop, {unroutable, Exchange, Payload}, S};

handle_info(#'basic.ack'{ delivery_tag = Tag
                        , multiple     = Multiple
                        }, S0) ->
  S = respond_pubs(ok, Tag, Multiple, S0),
  {noreply, S};

handle_info(#'basic.nack'{ delivery_tag = Tag
                         , multiple     = Multiple
                         }, S0) ->
  S = respond_pubs(error, Tag, Multiple, S0),
  {noreply, S};

handle_info(#simple_amqp_ack{delivery_tag = Tag}, S) ->
  Ack = #'basic.ack'{delivery_tag = Tag},
  amqp_channel:cast(S#s.channel_pid, Ack),
  {noreply, S};

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{ channel_pid = Pid
              , channel_ref = Ref} = S) ->
  error_logger:error_msg("channel died (~p): ~p~n", [?MODULE, Rsn]),
  {stop, Rsn, S};

handle_info(Info, S) ->
  error_logger:info_msg("weird info msg, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

terminate(_Rsn, S) ->
  error_logger:info_msg("closing channel (~p): ~p~n",
                        [?MODULE, S#s.channel_pid]),
  ok = amqp_channel:close(S#s.channel_pid),
  erlang:demonitor(S#s.channel_ref, [flush]).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
safe_do_cmd(Cmd, Args, From, S) ->
  try do_cmd(Cmd, Args, From, S)
  catch Class:Term ->
      gen_server:reply(From, {error, Term}),
      S
  end.

do_cmd(subscribe, [Queue, Ops], From, S) ->
  case dict:find(Queue, S#s.subs) of
    {ok, #sub{state = open}} ->
      gen_server:reply(From, {ok, self()}),
      S;
    {ok, #sub{state = {State, _From}}}
      when State == setup;
           State == close ->
      gen_server:reply(From, {error, in_progress}),
      S;
    error ->
      Qos = #'basic.qos'{prefetch_count = 1},
      #'basic.qos_ok'{} = amqp_channel:call(S#s.channel_pid, Qos),
      Consume = #'basic.consume'{
         queue        = Queue
       , consumer_tag = Queue
       , no_ack       = ops(no_ack,    Ops, false)
       , exclusive    = ops(exclusive, Ops, false)
       },
      #'basic.consume_ok'{consumer_tag = Queue} =
        amqp_channel:call(S#s.channel_pid, Consume),
      Sub = #sub{state = {setup, From}},
      S#s{subs = dict:store(Queue, Sub, S#s.subs)}
  end;

do_cmd(unsubscribe, [Queue, _Ops], From, S) ->
  case dict:find(Queue, S#s.subs) of
    {ok, #sub{state = {State, _From}}}
      when State == close;
           State == setup ->
      gen_server:reply(From, {error, in_progress}),
      S;
    {ok, #sub{state = open} = Sub0} ->
      Cancel = #'basic.cancel'{consumer_tag = Queue},
      #'basic.cancel_ok'{} = amqp_channel:call(S#s.channel_pid, Cancel),
      Sub = Sub0#sub{state = {close, From}},
      S#s{subs = dict:store(Queue, Sub, S#s.subs)};
    error ->
      gen_server:reply(From, {error, not_subscribed}),
      S
  end;

do_cmd(publish, [Exchange, RoutingKey, Payload, Ops], From, S) ->
  Publish = #'basic.publish'{
     exchange    = Exchange
   , routing_key = RoutingKey
   , mandatory   = ops(mandatory, Ops, false) %%true
   , immediate   = ops(immediate, Ops, false) %%true
   },

  Props = #'P_basic'{
     delivery_mode  = ops(delivery_mode,  Ops, 2)  %% 1 not persistent,
                                                   %% 2 persistent
   , correlation_id = ops(correlation_id, Ops, undefined)
   , message_id     = ops(message_id, Ops, <<0>>)
   , reply_to       = ops(reply_to, Ops, undefined)
   },

  Msg = #amqp_msg{ payload = Payload
                 , props   = Props
                 },
  ok = amqp_channel:cast(S#s.channel_pid, Publish, Msg),
  S#s{ next_pubno = S#s.next_pubno+1
     , pubs       = [{S#s.next_pubno, From} | S#s.pubs]
     };

do_cmd(exchange_declare, [Exchange, Ops], From, S) ->
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
  #'exchange.declare_ok'{} =
    amqp_channel:call(S#s.channel_pid, Declare),
  gen_server:reply(From, ok),
  S;

do_cmd(exchange_delete, [Exchange, Ops], From, S) ->
  Delete = #'exchange.delete'{
     exchange  = Exchange
   , ticket    = ops(ticket,    Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , nowait    = ops(nowait,    Ops, false)
   },
  #'exchange.delete_ok'{} = amqp_channel:call(S#s.channel_pid, Delete),
  gen_server:reply(From, ok),
  S;

do_cmd(queue_declare, [Queue0, Ops], From, S) ->
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
    amqp_channel:call(S#s.channel_pid, Declare),
  gen_server:reply(From, {ok, Queue}),
  S;

do_cmd(queue_delete, [Queue, Ops], From, S) ->
  Delete = #'queue.delete'{
     queue = Queue
   , ticket    = ops(ticket, Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , if_empty  = ops(if_empty,  Ops, false)
   , nowait    = ops(nowait,    Ops, false)
   },
  #'queue.delete_ok'{message_count = _MessageCount} =
    amqp_channel:call(S#s.channel_pid, Delete),
  gen_server:reply(From, ok),
  S;

do_cmd(bind, [Queue, Exchange, RoutingKey], From, S) ->
  Binding = #'queue.bind'{ queue       = Queue
                         , exchange    = Exchange
                         , routing_key = RoutingKey},
  #'queue.bind_ok'{} = amqp_channel:call(S#s.channel_pid, Binding),
  gen_server:reply(From, ok),
  S;

do_cmd(unbind, [Queue, Exchange, RoutingKey], From, S) ->
  Binding = #'queue.unbind'{ queue       = Queue
                           , exchange    = Exchange
                           , routing_key = RoutingKey},
  #'queue.unbind_ok'{} = amqp_channel:call(S#s.channel_pid, Binding),
  gen_server:reply(From, ok),
  S.

respond_pubs(What, Tag, true = _Multiple, S) ->
  Pubs = lists:filter(fun({ Id, _From}) when Id > Tag -> true;
                         ({_Id,  From}) ->
                          gen_server:reply(From, What),
                          false
                      end, S#s.pubs),
  S#s{pubs = Pubs};

respond_pubs(What, Tag, false = _Multiple, S) ->
  case lists:keytake(Tag, 1, S#s.pubs) of
    {value, {Tag, From}, Pubs} -> gen_server:reply(From, What),
                                  S#s{pubs = Pubs};
    false ->                      S
  end.

ops(K,Ops,Def) -> proplists:get_value(K, Ops, Def).

cast(Pid, Args) -> gen_server:cast(Pid, Args).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
