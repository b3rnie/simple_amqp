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

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { client_pid %% pid()
           , channel    %% {pid(), ref}
           , subs       %% dict()
           }).

-record(sub, { state %% {setup, From} open, {close, From}
             }).

%%%_ * API -------------------------------------------------------------
start(Args)      -> gen_server:start(?MODULE, Args, []).
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).

stop(Pid)                 -> cast(Pid, stop).
cmd(Pid, Cmd, Args, From) -> cast(Pid, {cmd, Cmd, Args, From}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  ConnectionPid = proplists:get_value(connection_pid, Args),
  ClientPid     = proplists:get_value(client_pid, Args),

  case amqp_connection:open_channel(ConnectionPid) of
    {ok, ChannelPid} ->
      {ok, #s{ client_pid = ClientPid
             , channel    = {ChannelPid, erlang:monitor(process, ChannelPid)}
             , subs       = dict:new()
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(stop, S) ->
  {stop, normal, S};

handle_cast({cmd, Cmd, Args, From}, S0) ->
  case safe_do_cmd(Cmd, Args, From, S0) of
    {reply, Res, S} ->
      gen_server:reply(From, Res),
      {noreply, S};
    {noreply, S} ->
      {noreply, S}
  end.

handle_info(Info, S0) ->
  case do_info(Info, S0) of
    {noreply, S}   -> {noreply, S};
    {stop, Rsn, S} -> {stop, Rsn, S}
  end.

terminate(_Rsn, #s{channel = {Pid, Ref}}) ->
  ok = amqp_channel:close(Pid),
  erlang:demonitor(Ref, [flush]).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
safe_do_cmd(Cmd, Args, From, S) ->
  try do_cmd(Cmd, Args, From, S)
  catch Class:Term ->
      {reply, {error, {Class, Term}}, S}
  end.

do_cmd(subscribe, [Queue, Ops], From,
       #s{ channel = {Pid, _Ref}
         , subs    = Subs0} = S) ->
  case dict:find(Queue, Subs0) of
    {ok, #sub{state = open}} ->
      {reply, {ok, self()}, S};
    {ok, #sub{state = {setup, _From}}} ->
      {reply, {error, setup_in_progress}, S};
    {ok, #sub{state = {close, _From}}} ->
      {reply, {error, close_in_progress}, S};
    error ->
      Qos = #'basic.qos'{prefetch_count = 1},
      #'basic.qos_ok'{} = amqp_channel:call(Pid, Qos),
      Consume = #'basic.consume'{
         queue        = Queue
       , consumer_tag = Queue
       , no_ack       = ops(no_ack, Ops, false)
       , exclusive    = ops(exclusive, Ops, false)
       },
      #'basic.consume_ok'{consumer_tag = Queue} =
        amqp_channel:call(Pid, Consume),
      Sub  = #sub{state = {setup, From}},
      Subs = dict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}}
  end;

do_cmd(unsubscribe, [Queue, _Ops], From,
       #s{ channel = {Pid, _Ref}
         , subs    = Subs0} = S) ->
  case dict:find(Queue, Subs0) of
    {ok, #sub{state = {close, _From}}} ->
      {reply, {error, close_in_progress}, S};
    {ok, #sub{state = {setup, _From}}} ->
      {reply, {error, setup_in_progress}, S};
    {ok, #sub{state = open} = Sub0} ->
      Cancel = #'basic.cancel'{consumer_tag = Queue},
      #'basic.cancel_ok'{} = amqp_channel:call(Pid, Cancel),
      Sub = Sub0#sub{state = {close, From}},
      Subs = dict:store(Queue, Sub, Subs0),
      {noreply, S#s{subs = Subs}};
    error ->
      {reply, {error, not_subscribed}, S}
  end;

do_cmd(publish, [Exchange, RoutingKey, Payload, Ops], _From,
       #s{channel = {Pid, _Ref}} = S) ->
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
  ok = amqp_channel:cast(Pid, Publish, Msg),
  {reply, ok, S};

do_cmd(exchange_declare, [Exchange, Ops], _From,
       #s{channel = {Pid, _Ref}} = S) ->
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
  #'exchange.declare_ok'{} = amqp_channel:call(Pid, Declare),
  {reply, ok, S};

do_cmd(exchange_delete, [Exchange, Ops], _From,
       #s{channel = {Pid, _Ref}} = S) ->
  Delete = #'exchange.delete'{
     exchange  = Exchange
   , ticket    = ops(ticket, Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , nowait    = ops(nowait, Ops, false)
   },
  #'exchange.delete_ok'{} = amqp_channel:call(Pid, Delete),
  {reply, ok, S};

do_cmd(queue_declare, [Queue0, Ops], _From,
       #s{channel = {Pid, _Ref}} = S) ->
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
    amqp_channel:call(Pid, Declare),
  {reply, {ok, Queue}, S};

do_cmd(queue_delete, [Queue, Ops], _From,
       #s{channel = {Pid, _Ref}} = S) ->
  Delete = #'queue.delete'{
     queue = Queue
   , ticket    = ops(ticket, Ops, 0)
   , if_unused = ops(if_unused, Ops, false)
   , if_empty  = ops(if_empty,  Ops, false)
   , nowait    = ops(nowait,    Ops, false)
   },
  #'queue.delete_ok'{message_count = _MessageCount} =
    amqp_channel:call(Pid, Delete),
  {reply, ok, S};

do_cmd(bind, [Queue, Exchange, RoutingKey], _From,
       #s{channel = {Pid, _Ref}} = S) ->
  Binding = #'queue.bind'{ queue       = Queue
                         , exchange    = Exchange
                         , routing_key = RoutingKey},
  #'queue.bind_ok'{} = amqp_channel:call(Pid, Binding),
  {reply, ok, S};

do_cmd(unbind, [Queue, Exchange, RoutingKey], _From,
       #s{channel = {Pid, _Ref}} = S) ->
  Binding = #'queue.unbind'{ queue       = Queue
                           , exchange    = Exchange
                           , routing_key = RoutingKey},
  #'queue.unbind_ok'{} = amqp_channel:call(Pid, Binding),
  {reply, ok, S}.

do_info(#'basic.consume_ok'{consumer_tag = Queue},
        #s{subs = Subs0} = S) ->
  error_logger:info_msg("basic consume (~p): tag = ~p~n",
                        [?MODULE, Queue]),
  #sub{state = {setup, From}} = Sub = dict:fetch(Queue, Subs0),
  gen_server:reply(From, {ok, self()}),
  Subs = dict:store(Queue, Sub#sub{state = open}, Subs0),
  {noreply, S#s{subs = Subs}};

do_info(#'basic.cancel_ok'{consumer_tag = Queue},
        #s{subs = Subs0} = S) ->
  error_logger:info_msg("basic.cancel_ok (~p): ~p~n",
                        [?MODULE, Queue]),
  #sub{state = {close, From}} = dict:fetch(Queue, Subs0),
  gen_server:reply(From, ok),
  Subs = dict:erase(Queue, Subs0),
  {noreply, S#s{subs = Subs}};

do_info({#'basic.deliver'{ consumer_tag = ConsumerTag
                         , delivery_tag = DeliveryTag
                         %%, exchange     = Exchange
                         , routing_key  = RoutingKey},
         #amqp_msg{payload = Payload}},
        #s{client_pid = ClientPid} = S) ->
  error_logger:info_msg("basic deliver (~p): ~p~n",
                        [?MODULE, ConsumerTag]),
  ClientPid ! {msg, self(), DeliveryTag, RoutingKey, Payload},
  {noreply, S};

do_info({#'basic.return'{ reply_text = <<"unroutable">>
                        , exchange   = Exchange}, Payload}, S) ->
  error_logger:error_msg("unroutable (~p): ~p", [?MODULE, Exchange]),
  %% slightly drastic for now.
  {stop, {unroutable, Exchange, Payload}, S};

do_info({ack, Tag},
        #s{channel = {Pid, _Ref}} = S) ->
  Ack = #'basic.ack'{delivery_tag = Tag},
  amqp_channel:cast(Pid, Ack),
  {noreply, S};

do_info({'DOWN', Ref, process, Pid, Rsn},
        #s{channel = {Pid, Ref}} = S) ->
  error_logger:error_msg("channel died (~p): ~p~n", [?MODULE, Rsn]),
  {stop, Rsn, S};

do_info(Info, S) ->
  error_logger:info_msg("weird info msg, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

ops(K,Ops,Def) -> proplists:get_value(K, Ops, Def).

cast(Pid, Args) -> gen_server:cast(Pid, Args).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
