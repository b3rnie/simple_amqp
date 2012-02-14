%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp interface
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp).

%%%_* Exports ==========================================================
-export([ subscribe/1
        , subscribe/2
        , unsubscribe/1
        , unsubscribe/2
        , publish/3
        , publish/4
        , exchange_declare/1
        , exchange_declare/2
        , exchange_delete/1
        , exchange_delete/2
        , queue_declare/1
        , queue_declare/2
        , queue_delete/1
        , queue_delete/2
        , bind/3
        , unbind/3
        , cleanup/0
        ]).
%%%_* Includes =========================================================
-include_lib("simple_amqp/include/simple_amqp.hrl").

%%%_ * API -------------------------------------------------------------
%% @spec subscribe(Queue) -> {ok, Pid::pid()} | {error, Rsn}
%% @doc Subscribe to a queue, returned pid is the channel 'handler'
%%      and can be monitored/linked
subscribe(Queue) ->
  subscribe(Queue, []).

subscribe(Queue, Ops)
  when is_binary(Queue) ->
  simple_amqp_server:cmd(subscribe, [Queue, Ops]).

%% @spec unsubscribe(Queue::binary()) -> ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Queue) ->
  unsubscribe(Queue, []).

unsubscribe(Queue, Ops)
  when is_binary(Queue) ->
  simple_amqp_server:cmd(unsubscribe, [Queue, Ops]).

%% @spec publish(Exchange, RoutingKey, Payload) -> ok
%% @doc Publish a message
%% xxx unroutable requests wont be handled very nicely
publish(Exchange, RoutingKey, Payload) ->
  publish(Exchange, RoutingKey, Payload, []).

publish(Exchange, RoutingKey, Payload, Ops)
  when is_binary(Exchange),
       is_binary(RoutingKey),
       is_binary(Payload) ->
  simple_amqp_server:cmd(publish,
                         [Exchange, RoutingKey, Payload, Ops]).

%% @spec exchange_declare(Exchange) -> ok
%% @doc declare a new exchange
exchange_declare(Exchange) ->
  exchange_declare(Exchange, []).

exchange_declare(Exchange, Ops)
  when is_binary(Exchange) ->
  simple_amqp_server:cmd(exchange_declare, [Exchange, Ops]).

%% @spec exchange_delete(Exchange) -> ok
%% @doc delete an exchange
exchange_delete(Exchange) ->
  exchange_delete(Exchange, []).

exchange_delete(Exchange, Ops)
  when is_binary(Exchange) ->
  simple_amqp_server:cmd(exchange_delete, [Exchange, Ops]).

%% @spec queue_declare(Queue) -> {ok, Queue}
%% @doc declare a new queue
queue_declare(Queue) ->
  queue_declare(Queue, []).

queue_declare(Queue, Ops)
  when is_binary(Queue) ->
  simple_amqp_server:cmd(queue_declare, [Queue, Ops]).

%% @spec queue_delete(Queue) -> ok
%% @doc delete a queue
queue_delete(Queue) ->
  queue_delete(Queue, []).

queue_delete(Queue, Ops)
  when is_binary(Queue) ->
  simple_amqp_server:cmd(queue_delete, [Queue, Ops]).

%% @spec bind(Queue, Exchange, RoutingKey) -> ok
%% @doc create routing rule
bind(Queue, Exchange, RoutingKey)
  when is_binary(Queue),
       is_binary(Exchange),
       is_binary(RoutingKey) ->
  simple_amqp_server:cmd(bind, [Queue, Exchange, RoutingKey]).

%% @spec unbind(Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc remove a routing rule
unbind(Queue, Exchange, RoutingKey)
  when is_binary(Queue),
       is_binary(Exchange),
       is_binary(RoutingKey) ->
  simple_amqp_server:cmd(unbind, [Queue, Exchange, RoutingKey]).

%% @spec cleanup() -> ok
%% @doc Explicitly leanup after process (done automatically when
%%      'client' dies otherwise)
cleanup() ->
  simple_amqp_server:cleanup().

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  ok = application:start(?MODULE),
  wait_for_connections(),
  Queue    = <<"test_queue">>,
  Exchange = <<"test_exchange">>,
  RK       = <<"test_routing_key">>,
  Queue    = basic_setup(Exchange, Queue, RK),
  Daddy    = self(),
  Consumer = proc_lib:spawn_link(fun() ->
                                     basic_consumer(Daddy, Queue)
                                 end),
  Producer = proc_lib:spawn_link(fun() ->
                                     basic_producer(Daddy, Exchange, RK)
                                 end),
  receive {ok, Consumer} -> ok end,
  ok = basic_close(Exchange, Queue, RK),
  lists:foreach(fun(_Pid) ->
                    ok = simple_amqp:cleanup()
                end, [Daddy, Consumer, Producer]),
  application:stop(?MODULE),
  ok.

basic_producer(_Daddy, X, RK) ->
  F = fun(Bin) -> simple_amqp:publish(X, RK, Bin) end,
  lists:foreach(F, basic_dataset()).

basic_consumer(Daddy, Queue) ->
  {error, not_subscribed} = simple_amqp:unsubscribe(Queue),
  {ok, Pid} = simple_amqp:subscribe(Queue),
  {ok, Pid} = simple_amqp:subscribe(Queue),
  basic_consumer_consume(Pid, basic_dataset()),
  ok = simple_amqp:unsubscribe(Queue),
  {error, not_subscribed} = simple_amqp:unsubscribe(Queue),
  Daddy ! {ok, self()}.

basic_consumer_consume(_Pid, []) -> ok;
basic_consumer_consume(Pid, Dataset) ->
  receive
    #simple_amqp_deliver{ pid          = Pid
                        , delivery_tag = DeliveryTag
                        , payload      = Payload
                        , message_id   = _MsgId
                        } ->
      Pid ! #simple_amqp_ack{delivery_tag = DeliveryTag},
      basic_consumer_consume(Pid, Dataset -- [Payload])
  end.

basic_setup(X, Q0, RK) ->
  ok      = simple_amqp:exchange_declare(X),
  {ok, Q} = simple_amqp:queue_declare(Q0),
  ok      = simple_amqp:bind(Q, X, RK),
  Q.

basic_close(X, Q, RK) ->
  ok = simple_amqp:unbind(Q, X, RK),
  ok = simple_amqp:queue_delete(Q),
  ok = simple_amqp:exchange_delete(X).

basic_dataset() ->
  [term_to_binary(Term) || Term <- [1, 2, 3, 4]].

wait_for_connections() ->
  case simple_amqp_server:open_connections() == 0 of
    true  -> timer:sleep(100),
             wait_for_connections();
    false -> ok
  end.

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
