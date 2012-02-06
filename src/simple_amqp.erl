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

%%%_ * API -------------------------------------------------------------
%% @spec subscribe(Queue) -> {ok, Pid::pid()} | {error, Rsn}
%% @doc Subscribe to a queue, returned pid is the channel 'handler'
%%      and can be monitored/linked
subscribe(Queue) ->
  subscribe(Queue, []).

subscribe(Queue, Ops) ->
  simple_amqp_server:cmd(subscribe, [Queue, Ops]).

%% @spec unsubscribe(Queue::binary()) -> ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Queue) ->
  unsubscribe(Queue, []).

unsubscribe(Queue, Ops) ->
  simple_amqp_server:cmd(unsubscribe, [Queue, Ops]).

%% @spec unsubscribe(Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc Publish a message
publish(Exchange, RoutingKey, Payload) ->
  publish(Exchange, RoutingKey, Payload, []).

publish(Exchange, RoutingKey, Payload, Ops) ->
  simple_amqp_server:cmd(publish,
                         [Exchange, RoutingKey, Payload, Ops]).

%% @spec exchange_declare(Exchange) -> ok | {error, Rsn}
%% @doc declare a new exchange
exchange_declare(Exchange) ->
  exchange_declare(Exchange, []).

exchange_declare(Exchange, Ops) ->
  simple_amqp_server:cmd(exchange_declare, [Exchange, Ops]).

%% @spec exchange_delete(Exchange) -> ok | {error, Rsn}
%% @doc delete an exchange
exchange_delete(Exchange) ->
  exchange_delete(Exchange, []).

exchange_delete(Exchange, Ops) ->
  simple_amqp_server:cmd(exchange_delete, [Exchange, Ops]).

%% @spec queue_declare(Queue) -> {ok, Queue} | {error, Rsn}
%% @doc declare a new queue
queue_declare(Queue) ->
  queue_declare(Queue, []).

queue_declare(Queue, Ops) ->
  simple_amqp_server:cmd(queue_declare, [Queue, Ops]).

%% @spec queue_delete( Queue) -> ok | {error, Rsn}
%% @doc delete a queue
queue_delete(Queue) ->
  queue_delete(Queue, []).

queue_delete(Queue, Ops) ->
  simple_amqp_server:cmd(queue_delete, [Queue, Ops]).

%% @spec bind(Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc create routing rule
bind(Queue, Exchange, RoutingKey) ->
  simple_amqp_server:cmd(bind, [Queue, Exchange, RoutingKey]).

%% @spec unbind(Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc remove a routing rule
unbind(Queue, Exchange, RoutingKey) ->
  simple_amqp_server:cmd(unbind, [Queue, Exchange, RoutingKey]).

%% @spec cleanup() -> ok
%% @doc Cleanup after process
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
  ok = simple_amqp:cleanup(),
  ok = simple_amqp:cleanup(),
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

basic_consumer_consume(Pid, []) -> ok;
basic_consumer_consume(Pid, Dataset) ->
  receive
    {msg, Pid, DeliveryTag, _RK, Payload, _To, _Id} ->
      Pid ! {ack, DeliveryTag},
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
