%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp interface
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp).

%%%_* Exports ==========================================================
-export([ subscribe/2
        , subscribe/3
        , unsubscribe/2
        , unsubscribe/3
        , publish/4
        , publish/5
        , exchange_declare/2
        , exchange_declare/3
        , exchange_delete/2
        , exchange_delete/3
        , queue_declare/2
        , queue_declare/3
        , queue_delete/2
        , queue_delete/3
        , bind/4
        , unbind/4
        , cleanup/1
        ]).

%%%_ * API -------------------------------------------------------------
%% @spec subscribe(Pid::pid(), Queue::binary()) ->
%%                 {ok, Pid::pid()} | {error, Rsn}
%% @doc Subscribe to a queue, returned pid is the channel 'handler'
%%      and can be monitored/linked
subscribe(Pid, Queue) ->
  subscribe(Pid, Queue, []).

subscribe(Pid, Queue, Ops) ->
  simple_amqp_server:cmd(subscribe, [Queue, Ops], Pid).

%% @spec unsubscribe(Pid::pid(), Queue::binary()) ->
%%                   ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Pid, Queue) ->
  unsubscribe(Pid, Queue, []).

unsubscribe(Pid, Queue, Ops) ->
  simple_amqp_server:cmd(unsubscribe, [Queue, Ops], Pid).

%% @spec unsubscribe(Pid::pid(), Exchange::binary(),
%%                   RoutingKey::binary(), Payload::any()) ->
%%                   ok | {error, Rsn}
%% @doc Publish a message
publish(Pid, Exchange, RoutingKey, Payload) ->
  publish(Pid, Exchange, RoutingKey, Payload, []).

publish(Pid, Exchange, RoutingKey, Payload, Ops) ->
  simple_amqp_server:cmd(publish,
                         [Exchange, RoutingKey, Payload, Ops],
                         Pid).

%% @spec exchange_declare(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc declare a new exchange
exchange_declare(Pid, Exchange) ->
  exchange_declare(Pid, Exchange, []).

exchange_declare(Pid, Exchange, Ops) ->
  simple_amqp_server:cmd(exchange_declare, [Exchange, Ops], Pid).

%% @spec exchange_delete(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc delete an exchange
exchange_delete(Pid, Exchange) ->
  exchange_delete(Pid, Exchange, []).

exchange_delete(Pid, Exchange, Ops) ->
  simple_amqp_server:cmd(exchange_delete, [Exchange, Ops], Pid).

%% @spec queue_declare(Pid::pid(), Queue) -> {ok, Queue} | {error, Rsn}
%% @doc declare a new queue
queue_declare(Pid, Queue) ->
  queue_declare(Pid, Queue, []).

queue_declare(Pid, Queue, Ops) ->
  simple_amqp_server:cmd(queue_declare, [Queue, Ops], Pid).

%% @spec queue_delete(Pid::pid(), Queue) -> ok | {error, Rsn}
%% @doc delete a queue
queue_delete(Pid, Queue) ->
  queue_delete(Pid, Queue, []).

queue_delete(Pid, Queue, Ops) ->
  simple_amqp_server:cmd(queue_delete, [Queue, Ops], Pid).

%% @spec bind(Pid::pid(), Queue, Exchange, RoutingKey) ->
%%            ok | {error, Rsn}
%% @doc create routing rule
bind(Pid, Queue, Exchange, RoutingKey) ->
  simple_amqp_server:cmd(bind, [Queue, Exchange, RoutingKey], Pid).

%% @spec unbind(Pid::pid(), Queue, Exchange, RoutingKey) ->
%%              ok | {error, Rsn}
%% @doc remote a routing rule
unbind(Pid, Queue, Exchange, RoutingKey) ->
  simple_amqp_server:cmd(unbind, [Queue, Exchange, RoutingKey], Pid).

%% @spec unsubscribe(Pid::pid()) -> ok | {error, Rsn}
%% @doc Cleanup after pid (channel, gen_server, etc).
cleanup(Pid) ->
  simple_amqp_server:cleanup(Pid).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
  ok = application:start(?MODULE),
  Queue    = <<"test_queue">>,
  Exchange = <<"test_exchange">>,
  RK       = <<"test_routing_key">>,
  Queue    = basic_setup(self(), Exchange, Queue, RK),
  Daddy    = self(),
  Consumer = proc_lib:spawn_link(fun() ->
                                     basic_consumer(Daddy, Queue)
                                 end),
  Producer = proc_lib:spawn_link(fun() ->
                                     basic_producer(Daddy, Exchange, RK)
                                 end),
  receive {ok, Consumer} -> ok end,
  ok = basic_close(self(), Exchange, Queue, RK),
  lists:foreach(fun(Pid) ->
                    ok = simple_amqp:cleanup(Pid)
                end, [Daddy, Consumer, Producer]),
  application:stop(?MODULE),
  ok.

basic_producer(_Daddy, X, RK) ->
  F = fun(Bin) -> simple_amqp:publish(self(), X, RK, Bin) end,
  lists:foreach(F, basic_dataset()).

basic_consumer(Daddy, Queue) ->
  {ok, Pid} = simple_amqp:subscribe(self(), Queue),
  basic_consumer_consume(Pid, basic_dataset()),
  Daddy ! {ok, self()}.

basic_consumer_consume(Pid, []) -> ok;
basic_consumer_consume(Pid, Dataset) ->
  receive
    {msg, Pid, DeliveryTag, _RK, Payload} ->
      Pid ! {ack, DeliveryTag},
      basic_consumer_consume(Pid, Dataset -- [Payload])
  end.

basic_setup(Pid, X, Q0, RK) ->
  ok      = simple_amqp:exchange_declare(Pid, X),
  {ok, Q} = simple_amqp:queue_declare(Pid, Q0),
  ok      = simple_amqp:bind(Pid, Q, X, RK),
  Q.

basic_close(Pid, X, Q, RK) ->
  ok = simple_amqp:unbind(Pid, Q, X, RK).

basic_dataset() ->
  [term_to_binary(Term) || Term <- [1, 2, 3, 4]].

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
