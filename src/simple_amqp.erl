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
  simple_amqp_server:subscribe(Pid, Queue, Ops).

%% @spec unsubscribe(Pid::pid(), Queue::binary()) ->
%%                   ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Pid, Queue) ->
  unsubscribe(Pid, Queue, []).

unsubscribe(Pid, Queue, Ops) ->
  simple_amqp_server:unsubscribe(Pid, Queue, Ops).

%% @spec unsubscribe(Pid::pid(), Exchange::binary(),
%%                   RoutingKey::binary(), Payload::any()) ->
%%                   ok | {error, Rsn}
%% @doc Publish a message
publish(Pid, Exchange, RoutingKey, Payload) ->
  simple_amqp_server:publish(Pid, Exchange, RoutingKey, Payload).

%% @spec exchange_declare(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc declare a new exchange
exchange_declare(Pid, Exchange) ->
  exchange_declare(Pid, Exchange, []).

exchange_declare(Pid, Exchange, Ops) ->
  simple_amqp_server:exchange_declare(Pid, Exchange, Ops).

%% @spec exchange_delete(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc delete an exchange
exchange_delete(Pid, Exchange) ->
  exchange_delete(Pid, Exchange, []).

exchange_delete(Pid, Exchange, Ops) ->
  simple_amqp_server:exchange_delete(Pid, Exchange, Ops).

%% @spec queue_declare(Pid::pid(), Queue) -> {ok, Queue} | {error, Rsn}
%% @doc declare a new queue
queue_declare(Pid, Queue) ->
  queue_declare(Pid, Queue, []).

queue_declare(Pid, Queue, Ops) ->
  simple_amqp_server:queue_declare(Pid, Queue, Ops).

%% @spec queue_delete(Pid::pid(), Queue) -> ok | {error, Rsn}
%% @doc delete a queue
queue_delete(Pid, Queue) ->
  queue_delete(Pid, Queue, []).

queue_delete(Pid, Queue, Ops) ->
  simple_amqp_server:queue_delete(Pid, Queue, Ops).

%% @spec bind(Pid::pid(), Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc create routing rule
bind(Pid, Queue, Exchange, RoutingKey) ->
  simple_amqp_server:bind(Pid, Queue, Exchange, RoutingKey).

%% @spec unbind(Pid::pid(), Queue, Exchange, RoutingKey) -> ok | {error, Rsn}
%% @doc remote a routing rule
unbind(Pid, Queue, Exchange, RoutingKey) ->
  simple_amqp_server:unbind(Pid, Queue, Exchange, RoutingKey).

%% @spec unsubscribe(Pid::pid()) -> ok | {error, Rsn}
%% @doc Cleanup after pid (channel, gen_server, etc).
cleanup(Pid) ->
  simple_amqp_server:cleanup(Pid).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
