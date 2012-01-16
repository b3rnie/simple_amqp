%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp interface
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp).

%%%_* Exports ==========================================================
-export([ subscribe/2
        , unsubscribe/2
        , publish/4
        , declare_exchange/2
        , delete_exchange/2
        , declare_queue/2
        , delete_queue/2
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
  simple_amqp_server:subscribe(Pid, Queue).

%% @spec unsubscribe(Pid::pid(), Queue::binary()) ->
%%                   ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Pid, Queue) ->
  simple_amqp_server:unsubscribe(Pid, Queue).

%% @spec unsubscribe(Pid::pid(), Exchange::binary(),
%%                   RoutingKey::binary(), Payload::any()) ->
%%                   ok | {error, Rsn}
%% @doc Publish a message
publish(Pid, Exchange, RoutingKey, Payload) ->
  simple_amqp_server:publish(Pid, Exchange, RoutingKey, Payload).

%% @spec declare_exchange(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc declare a new exchange
declare_exchange(Pid, Exchange) ->
  simple_amqp_server:declare_exchange(Pid, Exchange).

%% @spec delete_exchange(Pid::pid(), Exchange) -> ok | {error, Rsn}
%% @doc delete an exchange
delete_exchange(Pid, Exchange) ->
  simple_amqp_server:delete_exchange(Pid, Exchange).

%% @spec declare_queue(Pid::pid(), Queue) -> ok | {error, Rsn}
%% @doc declare a new queue
declare_queue(Pid, Queue) ->
  simple_amqp_server:declare_queue(Pid, Queue).

%% @spec delete_queue(Pid::pid(), Queue) -> ok | {error, Rsn}
%% @doc delete a queue
delete_queue(Pid, Queue) ->
  simple_amqp_server:delete_queue(Pid, Queue).

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
