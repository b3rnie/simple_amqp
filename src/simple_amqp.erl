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
        , cleanup/1
        ]).

%%%_ * API -------------------------------------------------------------
%% @spec subscribe(Pid::pid(), Queue::binary()) ->
%%                 {ok, Pid::pid()} | {error, Rsn}
%% @doc Subscribe to a queue, returned pid is the channel 'handler'
%%      and can be monitored/linked.
subscribe(Pid, Queue) ->
  simple_amqp_server:subscribe(Queue).

%% @spec unsubscribe(Pid::pid(), Queue::binary()) ->
%%                   ok | {error, Rsn}
%% @doc Unsubscribe from a queue
unsubscribe(Pid, Queue) ->
  simple_amqp_server:unsubscribe(Queue).

%% @spec unsubscribe(Pid::pid(), Exchange::binary(),
%%                   RoutingKey::binary(), Payload::any()) ->
%%                   ok | {error, Rsn}
%% @doc Publish a message
publish(Pid, Exchange, RoutingKey, Payload) ->
  simple_amqp_server:publish(Exchange, RoutingKey, Payload).

%% @spec unsubscribe(Pid::pid()) -> ok | {error, Rsn}
%% @doc Cleanup after pid (channel, gen_server, etc).
cleanup(Pid) ->
  simple_amqp_server:cleanup(Pid).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
