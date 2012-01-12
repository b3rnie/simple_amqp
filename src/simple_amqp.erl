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
subscribe(Pid, Queue) ->
  simple_amqp_server:subscribe(Queue).

unsubscribe(Pid, Queue) ->
  simple_amqp_server:unsubscribe(Queue).

publish(Pid, Exchange, RoutingKey, Msg) ->
  simple_amqp_server:publish(Exchange, RoutingKey, Msg).

cleanup(Pid) ->
  simple_amqp_server:cleanup(Pid).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
