%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_app).
-behaviour(application).

%%%_* Exports ==========================================================
-export([ start/2
        , prep_stop/1
        , stop/1]).

%%%_* Code =============================================================
start(Type, []) ->
  {ok, Brokers} = application:get_env(simple_amqp, brokers),
  start(Type, [{brokers, Brokers}]);
start(_Type, Args) ->
  simple_amqp_sup:start_link(Args).

prep_stop(State) ->
  simple_amqp_server:stop(),
  State.

stop(_State) -> ok.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
