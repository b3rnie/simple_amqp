%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_app).
-behaviour(application).

%%%_* Exports ==========================================================
-export([start/2, stop/1]).

%%%_* Code =============================================================
start(_Type, _Args) ->
  {ok, Brokers} = application:get_env(simple_amqp, brokers),
  simple_amqp_sup:start_link([{brokers, Brokers}]).

stop(_State)       -> ok.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
