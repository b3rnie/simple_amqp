%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_connection).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/1
        , connect/2
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
-define(retry_interval, 3000).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { brokers     %% [{type, conf}]
           , connections
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop(Pid)        -> gen_server:cast(Pid, stop).
connect(Pid, N)  -> gen_server:cast(Pid, {connect, N}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  {ok, #s{ brokers     = proplists:get_value(brokers, Args)
         , connections = 0}}.

handle_call(sync, _From, S) ->
  {reply, ok, S, 0}.

handle_cast({connect, N}, #s{connections = Connections} = S) ->
  {noreply, S#s{connections = Connections + N}, 0};

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info(timeout, #s{connections = 0} = S) ->
  {noreply, S};

handle_info(timeout, #s{ brokers     = [{Type, Conf} | Brokers]
                       , connections = Connections} = S0)
  when Connections > 0 ->
  error_logger:info_msg("trying to connect (~p): ~p~n",
                        [?MODULE, {Type, Conf}]),
  case amqp_connection:start(params(Type, Conf)) of
    {ok, Pid} ->
      %% xxx better logging
      error_logger:info_msg("connect successful (~p): ~p~n",
                            [?MODULE, Pid]),
      S = S0#s{ connections = Connections - 1
              , brokers     = [{Type,Conf} | Brokers]},
      simple_amqp_server:add_connection(Pid),
      {noreply, S, 0};
    {error, Rsn} ->
      error_logger:error_msg("connect failed (~p): ~p~n",
                             [?MODULE, Rsn]),
      {noreply, S0#s{brokers = Brokers ++ [{Type,Conf}]},
       ?retry_interval}
  end;

handle_info(Info, S) ->
  error_logger:info_msg("weird info message, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

terminate(_Rsn, _S) ->
  ok.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
params(direct, Args) ->
  F = fun(K) -> proplists:get_value(K, Args) end,
  #amqp_params_direct{ username          = F(username)
                     , virtual_host      = F(virtual_host)
                     , node              = F(node)
                     };

params(network, Args) ->
  F = fun(K) -> proplists:get_value(K, Args) end,
  F2 = fun(K, Def) -> proplists:get_value(K, Args, Def) end,
  #amqp_params_network{ username     = F(username)
                      , password     = F(password)
                      , virtual_host = F(virtual_host)
                      , host         = F(host)
                      , port         = F(port)
                      , heartbeat    = F2(heartbeat, 0)
                      }.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
