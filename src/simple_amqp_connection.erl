%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp server
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_connection).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/1
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
-record(s, { connection %% {pid, ref} | undefined
           , brokers    %% [{type, conf}]
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop(Pid)        -> gen_server:cast(Pid, stop).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  {ok, #s{brokers  = proplists:get_value(brokers, Args)}, 0}.

handle_call(sync, _From, S) ->
  {reply, ok, S, 0}.

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info(timeout, #s{ connection = undefined
                       , brokers    = [{Type, Conf} | Brokers]} = S0) ->
  error_logger:info_msg("trying to connect (~p): ~p~n",
                        [?MODULE, {Type, Conf}]),
  case amqp_connection:start(params(Type, Conf)) of
    {ok, Pid} ->
      %% xxx better logging
      error_logger:info_msg("connect successful (~p): ~p~n",
                            [?MODULE, Pid]),
      S = S0#s{ connection = {Pid, erlang:monitor(process, Pid)}
              , brokers    = [{Type, Conf} | Brokers]},
      simple_amqp_server:add_connection(Pid),
      {noreply, S};
    {error, Rsn} ->
      error_logger:error_msg("connect failed (~p): ~p~n",
                             [?MODULE, Rsn]),
      {noreply, S0#s{brokers = Brokers ++ [{Type, Conf}]},
       ?retry_interval}
  end;

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{connection = {Pid, Ref}} = S) ->
  error_logger:info_msg("connection died (~p): ~p~n", [?MODULE, Rsn]),
  erlang:demonitor(Ref, [flush]),
  simple_amqp_server:del_connection(Pid),
  {noreply, S#s{connection = undefined}, 0};

handle_info(Info, S) ->
  error_logger:info_msg("weird info message, investigate (~p): ~p~n",
                        [?MODULE, Info]),
  {noreply, S}.

terminate(_Rsn, #s{connection = {Pid, Ref}}) ->
  error_logger:info_msg("closing connection (~p): ~p~n", [?MODULE, Pid]),
  erlang:demonitor(Ref, [flush]),
  ok = amqp_connection:close(Pid);
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
  #amqp_params_network{ username     = F(username)
                      , password     = F(password)
                      , virtual_host = F(virtual_host)
                      , host         = F(host)
                      , port         = F(port)
                      }.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
