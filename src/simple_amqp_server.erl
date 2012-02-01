%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp server
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_server).

%%%_* Exports ==========================================================
-export([ start/1
        , start_link/1
        , stop/0
        , cleanup/0
        , cmd/2
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

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { clientpid_to_channel    %% dict
           , channelpid_to_clientpid %% dict
           , connection_pid          %% pid()
           , connection_ref       %% monitor()
           }).

-record(channel, { pid %% pid()
                 , ref %% monitor()
                 }).
%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop()         -> cast(stop).
cleanup()      -> call(cleanup).
cmd(Cmd, Args) -> call({cmd, Cmd, Args}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  case connect(Args) of
    {ok, Pid} ->
      Ref = erlang:monitor(process, Pid),
      {ok, #s{ clientpid_to_channel    = dict:new()
             , channelpid_to_clientpid = dict:new()
             , connection_pid          = Pid
             , connection_ref          = Ref
             }};
    {error, Rsn} ->
      {stop, Rsn}
  end.

handle_call({cmd, unsubscribe, Args}, {Pid, _} = From,
            #s{clientpid_to_channel = CDict} = S) ->
  case dict:find(Pid, CDict) of
    {ok, #channel{pid = ChannelPid}} ->
      simple_amqp_channel:cmd(ChannelPid, From, unsubscribe, Args),
      {noreply, S};
    error ->
      {reply, {error, not_subscribed}, S}
  end;

handle_call({cmd, Cmd, Args}, {Pid, _} = From, S0) ->
  {ChannelPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:cmd(ChannelPid, From, Cmd, Args),
  {noreply, S};

handle_call(cleanup, {Pid, _} = _From,
            #s{clientpid_to_channel = CDict} = S0) ->
  case dict:is_key(Pid, CDict) of
    true  -> {reply, ok, delete(Pid, S0)};
    false -> {reply, ok, S0}
  end.

handle_cast(stop, S) ->
  {stop, normal, S}.

handle_info({'DOWN', Ref, process, Pid, Rsn},
            #s{ connection_pid = Pid
              , connection_ref = Ref
              } = S) ->
  {stop, Rsn, S};

handle_info({'DOWN', _Mon, process, Pid, Rsn},
            #s{channelpid_to_clientpid = CDict} = S) ->
  case dict:find(Pid, CDict) of
    {ok, ChannelPid} ->
      error_logger:info_msg("Channel died (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, delete(ChannelPid, S)};
    error ->
      error_logger:info_msg("monitored process died, "
                            "investigate! (~p): ~p~n", [?MODULE, Rsn]),
      {noreply, S}
  end;

handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Rsn, #s{ connection_pid       = ConnectionPid
                  , connection_ref       = ConnectionRef
                  , clientpid_to_channel = CDict}) ->
  dict:fold(fun(_ClientPid, #channel{ pid = Pid
                                    , ref = Ref}, '_') ->
                erlang:demonitor(Ref, [flush]),
                simple_amqp_channel:stop(Pid)
            end, '_', CDict),
  erlang:demonitor(ConnectionRef, [flush]),
  ok = amqp_connection:close(ConnectionPid).

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(ClientPid, #s{ connection_pid          = ConnectionPid
                       , clientpid_to_channel    = Channels0
                       , channelpid_to_clientpid = Clients0} = S0) ->

  case dict:find(ClientPid, Channels0) of
    {ok, #channel{pid = ChannelPid}} ->
      {ChannelPid, S0};
    error ->
      Args = [ {connection_pid, ConnectionPid}
             , {client_pid,     ClientPid}
             ],
      {ok, ChannelPid}  = simple_amqp_channel:start(Args),
      ChannelRef        = erlang:monitor(process, ChannelPid),
      Channel = #channel{ pid = ChannelPid
                        , ref = ChannelRef},
      Channels = dict:store(ClientPid,  Channel,   Channels0),
      Clients  = dict:store(ChannelPid, ClientPid, Clients0),
      {ChannelPid, S0#s{ clientpid_to_channel    = Channels
                       , channelpid_to_clientpid = Clients}}
  end.

delete(ClientPid, #s{ clientpid_to_channel = Channels
                    , channelpid_to_clientpid = Clients} = S) ->
  #channel{ pid = ChannelPid
          , ref = ChannelRef} = dict:fetch(ClientPid, Channels),
  true = dict:is_key(ChannelPid, Clients),

  erlang:demonitor(ChannelRef, [flush]),
  simple_amqp_channel:stop(ChannelPid),
  S#s{ clientpid_to_channel    = dict:erase(ClientPid,  Channels)
     , channelpid_to_clientpid = dict:erase(ChannelPid, Clients)}.

connect(Args) ->
  do_connect(proplists:get_value(brokers, Args)).

do_connect([]) -> {error, no_working_brokers};
do_connect([{Type, Conf}|T]) ->
  Params = params(Type, Conf),
  case amqp_connection:start(Params) of
    {ok, ConnectionPid} -> {ok, ConnectionPid};
    {error, Rsn}        ->
      error_logger:info_msg("Connect failed (~p,~p): ~p ~n",
                            [?MODULE, Params, Rsn]),
      do_connect(T)
  end.

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

call(Args) -> gen_server:call(?MODULE, Args).
cast(Args) -> gen_server:cast(?MODULE, Args).
%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
