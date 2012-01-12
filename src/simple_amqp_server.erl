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

        , subscribe/2
        , unsubscribe/2
        , publish/4
        , cleanup/1
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([worker/2]).

%%%_* Includes =========================================================
-include_lib("amqp_client/include/amqp_client.hrl").

%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { pids
           , connection_pid
           , connection_mon
           }).

%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
  gen_server:cast(?MODULE, stop).

subscribe(Pid, Queue) ->
  gen_server:call(?MODULE, {subscribe, Pid, Queue}).

unsubscribe(Pid, Queue) ->
  gen_server:call(?MODULE, {unsubscribe, Pid, Queue}).

publish(Pid, Exchange, RoutingKey, Msg) ->
  gen_server:call(?MODULE, {publish, Pid, Exchange, RoutingKey, Msg}).

cleanup(Pid) ->
  gen_server:call(?MODULE, {unregister, Pid}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  Params = params(Args),

  {username          = <<"guest">>,
                             virtual_host      = <<"/">>,
                             node              = node(),
                             adapter_info      = none,
                             client_properties = []}).

  Params = #amqp_params_direct{ username = orddict:fetch(username)
                              , virtual_host = orddict:fetch(virtual_host, '/'
                              , node = node()
                              , client_properties = []
                              },
  {ok, Connection} = amqp_connection:start(params(Args)),
  {ok, #s{ pids           = orddict:new()
         , connection_pid = Connection
         , connection_mon = erlang:monitor(Connection)}}.

handle_call({subscribe, Pid, Queue}, From, #s{} = S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:subscribe(CPid, From, Queue),
  {noreply, S};

handle_call({unsubscribe, Pid, Queue}, From, #s{} = S) ->
  case orddict:find(Pid, S#s.pids) of
    {ok, CPid} -> simple_amqp_channel:unsubscribe(CPid, From, Queue),
                  {noreply, S};
    error -> {reply, {error, no_subscription}, S}
  end;

handle_call({publish, Pid, Exchange, RoutingKey, Msg},
            From, #s{} = S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:publish(Pid, From, Exchange, RoutingKey, Msg),
  {noreply, S}.

handle_call({unregister, Pid}, _From, #s{} = S) ->
  erlang:unlink(Pid),
  Pids = orddict:erase(Pid, S#s.pids),
  {reply, ok, S#s{pids = Pids}}.

handle_cast(stop, #s{} = S) ->
  {stop, normal, S}.

handle_info({'DOWN', Mon, process, Pid, Rsn},
            #s{ connection_pid = Pid
              , connection_mon = Mon} = S) ->
  {stop, Rsn, S};

handle_info({'EXIT', Pid, Rsn}, #s{pids = Pids} = S) ->
  Pids = orddict:erase(Pid, S#s.pids),
  {noreply, S#s{pids = Pids}};

handle_info(_Msg, #s{} = S) ->
  {noreply, S}.

terminate(_Reason, #s{connection = Connection}) ->
  orddict:fold(fun({Pid, CPid}, _) ->
                   simple_amqp_channel:stop(CPid)
               end, '_', Pids),
  amqp_connection:close(Connection),
  ok.

code_change(_OldVsn, #s{} = S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
maybe_new(Pid, S0) ->
  case orddict:find(Pid, S0#s.pids) of
    {ok, CPid} -> {CPid, S0};
    error      ->
      Args = orddict:from_list([ {pid, Pid}
                               , {connection, S#s.connection}
                               ],
      {ok, CPid} = simple_amqp_channel:spawn_link(Args),
      Pids = orddict:store(Pid, CPid, S0#s.pids),
      {CPid, S0#s{pids = Pids}}
  end.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
