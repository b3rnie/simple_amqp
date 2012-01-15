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
-record(s, { channels           :: orddict()
           , connection_pid     :: pid()
           , connection_monitor :: monitor()
           }).

-record(channel, { pid     :: pid()
                 , monitor :: monitor()
                 }).
%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
  gen_server:call(?MODULE, stop).

subscribe(Pid, Queue) ->
  gen_server:call(?MODULE, {subscribe, Pid, Queue}).

unsubscribe(Pid, Queue) ->
  gen_server:call(?MODULE, {unsubscribe, Pid, Queue}).

publish(Pid, Exchange, RoutingKey, Payload) ->
  gen_server:call(?MODULE, {publish, Pid, Exchange, RoutingKey, Payload}).

cleanup(Pid) ->
  gen_server:cast(?MODULE, {cleanup, Pid}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  case amqp_connection:start(params(Args)) of
    {ok, Connection} ->
      Monitor = erlang:monitor(process, Connection),
      {ok, #s{ channels           = orddict:new()
             , connection_pid     = Connection
             , connection_monitor = Monitor
             }}
  end.

handle_call({subscribe, Pid, Queue}, From, #s{} = S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:subscribe(CPid, From, Queue),
  {noreply, S};

handle_call({unsubscribe, Pid, Queue}, From, S) ->
  case orddict:find(Pid, S#s.pids) of
    {ok, CPid} -> simple_amqp_channel:unsubscribe(CPid, From, Queue),
                  {noreply, S};
    error      -> {reply, {error, no_subscription}, S}
  end;

handle_call({publish, Pid, Exchange, RoutingKey, Msg}, From, S0) ->
  {CPid, S} = maybe_new(Pid, S0),
  simple_amqp_channel:publish(Pid, From, Exchange, RoutingKey, Msg),
  {noreply, S}.

handle_call(stop, _From, S) ->
  {stop, normal, S}.

handle_cast({cleanup, Pid}, S0}) ->
  case delete(Pid, S0) of
    {ok, S}      -> {noreply, S};
    {error, Rsn} -> {noreply, S0}
  end;

handle_cast(stop, #s{} = S) ->
  {stop, normal, S}.

handle_info({'DOWN', Monitor, process, Pid, Rsn},
            #s{ connection_pid     = Pid
              , connection_monitor = Monitor
              } = S) ->
  true = Rsn /= normal,
  {stop, Rsn, S};

handle_info({'DOWN', Monitor, process, Pid, Rsn}, S) ->
  true = Rsn /= normal,
  {stop, Rsn, S}
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
  case orddict:find(Pid, S0#s.channels) of
    {ok, #channel{pid = CPid}} -> {CPid, S0};
    error                      ->
      Args = orddict:from_list([ {pid, Pid}
                               , {connection, S#s.connection}
                               ]),
      {ok, CPid} = simple_amqp_channel:spawn(Args),
      Monitor    = erlang:monitor(process, CPid),
      Channels   = orddict:store(Pid, #channel{ pid = CPid
                                              , monitor = Monitor},
                                 S0#s.channels),
      {CPid, S0#s{channels = Channels}}
  end.

maybe_delete(Pid, S0) ->
  case orddict:find(Pid, S#s.channels) of
    {ok, #channel{ pid     = CPid,
                 , monitor = CMon}} ->
      erlang:demonitor(CMon, [flush]),
      simple_amqp_channel:stop(CPid),
      {ok, S#s{pids = orddict:erase(Pid, S#s.channels)}};
    error -> {error, no_such_key}
  end.

params(Args) ->
  F = fun(K) -> orddict:fetch(K, Args) end,
  #amqp_params_direct{ username          = F(username)
                     , virtual_host      = F(virtual_host)
                     , node              = F(node)
                     , adapter_info      = F(adapter_info)
                     , client_properties = F(client_properties)
                     }.
%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
