%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp channel
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_channel).

%%%_* Exports ==========================================================
-export([ start_link/1
        , stop/0
        , subscribe/3
        , unsubscribe/3
        , publish/5
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
-record(s, { pid
           , ref
           , connection
           , channel
           }).

%%%_ * API -------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

stop(Pid) ->
  gen_server:cast(Pid, stop).

subscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {subscribe, From, Queue}).

unsubscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {unsubscribe, From, Queue}).

publish(Pid, From, Exchange, RoutingKey, Msg) ->
  gen_server:cast(Pid, {publish, From, Exchange, RoutingKey, Msg}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  Pid           = orddict:fetch(pid, Args),
  Connection    = orddict:fetch(connection, Args),
  Mon           = erlang:monitor(process, Pid),
  {ok, Channel} = amqp_connection:open_channel(Connection)
  {ok, #s{ pid = Pid
         , mon = Mon
         , connection = Connection
         , channel    = Channel
         }}.

handle_call(_, _From, #s{} = S) ->
  {reply, ok, S}.

handle_cast(stop, #s{} = S) ->
  {stop, normal, S};

handle_cast({subscribe, From, Queue}, #s{} = S) ->
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({unsubscribe, From, Queue}, #s{} = S) ->
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({publish, From, Exchange, RoutingKey, Msg}, #s{} = S) ->
  gen_server:reply(From, ok),
  {noreply, S}.

handle_info({'DOWN', Ref, process, Pid, Rsn}, #s{ pid = Pid
                                                , ref = Ref} = S) ->
  {stop, normal, S}.


terminate(_Reason, #s{channel = Channel}) ->
  amqp_channel:close(Channel),
  ok.
