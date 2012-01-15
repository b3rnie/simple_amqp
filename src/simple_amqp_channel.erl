%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc simple amqp channel
%%% @copyright 2011 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(simple_amqp_channel).

%%%_* Exports ==========================================================
-export([ start/1
        , start_link/1
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
           , monitor
           , channel
           }).

%%%_ * API -------------------------------------------------------------
start(Args)      -> gen_server:start(?MODULE, Args, []).
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop(Pid)        -> gen_server:call(Pid, stop).
stop_async(Pid)  -> gen_server:cast(Pid, stop).

subscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {subscribe, From, Queue}).

unsubscribe(Pid, From, Queue) ->
  gen_server:cast(Pid, {unsubscribe, From, Queue}).

publish(Pid, From, Exchange, RoutingKey, Msg) ->
  gen_server:cast(Pid, {publish, From, Exchange, RoutingKey, Msg}).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  Connection = orddict:fetch(connection, Args),
  case amqp_connection:open_channel(Connection) of
    {ok, Channel} ->
      Pid = orddict:fetch(pid, Args),
      {ok, #s{ pid        = Pid
             , monitor    = erlang:monitor(process, Pid)
             , connection = orddict:fetch(connection, Args)
             , channel    = Channel
             }}
  end.

handle_call(stop, _From, S) ->
  {stop, normal, S}.

handle_cast(stop, #s{} = S) ->
  {stop, normal, S};

handle_cast({subscribe, From, Queue}, #s{} = S) ->
  amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
  #'basic.consume_ok'{consumer_tag = Tag} =
    amqp_channel:subscribe(Channel,
                           #'basic.consume'{ queue = Queue
                                           , consumer_tag = <<"foo">>
                                         , 
                         
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({unsubscribe, From, Queue}, #s{} = S) ->
  gen_server:reply(From, ok),
  {noreply, S};

handle_cast({publish, From, Exchange, RoutingKey, PayLoad},
            #s{channel = Channel} = S) ->
  amqp_channel:cast(Channel,
                    #'basic.publish'{ exchange    = Exchange
                                    , routing_key = RoutingKey
                                    },
                    #amqp_msg{payload = Payload}
                    ),
  gen_server:reply(From, ok),
  {noreply, S};

handle_info(#'basic.consume_ok'{}, S) ->
  {noreply, S};

handle_info(#'basic.cancel_ok'{}, S) ->
  {noreply, S};

handle_info({#'basic.deliver'{}, Content}, S) ->
  {noreply, S};

handle_info({'DOWN', Ref, process, Pid, Rsn}, #s{ pid = Pid
                                                , ref = Ref} = S) ->
  erlang:demonitor(Ref, [flush]),
  simple_amqp_server:cleanup_async(Pid),
  {noreply, S};



handle_info(_Info, S) ->
  {noreply, S}.

terminate(_Reason, #s{channel = Channel}) ->
  amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
  amqp_channel:close(Channel),
  ok.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
