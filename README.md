Pid = self().

{ok, <<"test_queue">>} = simple_amqp:queue_declare(Pid, <<"test_queue">>).

simple_amqp:exchange_declare(Pid, <<"test_exchange">>, [ {type, <<"direct">>}
                                                       , {auto_delete, false}
                                                       ]).
simple_amqp:bind(Pid, <<"test_queue">>, <<"test_exchange">>, <<"routing_key">>).

simple_amqp:publish(Pid, <<"test_exchange">>, <<"routing_key">>, <<"Payload">>, [ {mandatory, true}
                                                                                , {immediate, true}
                                                                                ]).
