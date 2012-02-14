-ifndef(_SIMPLE_AMQP_HRL_).
-define(_SIMPLE_AMQP_HRL_, true).
-record(simple_amqp_deliver, { pid
                             , consumer_tag
                             , delivery_tag
                             , exchange
                             , routing_key
                             , payload
                             , reply_to
                             , correlation_id
                             , message_id
                             }).

-record(simple_amqp_ack, {delivery_tag}).

-endif.
