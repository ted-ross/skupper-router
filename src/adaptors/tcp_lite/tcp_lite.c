/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <qpid/dispatch/ctools.h>
#include <qpid/dispatch/amqp.h>
#include <qpid/dispatch/enum.h>
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/server.h>
#include <qpid/dispatch/log.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>
#include <proton/listener.h>

#include "dispatch_private.h"
#include "delivery.h"
#include "adaptors/adaptor_common.h"
#include "adaptors/adaptor_listener.h"

typedef struct tcplite_common_t     tcplite_common_t;
typedef struct tcplite_listener_t   tcplite_listener_t;
typedef struct tcplite_connector_t  tcplite_connector_t;
typedef struct tcplite_connection_t tcplite_connection_t;

ALLOC_DECLARE(tcplite_listener_t);
ALLOC_DECLARE(tcplite_connector_t);
ALLOC_DECLARE(tcplite_connection_t);

DEQ_DECLARE(tcplite_listener_t,   tcplite_listener_list_t);
DEQ_DECLARE(tcplite_connector_t,  tcplite_connector_list_t);
DEQ_DECLARE(tcplite_connection_t, tcplite_connection_list_t);


typedef enum {
    TL_LISTENER,
    TL_CONNECTOR,
    TL_CONNECTION
} tcplite_context_type_t;

struct tcplite_common_t {
    tcplite_context_type_t   context_type;
    tcplite_common_t        *parent;
    sys_mutex_t              lock;
    vflow_record_t          *vflow;
    qd_timer_t              *activate_timer;
};

struct tcplite_listener_t {
    tcplite_common_t           common;
    DEQ_LINKS(tcplite_listener_t);
    qd_adaptor_config_t        adaptor_config;
    uint64_t                   conn_id;
    qdr_connection_t          *conn;
    uint64_t                   link_id;
    qdr_link_t                *in_link;
    qd_adaptor_listener_t     *adaptor_listener;
    tcplite_connection_list_t  connections;
};

ALLOC_DEFINE(tcplite_listener_t);


typedef struct tcplite_connector_t {
    tcplite_common_t           common;
    DEQ_LINKS(tcplite_connector_t);
    qd_adaptor_config_t        adaptor_config;
    uint64_t                   conn_id;
    qdr_connection_t          *conn;
    uint64_t                   link_id;
    qdr_link_t                *out_link;
    tcplite_connection_list_t  connections;
} tcplite_connector_t;

ALLOC_DEFINE(tcplite_connector_t);


typedef enum {
    LSIDE_INITIAL,
    LSIDE_LINK_SETUP,
    LSIDE_STREAM_START,
    LSIDE_FLOW,
    CSIDE_INITIAL,
    CSIDE_RAW_CONNECTION_OPENING,
    CSIDE_LINK_SETUP,
    CSIDE_FLOW
} tcplite_connection_state_t;
ENUM_DECLARE(tcplite_connection_state);

static const char *state_names[] =
{ "LSIDE_INITIAL", "LSIDE_LINK_SETUP", "LSIDE_STREAM_START", "LSIDE_FLOW",
  "CSIDE_INITIAL", "CSIDE_RAW_CONNECTION_OPENING", "CSIDE_LINK_SETUP", "CSIDE_FLOW"
};
ENUM_DEFINE(tcplite_connection_state, state_names);

#define RAW_BUFFER_BATCH_SIZE 16


//
// Important note about the polarity of the link/stream/delivery/disposition tuples:
//
//                   Listener Side      Connector Side
//               +------------------+-------------------+
//      Inbound  |      Client      |      Server       |
//               +------------------+-------------------+
//     Outbound  |      Server      |      Client       |
//               +------------------+-------------------+
//
typedef struct tcplite_connection_t {
    tcplite_common_t            common;
    DEQ_LINKS(tcplite_connection_t);
    pn_raw_connection_t        *raw_conn;
    qdr_link_t                 *inbound_link;
    qd_message_t               *inbound_stream;
    qdr_delivery_t             *inbound_delivery;     // protected by lock
    sys_atomic_t                inbound_disposition;
    uint64_t                    inbound_link_id;
    qdr_link_t                 *outbound_link;
    qd_message_t               *outbound_stream;
    qdr_delivery_t             *outbound_delivery;    // protected by lock
    sys_atomic_t                outbound_disposition;
    uint64_t                    outbound_link_id;
    pn_condition_t             *error;
    char                       *reply_to;             // protected by lock (LSIDE only)
    uint64_t                    conn_id;
    qd_handler_context_t        context;
    tcplite_connection_state_t  state;
    bool                        listener_side;
    bool                        inbound_credit;       // protected by lock
} tcplite_connection_t;

ALLOC_DEFINE(tcplite_connection_t);


typedef struct {
    qdr_core_t                *core;
    qd_dispatch_t             *qd;
    qd_server_t               *server;
    qdr_protocol_adaptor_t    *pa;
    tcplite_listener_list_t    listeners;
    tcplite_connector_list_t   connectors;
    tcplite_connection_list_t  connections;
    qd_log_source_t           *log_source;
    sys_mutex_t                lock;
    pn_proactor_t             *proactor;
} tcplite_context_t;


static tcplite_context_t *tcplite_context;

static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context);



//=================================================================================
// Core Activation Handler
//=================================================================================
static void on_core_activate_LSIDE(void *context)
{
    qdr_connection_t *core_conn = ((tcplite_listener_t*) context)->conn;
    qdr_connection_process(core_conn);
}


static void on_core_activate_CSIDE(void *context)
{
    qdr_connection_t *core_conn = ((tcplite_connector_t*) context)->conn;
    qdr_connection_process(core_conn);
}


//=================================================================================
// Helper Functions
//=================================================================================
static pn_data_t *TL_conn_properties(void)
{
   // Return a new tcp connection properties map.
    pn_data_t *props = pn_data(0);
    pn_data_put_map(props);
    pn_data_enter(props);
    pn_data_put_symbol(props,
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_ADAPTOR_KEY),
                                       QD_CONNECTION_PROPERTY_ADAPTOR_KEY));
    pn_data_put_string(props,
                       pn_bytes(strlen(QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE),
                                       QD_CONNECTION_PROPERTY_TCP_ADAPTOR_VALUE));
    pn_data_exit(props);
    return props;
}


static qdr_connection_t *TL_open_core_connection(uint64_t conn_id, bool incoming)
{
    qdr_connection_t *conn;
    
    //
    // The qdr_connection_info() function makes its own copy of the passed in tcp_conn_properties.
    // So, we need to call pn_data_free(tcp_conn_properties)
    //
    pn_data_t *properties       = TL_conn_properties();
    qdr_connection_info_t *info = qdr_connection_info(false,        // is_encrypted,
                                                      false,        // is_authenticated,
                                                      true,         // opened,
                                                      "",           // sasl_mechanisms,
                                                      incoming ? QD_INCOMING : QD_OUTGOING,  // dir,
                                                      "tcplite",    // host,
                                                      "",           // ssl_proto,
                                                      "",           // ssl_cipher,
                                                      "",           // user,
                                                      "TcpAdaptor", // container,
                                                      properties,   // connection_properties,
                                                      0,            // ssl_ssf,
                                                      false,        // ssl,
                                                      "",           // peer router version,
                                                      true,         // streaming links
                                                      false);       // connection trunking
    pn_data_free(properties);

    conn = qdr_connection_opened(tcplite_context->core,
                                 tcplite_context->pa,
                                 incoming,        // incoming
                                 QDR_ROLE_NORMAL, // role
                                 1,               // cost
                                 conn_id,         // management_id
                                 0,               // label
                                 0,               // remote_container_id
                                 false,           // strip_annotations_in
                                 false,           // strip_annotations_out
                                 5,               // link_capacity
                                 0,               // policy_spec
                                 info,            // connection_info
                                 0,               // context_binder
                                 0);              // bind_token

    return conn;
}


static void TL_setup_listener(tcplite_listener_t *li)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this listener
    //
    li->conn_id = qd_server_allocate_connection_id(tcplite_context->server);
    li->conn    = TL_open_core_connection(li->conn_id, true);
    qdr_connection_set_context(li->conn, li);

    //
    // Attach an in-link to represent the desire to send connection streams to the address
    //
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, li->adaptor_config.address);

    li->in_link = qdr_link_first_attach(li->conn, QD_INCOMING, 0, target, "tcp.listener.in", 0, false, 0, &li->link_id);
    qdr_link_set_context(li->in_link, li);

    //
    // Create a vflow record for this listener
    //
    li->common.vflow = vflow_start_record(VFLOW_RECORD_LISTENER, 0);
    vflow_set_string(li->common.vflow, VFLOW_ATTRIBUTE_PROTOCOL, "tcp");
}


static void TL_setup_connector(tcplite_connector_t *cr)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this connector
    //
    cr->conn_id = qd_server_allocate_connection_id(tcplite_context->server);
    cr->conn    = TL_open_core_connection(cr->conn_id, false);
    qdr_connection_set_context(cr->conn, cr);

    //
    // Attach an out-link to represent our desire to receive connection streams for the address
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, cr->adaptor_config.address);

    //
    // Create a vflow record for this connector
    //
    cr->common.vflow = vflow_start_record(VFLOW_RECORD_CONNECTOR, 0);
    vflow_set_string(cr->common.vflow, VFLOW_ATTRIBUTE_PROTOCOL, "tcp");

    cr->out_link = qdr_link_first_attach(cr->conn, QD_OUTGOING, source, 0, "tcp.connector.out", 0, false, 0, &cr->link_id);
    qdr_link_set_context(cr->out_link, cr);
    qdr_link_flow(tcplite_context->core, cr->out_link, 5, false);
}


static void drain_read_buffers_IO(pn_raw_connection_t *raw_conn)
{
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;
    size_t           drained = 0;

    while ((count = pn_raw_connection_take_read_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            qd_buffer_free(buf);
            drained++;
        }
    }

    printf("drain_read_buffers_IO  %ld buffers\n", drained);
}


static void drain_write_buffers_IO(pn_raw_connection_t *raw_conn)
{
    pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
    size_t           count;
    size_t           drained = 0;

    while ((count = pn_raw_connection_take_written_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
        for (size_t i = 0; i < count; i++) {
            qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
            qd_buffer_free(buf);
            drained++;
        }
    }

    printf("drain_write_buffers_IO  %ld buffers\n", drained);
}


static void free_connection_IO(tcplite_connection_t *conn)
{
    printf("free_connection_IO - %cSIDE\n", conn->listener_side ? 'L' : 'C');

    free(conn->reply_to);
    if (!!conn->raw_conn) {
        drain_read_buffers_IO(conn->raw_conn);
        drain_write_buffers_IO(conn->raw_conn);
        pn_raw_connection_close(conn->raw_conn);
    }

    if (!!conn->inbound_link) {
        qdr_link_detach(conn->inbound_link, QD_LOST, 0);
    }

    if (!!conn->inbound_delivery) {
        if (!!conn->inbound_stream) {
            qd_message_receive_complete(conn->inbound_stream);
            qdr_delivery_continue(tcplite_context->core, conn->inbound_delivery, true);
        }

        qdr_delivery_remote_state_updated(tcplite_context->core, conn->inbound_delivery, 0, true, 0, false);
        qdr_delivery_set_context(conn->inbound_delivery, 0);
        qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "free_connection_IO - inbound_delivery");
    }

    if (!!conn->outbound_link) {
        qdr_link_detach(conn->outbound_link, QD_LOST, 0);
    }

    if (!!conn->outbound_delivery) {
        qdr_delivery_remote_state_updated(tcplite_context->core, conn->outbound_delivery, PN_MODIFIED, true, 0, false);
        qdr_delivery_set_context(conn->outbound_delivery, 0);
        qdr_delivery_decref(tcplite_context->core, conn->outbound_delivery, "free_connection_IO - outbound_delivery");
    }

    sys_atomic_destroy(&conn->inbound_disposition);
    sys_atomic_destroy(&conn->outbound_disposition);

    qd_timer_free(conn->common.activate_timer);
    sys_mutex_free(&conn->common.lock);
    vflow_end_record(conn->common.vflow);

    if (!!conn->common.parent && conn->common.parent->context_type == TL_LISTENER) {
        tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
        sys_mutex_lock(&li->common.lock);
        DEQ_REMOVE(li->connections, conn);
        sys_mutex_unlock(&li->common.lock);
    } else {
        tcplite_connector_t *cr = (tcplite_connector_t*) conn->common.parent;
        sys_mutex_lock(&cr->common.lock);
        DEQ_REMOVE(cr->connections, conn);
        sys_mutex_unlock(&cr->common.lock);
    }

    free_tcplite_connection_t(conn);
}


static void set_state_IO(tcplite_connection_t *conn, tcplite_connection_state_t new_state)
{
    qd_log(tcplite_context->log_source, QD_LOG_DEBUG, "[C%"PRIu64"] State change %s -> %s",
           conn->conn_id, tcplite_connection_state_name(conn->state), tcplite_connection_state_name(new_state));
    conn->state = new_state;
}


static void grant_read_buffers_IO(pn_raw_connection_t *raw_conn, const size_t count)
{
    pn_raw_buffer_t raw_buffers[count];

    for (size_t i = 0; i < count; i++) {
        qd_buffer_t *buf = qd_buffer();
        raw_buffers[i].context  = (uintptr_t) buf;
        raw_buffers[i].bytes    = (char*) qd_buffer_base(buf);
        raw_buffers[i].capacity = qd_buffer_capacity(buf);
        raw_buffers[i].offset   = 0;
        raw_buffers[i].size     = 0;
    }

    printf("grant_read_buffers_IO - %ld\n", count);
    pn_raw_connection_give_read_buffers(raw_conn, raw_buffers, count);
}


static bool produce_read_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn, qd_message_t *stream)
{
    if (qd_message_can_produce_buffers(stream)) {
        qd_buffer_list_t qd_buffers = DEQ_EMPTY;
        pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
        size_t           count;

        while ((count = pn_raw_connection_take_read_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
            for (size_t i = 0; i < count; i++) {
                qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
                qd_buffer_insert(buf, raw_buffers[i].size);
                if (qd_buffer_size(buf) > 0) {
                    DEQ_INSERT_TAIL(qd_buffers, buf);
                } else {
                    qd_buffer_free(buf);
                }
            }
        }

        if (!DEQ_IS_EMPTY(qd_buffers)) {
            printf("produce_read_buffers_XSIDE_IO - Producing %ld buffers\n", DEQ_SIZE(qd_buffers));
            qd_message_produce_buffers(stream, &qd_buffers);

            //
            // TODO - Activate the consuming connection.
            //

            return true;
        }
    }

    return false;
}


static void consume_write_buffers_XSIDE_IO(pn_raw_connection_t *raw_conn, qd_message_t *stream)
{
    size_t limit = pn_raw_connection_write_buffers_capacity(raw_conn);

    if (limit > 0) {
        bool was_blocked = !qd_message_can_produce_buffers(stream);
        qd_buffer_list_t buffers = DEQ_EMPTY;
        size_t actual = qd_message_consume_buffers(stream, &buffers, limit);
        assert(actual == DEQ_SIZE(buffers));
        pn_raw_buffer_t raw_buffers[actual];
        qd_buffer_t *buf = DEQ_HEAD(buffers);
        for (size_t i = 0; i < actual; i++) {
            raw_buffers[i].context  = (uintptr_t) buf;
            raw_buffers[i].bytes    = (char*) qd_buffer_base(buf);
            raw_buffers[i].capacity = qd_buffer_capacity(buf);
            raw_buffers[i].size     = qd_buffer_size(buf);
            raw_buffers[i].offset   = 0;
            buf = DEQ_NEXT(buf);
        }
        printf("consume_write_buffers_XSIDE_IO - Consuming %ld buffers\n", actual);
        pn_raw_connection_write_buffers(raw_conn, raw_buffers, actual);

        if (was_blocked && qd_message_can_produce_buffers(stream)) {
            //
            // TODO - Activate the producing connection
            //
        }
    }
}


static void link_setup_LSIDE_IO(tcplite_connection_t *conn)
{
    tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_t *source = qdr_terminus(0);

    qdr_terminus_set_address(target, li->adaptor_config.address);
    qdr_terminus_set_dynamic(source);
    
    conn->inbound_link = qdr_link_first_attach(li->conn, QD_INCOMING, qdr_terminus(0), target, "tcp.lside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
    conn->outbound_link = qdr_link_first_attach(li->conn, QD_OUTGOING, source, qdr_terminus(0), "tcp.lside.out", 0, false, 0, &conn->outbound_link_id);
    qdr_link_set_context(conn->outbound_link, conn);
    qdr_link_set_user_streaming(conn->outbound_link);
    qdr_link_flow(tcplite_context->core, conn->outbound_link, 1, false);
}


static void link_setup_CSIDE_IO(tcplite_connection_t *conn)
{
    tcplite_connector_t *cr = (tcplite_connector_t*) conn->common.parent;
    qdr_terminus_t *target = qdr_terminus(0);

    qdr_terminus_set_address(target, conn->reply_to);

    conn->inbound_link = qdr_link_first_attach(cr->conn, QD_INCOMING, qdr_terminus(0), target, "tcp.cside.in", 0, false, 0, &conn->inbound_link_id);
    qdr_link_set_context(conn->inbound_link, conn);
}


static bool try_compose_and_send_client_stream_LSIDE_IO(tcplite_connection_t *conn)
{
    tcplite_listener_t  *li = (tcplite_listener_t*) conn->common.parent;
    qd_composed_field_t *props = 0;

    //
    // The lock is used here to protect access to the reply_to field.  This field is written
    // by an IO thread associated with the core connection, not this raw connection.
    //
    // The content-type value of "application/octet-stream" is used to signal to the network that
    // the body of this stream will be a completely unstructured octet stream, without even an
    // application-data performative.  The octets directly following the application-properties
    // (or properties if there are no application-properties) section will constitute the stream
    // and will consist solely of AMQP transport frames.
    //
    sys_mutex_lock(&conn->common.lock);
    if (!!conn->reply_to) {
        props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
        qd_compose_start_list(props);
        qd_compose_insert_null(props);                                // message-id
        qd_compose_insert_null(props);                                // user-id
        qd_compose_insert_string(props, li->adaptor_config.address);  // to
        qd_compose_insert_null(props);                                // subject
        qd_compose_insert_string(props, conn->reply_to);              // reply-to
        vflow_serialize_identity(conn->common.vflow, props);          // correlation-id
        qd_compose_insert_string(props, QD_CONTENT_TYPE_APP_OCTETS);  // content-type
        //qd_compose_insert_null(props);                              // content-encoding
        //qd_compose_insert_timestamp(props, 0);                      // absolute-expiry-time
        //qd_compose_insert_timestamp(props, 0);                      // creation-time
        //qd_compose_insert_null(props);                              // group-id
        //qd_compose_insert_uint(props, 0);                           // group-sequence
        //qd_compose_insert_null(props);                              // reply-to-group-id
        qd_compose_end_list(props);
    }
    sys_mutex_unlock(&conn->common.lock);

    if (props == 0) {
        return false;
    }

    conn->inbound_stream = qd_message();
    qd_message_set_streaming_annotation(conn->inbound_stream);

    qd_message_compose_2(conn->inbound_stream, props, false);
    qd_compose_free(props);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_start_unicast_cutthrough(conn->inbound_stream);

    //
    // Start latency timer for this cross-van connection.
    //
    vflow_latency_start(conn->common.vflow);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    conn->inbound_delivery = qdr_link_deliver(conn->inbound_link, conn->inbound_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(tcplite_context->log_source, QD_LOG_DEBUG,
            "[C%" PRIu64 "][L%" PRIu64 "] Initiating listener side empty client stream message",
            conn->conn_id, conn->inbound_link_id);

    return true;
}


static void compose_and_send_server_stream_CSIDE_IO(tcplite_connection_t *conn)
{
    qd_composed_field_t *props = 0;

    //
    // The lock is used here to protect access to the reply_to field.  This field is written
    // by an IO thread associated with the core connection, not this raw connection.
    //
    // The content-type value of "application/octet-stream" is used to signal to the network that
    // the body of this stream will be a completely unstructured octet stream, without even an
    // application-data performative.  The octets directly following the application-properties
    // (or properties if there are no application-properties) section will constitute the stream
    // and will consist solely of AMQP transport frames.
    //
    props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
    qd_compose_start_list(props);
    qd_compose_insert_null(props);                                // message-id
    qd_compose_insert_null(props);                                // user-id
    qd_compose_insert_string(props, conn->reply_to);              // to
    qd_compose_insert_null(props);                                // subject
    qd_compose_insert_null(props);                                // reply-to
    qd_compose_insert_null(props);                                // correlation-id
    qd_compose_insert_string(props, QD_CONTENT_TYPE_APP_OCTETS);  // content-type
    //qd_compose_insert_null(props);                              // content-encoding
    //qd_compose_insert_timestamp(props, 0);                      // absolute-expiry-time
    //qd_compose_insert_timestamp(props, 0);                      // creation-time
    //qd_compose_insert_null(props);                              // group-id
    //qd_compose_insert_uint(props, 0);                           // group-sequence
    //qd_compose_insert_null(props);                              // reply-to-group-id
    qd_compose_end_list(props);

    conn->inbound_stream = qd_message();
    qd_message_set_streaming_annotation(conn->inbound_stream);

    qd_message_compose_2(conn->inbound_stream, props, false);
    qd_compose_free(props);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_start_unicast_cutthrough(conn->inbound_stream);

    //
    // The delivery comes with a ref-count to protect the returned value.  Inherit that ref-count as the
    // protection of our held pointer.
    //
    conn->inbound_delivery = qdr_link_deliver(conn->inbound_link, conn->inbound_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_set_context(conn->inbound_delivery, conn);

    qd_log(tcplite_context->log_source, QD_LOG_DEBUG,
            "[C%" PRIu64 "][L%" PRIu64 "] Initiating connector side empty server stream message",
            conn->conn_id, conn->inbound_link_id);
}


static void extract_metadata_from_stream_CSIDE(tcplite_connection_t *conn)
{
    qd_iterator_t *rt_iter = qd_message_field_iterator(conn->outbound_stream, QD_FIELD_REPLY_TO);
    qd_iterator_t *ci_iter = qd_message_field_iterator(conn->outbound_stream, QD_FIELD_CORRELATION_ID);

    if (!!rt_iter) {
        conn->reply_to = (char*) qd_iterator_copy(rt_iter);
        qd_iterator_free(rt_iter);
    }

    if (!!ci_iter) {
        vflow_set_ref_from_iter(conn->common.vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, ci_iter);
        qd_iterator_free(ci_iter);
    }
}


static void handle_outbound_delivery_LSIDE(tcplite_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery)
{
    printf("handle_outbound_delivery_LSIDE\n");

    sys_mutex_lock(&conn->common.lock);
    if (!conn->outbound_delivery) {
        qdr_delivery_incref(delivery, "handle_outbound_delivery_LSIDE");
        conn->outbound_delivery = delivery;
        conn->outbound_stream   = qdr_delivery_message(delivery);
    }
    sys_mutex_unlock(&conn->common.lock);

    pn_raw_connection_wake(conn->raw_conn);
}


/**
 * Handle the first indication of a new outbound delivery on CSIDE.  This is where the raw connection to the
 * external service is established.  This function executes in an IO thread not associated with a raw connection.
 */
static void handle_first_outbound_delivery_CSIDE(tcplite_connector_t *cr, qdr_link_t *link, qdr_delivery_t *delivery)
{
    printf("handle_first_outbound_delivery_CSIDE\n");
    assert(!qdr_delivery_get_context(delivery));

    tcplite_connection_t *conn = new_tcplite_connection_t();
    ZERO(conn);

    qdr_delivery_incref(delivery, "CORE_deliver_outbound CSIDE");
    qdr_delivery_set_context(delivery, conn);

    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (tcplite_common_t*) cr;

    conn->listener_side     = false;
    conn->state             = CSIDE_RAW_CONNECTION_OPENING;
    conn->outbound_delivery = delivery;
    conn->outbound_link     = link;
    conn->outbound_stream   = qdr_delivery_message(delivery);
    sys_mutex_init(&conn->common.lock);
    sys_atomic_init(&conn->inbound_disposition, 0);
    sys_atomic_init(&conn->outbound_disposition, 0);

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, cr->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

    extract_metadata_from_stream_CSIDE(conn);

    qd_message_start_unicast_cutthrough(conn->outbound_stream);

    conn->conn_id         = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_CSIDE_IO;

    sys_mutex_lock(&cr->common.lock);
    DEQ_INSERT_TAIL(cr->connections, conn);
    sys_mutex_unlock(&cr->common.lock);

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);

    //
    // The raw connection establishment must be the last thing done in this function.
    // After this call, a separate IO thread may immediately be invoked in the context
    // of the new connection to handle raw connection events.
    //
    pn_proactor_raw_connect(tcplite_context->proactor, conn->raw_conn, cr->adaptor_config.host_port);
}


/**
 * Handle subsequent pushes of the outbound delivery on CSIDE.  This is where delivery completion will be 
 * detected and raw connection write-close will occur.
 */
static void handle_outbound_delivery_CSIDE(tcplite_connection_t *conn, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    if (qd_message_receive_complete(conn->outbound_stream) && !pn_raw_connection_is_write_closed(conn->raw_conn)) {
        printf("handle_outbound_delivery_CSIDE - wake raw_conn for write-close\n");
        pn_raw_connection_wake(conn->raw_conn);
    }
}


/**
 * Manage the steady-state flow of a bi-directional connection from either-side point of view.
 *
 * @param conn Pointer to the TCP connection record
 * @return true if IO processing should be repeated due to state changes
 * @return false if IO processing should suspend until the next external event
 */
static bool manage_flow_XSIDE_IO(tcplite_connection_t *conn)
{
    printf("manage_flow_XSIDE_IO [C%"PRIu64"] - %cSIDE\n", conn->conn_id, conn->listener_side ? 'L' : 'C');

    if (!!conn->inbound_stream && !!conn->raw_conn) {
        //
        // If the raw connection is read-closed, settle and complete the inbound stream/delivery
        // and close out the inbound half of the connection.
        //
        if (pn_raw_connection_is_read_closed(conn->raw_conn) && !!conn->inbound_stream) {
            printf("    read-closed - complete inbound delivery\n");
            qd_message_set_receive_complete(conn->inbound_stream);
            qdr_delivery_continue(tcplite_context->core, conn->inbound_delivery, true);
            qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "TCP_LSIDE_IO - read-close");
            conn->inbound_delivery = 0;
            conn->inbound_stream   = 0;
            return true;
        }

        //
        // If the inbound stream is settled by the peer, there's been an abnormal close on
        // the outbound side.  Close the raw connection.
        //
        if (sys_atomic_get(&conn->inbound_disposition) != 0) {
            printf("    raw connection error - close inbound delivery\n");
            pn_raw_connection_close(conn->raw_conn);
            qdr_delivery_decref(tcplite_context->core, conn->inbound_delivery, "TCP_LSIDE_IO - delivery error");
            conn->inbound_delivery = 0;
            conn->inbound_stream   = 0;
            return false;
        }

        //
        // Produce available read buffers into the inbound stream
        //
        produce_read_buffers_XSIDE_IO(conn->raw_conn, conn->inbound_stream);

        //
        // Issue read buffers when the client stream is producible and the raw connection has capacity for read buffers
        //
        size_t capacity = pn_raw_connection_read_buffers_capacity(conn->raw_conn);
        if (qd_message_can_produce_buffers(conn->inbound_stream) && capacity > 0) {
            grant_read_buffers_IO(conn->raw_conn, capacity);
        }
    }

    if (!!conn->outbound_stream && !!conn->raw_conn) {
        //
        // Drain completed write buffers from the raw connection
        //
        drain_write_buffers_IO(conn->raw_conn);

        //
        // Consume available write buffers from the outbound stream
        //
        consume_write_buffers_XSIDE_IO(conn->raw_conn, conn->outbound_stream);

        //
        // Check the outbound stream for completion.  If complete, write-close the raw connection
        // TODO - do this only if we have consumed all of the stream buffers
        //
        if (qd_message_receive_complete(conn->outbound_stream)) {
            printf("    write-closing the raw connection\n");
            pn_raw_connection_write_close(conn->raw_conn);
            qdr_delivery_set_disposition(conn->outbound_delivery, PN_ACCEPTED);
            qdr_delivery_decref(tcplite_context->core, conn->outbound_delivery, "manage_flow_XSIDE_IO - release outbound");
            conn->outbound_delivery = 0;
            conn->outbound_stream   = 0;
        }
    }

    return false;
}


static bool connection_run_LSIDE_IO(tcplite_connection_t *conn)
{
    bool repeat = false;

    switch (conn->state) {
    case LSIDE_INITIAL:
        //
        // Begin the setup of the inbound and outbound links for this connection.
        //
        link_setup_LSIDE_IO(conn);
        set_state_IO(conn, LSIDE_LINK_SETUP);
        break;

    case LSIDE_LINK_SETUP:
        //
        // If we have a reply-to address, compose the stream message, convert it to a
        // unicast/cut-through stream and send it.
        // Set the state to LSIDE_FLOW.
        //
        if (try_compose_and_send_client_stream_LSIDE_IO(conn)) {
            set_state_IO(conn, LSIDE_FLOW);
            repeat = true;
        }
        break;

    case LSIDE_FLOW:
        //
        // Manage the ongoing bidirectional flow of the connection.
        //
        repeat = manage_flow_XSIDE_IO(conn);
        break;

    default:
        assert(false);
        break;
    }

    return repeat;
}


static bool connection_run_CSIDE_IO(tcplite_connection_t *conn)
{
    bool repeat = false;
    bool credit;

    switch (conn->state) {
    case CSIDE_RAW_CONNECTION_OPENING:
        if (!!conn->error && !!conn->outbound_delivery) {
            //
            // If there was an error during the connection-open, reject the client delivery.
            //
            qdr_delivery_remote_state_updated(tcplite_context->core, conn->outbound_delivery, PN_REJECTED, true, 0, false);
            qdr_delivery_set_context(conn->outbound_delivery, 0);
            qdr_delivery_decref(tcplite_context->core, conn->outbound_delivery, "connection_run_CSIDE_IO - setup failure");
            qdr_link_detach(conn->outbound_link, QD_LOST, 0);
            conn->outbound_delivery = 0;
            conn->outbound_link     = 0;
            conn->outbound_stream   = 0;

            free_connection_IO(conn);
        } else if (!pn_raw_connection_is_read_closed(conn->raw_conn)) {
            link_setup_CSIDE_IO(conn);
            set_state_IO(conn, CSIDE_LINK_SETUP);
        }
        break;

    case CSIDE_LINK_SETUP:
        sys_mutex_lock(&conn->common.lock);
        credit = conn->inbound_credit;
        sys_mutex_unlock(&conn->common.lock);

        if (credit) {
            compose_and_send_server_stream_CSIDE_IO(conn);
            set_state_IO(conn, CSIDE_FLOW);
        }
        break;

    case CSIDE_FLOW:
        //
        // Manage the ongoing bidirectional flow of the connection.
        //
        repeat = manage_flow_XSIDE_IO(conn);
        break;

    default:
        assert(false);
        break;
    }

    return repeat;
}


//=================================================================================
// Handlers for events from the Raw Connections
//=================================================================================
static void on_connection_event_LSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    tcplite_connection_t *conn = (tcplite_connection_t*) context;
    printf("on_connection_event_LSIDE_IO: %s\n", pn_event_type_name(pn_event_type(e)));

    if (pn_event_type(e) == PN_RAW_CONNECTION_DISCONNECTED) {
        free_connection_IO(conn);
        return;
    }

    bool run = true;
    while (run) {
        run = connection_run_LSIDE_IO(conn);
    }
}


static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    tcplite_connection_t *conn = (tcplite_connection_t*) context;
    printf("on_connection_event_CSIDE_IO: %s\n", pn_event_type_name(pn_event_type(e)));

    if (pn_event_type(e) == PN_RAW_CONNECTION_DISCONNECTED) {
        conn->error = pn_raw_connection_condition(conn->raw_conn);
        if (conn->state == CSIDE_FLOW || conn->state == CSIDE_LINK_SETUP) {
            free_connection_IO(conn);
            return;
        }
    }

    bool run = true;
    while (run) {
        run = connection_run_CSIDE_IO(conn);
    }
}


void on_accept(qd_adaptor_listener_t *listener, pn_listener_t *pn_listener, void *context)
{
    printf("on_accept\n");
    tcplite_listener_t *li = (tcplite_listener_t*) context;

    tcplite_connection_t *conn = new_tcplite_connection_t();
    ZERO(conn);

    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (tcplite_common_t*) li;

    conn->listener_side = true;
    conn->state         = LSIDE_INITIAL;
    sys_mutex_init(&conn->common.lock);
    sys_atomic_init(&conn->inbound_disposition, 0);
    sys_atomic_init(&conn->outbound_disposition, 0);

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, li->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);

    conn->conn_id         = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_LSIDE_IO;

    sys_mutex_lock(&li->common.lock);
    DEQ_INSERT_TAIL(li->connections, conn);
    sys_mutex_unlock(&li->common.lock);

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);
    pn_listener_raw_accept(pn_listener, conn->raw_conn);
}

//=================================================================================
// Callbacks from the Core Module
//=================================================================================
static void CORE_activate(void *context, qdr_connection_t *conn)
{
    tcplite_common_t *common = (tcplite_common_t*) qdr_connection_get_context(conn);
    qd_timer_schedule(common->activate_timer, 0);
}


static void CORE_first_attach(void               *context,
                              qdr_connection_t   *conn,
                              qdr_link_t         *link,
                              qdr_terminus_t     *source,
                              qdr_terminus_t     *target,
                              qd_session_class_t  ssn_class)
{
    printf("CORE_first_attach\n");
    tcplite_common_t *common = (tcplite_common_t*) qdr_connection_get_context(conn);
    qdr_link_set_context(link, common);

    qdr_terminus_t *local_source = qdr_terminus(0);
    qdr_terminus_t *local_target = qdr_terminus(0);

    qdr_terminus_set_address_iterator(local_source, qdr_terminus_get_address(target));
    qdr_terminus_set_address_iterator(local_target, qdr_terminus_get_address(source));
    qdr_link_second_attach(link, local_source, local_target);

    if (qdr_link_direction(link) == QD_OUTGOING) {
        qdr_link_flow(tcplite_context->core, link, 1, false);
    }
}


static void CORE_second_attach(void           *context,
                               qdr_link_t     *link,
                               qdr_terminus_t *source,
                               qdr_terminus_t *target)
{
    printf("CORE_second_attach\n");
    tcplite_common_t *common = (tcplite_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
        if (qdr_link_direction(link) == QD_OUTGOING) {
            sys_mutex_lock(&conn->common.lock);
            conn->reply_to = (char*) qd_iterator_copy(qdr_terminus_get_address(source));
            sys_mutex_unlock(&conn->common.lock);

            pn_raw_connection_wake(conn->raw_conn);
        }
    }
}


static void CORE_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
    printf("CORE_detach\n");
}


static void CORE_flow(void *context, qdr_link_t *link, int credit)
{
    printf("CORE_flow credit=%d\n", credit);
    tcplite_common_t *common = (tcplite_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_LISTENER) {
        tcplite_listener_t *li = (tcplite_listener_t*) common;
        if (!li->adaptor_listener) {
            //
            // There is no adaptor listener.  We need to allocate one.
            //
            li->adaptor_listener = qd_adaptor_listener(tcplite_context->qd, &li->adaptor_config, tcplite_context->log_source);

            //
            // Start listening on the socket
            //
            qd_adaptor_listener_listen(li->adaptor_listener, on_accept, li);
        }
    } else if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
        if (qdr_link_direction(link) == QD_INCOMING && credit > 0) {
            sys_mutex_lock(&conn->common.lock);
            conn->inbound_credit = true;
            sys_mutex_unlock(&conn->common.lock);
            pn_raw_connection_wake(conn->raw_conn);
        }
    }
}


static void CORE_offer(void *context, qdr_link_t *link, int delivery_count)
{
}


static void CORE_drained(void *context, qdr_link_t *link)
{
}


static void CORE_drain(void *context, qdr_link_t *link, bool mode)
{
}


static int CORE_push(void *context, qdr_link_t *link, int limit)
{
    return qdr_link_process_deliveries(tcplite_context->core, link, limit);
}


static uint64_t CORE_deliver_outbound(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    printf("CORE_deliver_outbound - dlv: %lx settled: %s completed: %s\n",
           (long) delivery, settled ? "true" : "false",
           qd_message_receive_complete(qdr_delivery_message(delivery)) ? "true" : "false");

    tcplite_common_t *common = (tcplite_common_t*) qdr_delivery_get_context(delivery);
    if (!common) {
        common = (tcplite_common_t*) qdr_link_get_context(link);
    }

    if (common->context_type == TL_CONNECTOR) {
        handle_first_outbound_delivery_CSIDE((tcplite_connector_t*) common, link, delivery);
    } else if (common->context_type == TL_CONNECTION) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;
        if (conn->listener_side) {
            handle_outbound_delivery_LSIDE(conn, link, delivery);
        } else {
            handle_outbound_delivery_CSIDE(conn, link, delivery, settled);
        }
    } else {
        assert(false);
    }

    return 0;
}


static int CORE_get_credit(void *context, qdr_link_t *link)
{
    return 1;
}


static void CORE_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    printf("CORE_delivery_update - disp: 0x%"PRIx64" settled: %s\n", disp, settled ? "true" : "false");

    bool              need_wake = false;
    tcplite_common_t *common = (tcplite_common_t*) qdr_delivery_get_context(dlv);
    if (!!common && common->context_type == TL_CONNECTION && settled) {
        tcplite_connection_t *conn = (tcplite_connection_t*) common;

        sys_mutex_lock(&common->lock);
        if (dlv == conn->outbound_delivery) {
            sys_atomic_set(&conn->outbound_disposition, (uint32_t) disp);
            need_wake = true;
        } else if (dlv == conn->inbound_delivery) {
            sys_atomic_set(&conn->inbound_disposition, (uint32_t) disp);
            need_wake = true;
        }
        sys_mutex_unlock(&common->lock);

        if (need_wake) {
            pn_raw_connection_wake(conn->raw_conn);
        }
    }
}


static void CORE_connection_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
}


static void CORE_connection_trace(void *context, qdr_connection_t *conn, bool trace)
{
}


//=================================================================================
// Entrypoints for Management
//=================================================================================
QD_EXPORT void *qd_dispatch_configure_tcp_listener(qd_dispatch_t *qd, qd_entity_t *entity)
{
    tcplite_listener_t *li = new_tcplite_listener_t();
    ZERO(li);

    if (qd_load_adaptor_config(&li->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(tcplite_context->log_source, QD_LOG_ERROR, "Unable to create tcp listener: %s", qd_error_message());
        free_tcplite_listener_t(li);
        return 0;
    }

    qd_log(tcplite_context->log_source, QD_LOG_INFO,
            "Configured TcpListener for %s, %s:%s",
            li->adaptor_config.address, li->adaptor_config.host, li->adaptor_config.port);

    li->common.activate_timer = qd_timer(tcplite_context->qd, on_core_activate_LSIDE, li);
    li->common.context_type   = TL_LISTENER;

    sys_mutex_lock(&tcplite_context->lock);
    DEQ_INSERT_TAIL(tcplite_context->listeners, li);
    sys_mutex_unlock(&tcplite_context->lock);

    TL_setup_listener(li);

    return li;
}


QD_EXPORT void qd_dispatch_delete_tcp_listener(qd_dispatch_t *qd, void *impl)
{
    tcplite_listener_t *li = (tcplite_listener_t*) impl;
    if (li) {

        sys_mutex_lock(&tcplite_context->lock);
        DEQ_REMOVE(tcplite_context->listeners, li);
        sys_mutex_unlock(&tcplite_context->lock);

        qd_log(tcplite_context->log_source, QD_LOG_INFO,
               "Deleted TcpListener for %s, %s:%s",
               li->adaptor_config.address, li->adaptor_config.host, li->adaptor_config.port);

        qd_timer_free(li->common.activate_timer);
        free_tcplite_listener_t(li);
    }
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpListener(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}


QD_EXPORT void *qd_dispatch_configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    tcplite_connector_t *cr = new_tcplite_connector_t();
    ZERO(cr);

    if (qd_load_adaptor_config(&cr->adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(tcplite_context->log_source, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        free_tcplite_connector_t(cr);
        return 0;
    }

    cr->common.activate_timer = qd_timer(tcplite_context->qd, on_core_activate_CSIDE, cr);
    cr->common.context_type   = TL_CONNECTOR;

    qd_log(tcplite_context->log_source, QD_LOG_INFO,
            "Configured TcpConnector for %s, %s:%s",
            cr->adaptor_config.address, cr->adaptor_config.host, cr->adaptor_config.port);

    DEQ_INSERT_TAIL(tcplite_context->connectors, cr);

    TL_setup_connector(cr);

    return cr;
}


QD_EXPORT void qd_dispatch_delete_tcp_connector(qd_dispatch_t *qd, void *impl)
{
    tcplite_connector_t *cr = (tcplite_connector_t*) impl;
    if (cr) {

        sys_mutex_lock(&tcplite_context->lock);
        DEQ_REMOVE(tcplite_context->connectors, cr);
        sys_mutex_unlock(&tcplite_context->lock);

        qd_log(tcplite_context->log_source, QD_LOG_INFO,
               "Deleted TcpConnector for %s, %s:%s",
               cr->adaptor_config.address, cr->adaptor_config.host, cr->adaptor_config.port);

        qd_timer_free(cr->common.activate_timer);
        free_tcplite_connector_t(cr);
    }
}


QD_EXPORT qd_error_t qd_entity_refresh_tcpConnector(qd_entity_t* entity, void *impl)
{
    return QD_ERROR_NONE;
}


//=================================================================================
// Interface to Protocol Adaptor registration
//=================================================================================
static void ADAPTOR_init(qdr_core_t *core, void **adaptor_context)
{
    tcplite_context = NEW(tcplite_context_t);
    ZERO(tcplite_context);

    tcplite_context->core   = core;
    tcplite_context->qd     = qdr_core_dispatch(core);
    tcplite_context->server = tcplite_context->qd->server;
    tcplite_context->pa     = qdr_protocol_adaptor(core, "tcp_lite", (void*) tcplite_context,
                                                   CORE_activate,
                                                   CORE_first_attach,
                                                   CORE_second_attach,
                                                   CORE_detach,
                                                   CORE_flow,
                                                   CORE_offer,
                                                   CORE_drained,
                                                   CORE_drain,
                                                   CORE_push,
                                                   CORE_deliver_outbound,
                                                   CORE_get_credit,
                                                   CORE_delivery_update,
                                                   CORE_connection_close,
                                                   CORE_connection_trace);
    tcplite_context->log_source = qd_log_source("TCP_ADAPTOR");
    sys_mutex_init(&tcplite_context->lock);
    tcplite_context->proactor = qd_server_proactor(tcplite_context->server);
}


static void ADAPTOR_final(void *adaptor_context)
{
    while (DEQ_HEAD(tcplite_context->connectors)) {
        qd_dispatch_delete_tcp_connector(tcplite_context->qd, DEQ_HEAD(tcplite_context->connectors));
    }
    while (DEQ_HEAD(tcplite_context->listeners)) {
        qd_dispatch_delete_tcp_listener(tcplite_context->qd, DEQ_HEAD(tcplite_context->listeners));
    }
    qdr_protocol_adaptor_free(tcplite_context->core, tcplite_context->pa);
    sys_mutex_free(&tcplite_context->lock);
    free(tcplite_context);
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("tcp-lite", ADAPTOR_init, ADAPTOR_final)
