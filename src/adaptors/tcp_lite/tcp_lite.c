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
    tcplite_common_t     common;
    DEQ_LINKS(tcplite_connector_t);
    qd_adaptor_config_t  adaptor_config;
    uint64_t             conn_id;
    qdr_connection_t    *conn;
    uint64_t             link_id;
    qdr_link_t          *out_link;
} tcplite_connector_t;

ALLOC_DEFINE(tcplite_connector_t);


typedef enum {
    LSIDE_INITIAL,
    LSIDE_LINK_SETUP,
    LSIDE_STREAM_START,
    LSIDE_FLOW,
    CSIDE_INITIAL,
    CSIDE_RAW_CONNECTION_OPENING,
    CSIDE_STREAM_START,
    CSIDE_FLOW
} tcplite_connection_state_t;
ENUM_DECLARE(tcplite_connection_state);

static const char *state_names[] =
{ "LSIDE_INITIAL", "LSIDE_LINK_SETUP", "LSIDE_STREAM_START", "LSIDE_FLOW",
  "CSIDE_INITIAL", "CSIDE_RAW_CONNECTION_OPENING", "CSIDE_STREAM_START", "CSIDE_FLOW"
};
ENUM_DEFINE(tcplite_connection_state, state_names);

#define RAW_BUFFER_BATCH_SIZE 16


typedef struct tcplite_connection_t {
    tcplite_common_t            common;
    DEQ_LINKS(tcplite_connection_t);
    pn_raw_connection_t        *raw_conn;
    qdr_link_t                 *client_link;
    qd_message_t               *client_stream;
    qdr_delivery_t             *client_delivery;
    qdr_link_t                 *server_link;
    qd_message_t               *server_stream;
    qdr_delivery_t             *server_delivery;
    const char                 *reply_to;
    uint64_t                    conn_id;
    uint64_t                    client_link_id;
    uint64_t                    server_link_id;
    qd_handler_context_t        context;
    tcplite_connection_state_t  state;
    bool                        listener_side;
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

    li->in_link = qdr_link_first_attach(li->conn, QD_INCOMING, 0, target, "tcp.ingress.in", 0, false, 0, &li->link_id);
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

    cr->out_link = qdr_link_first_attach(cr->conn, QD_OUTGOING, source, 0, "tcp.egress.out", 0, false, 0, &cr->link_id);
    qdr_link_set_context(cr->out_link, cr);
    qdr_link_flow(tcplite_context->core, cr->out_link, 5, false);
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


static bool produce_read_buffers_IO(pn_raw_connection_t *raw_conn, qd_message_t *stream)
{
    if (qd_message_can_produce_buffers(stream)) {
        qd_buffer_list_t qd_buffers = DEQ_EMPTY;
        pn_raw_buffer_t  raw_buffers[RAW_BUFFER_BATCH_SIZE];
        size_t           count;

        while ((count = pn_raw_connection_take_read_buffers(raw_conn, raw_buffers, RAW_BUFFER_BATCH_SIZE))) {
            for (size_t i = 0; i < count; i++) {
                qd_buffer_t *buf = (qd_buffer_t*) raw_buffers[i].context;
                if (qd_buffer_size(buf) > 0) {
                    DEQ_INSERT_TAIL(qd_buffers, buf);
                } else {
                    qd_buffer_free(buf);
                }
            }
        }

        if (!DEQ_IS_EMPTY(qd_buffers)) {
            qd_message_produce_buffers(stream, &qd_buffers);
            return true;
        }
    }

    return false;
}


static void link_setup_LSIDE_IO(tcplite_connection_t *conn)
{
    tcplite_listener_t *li = (tcplite_listener_t*) conn->common.parent;
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_t *source = qdr_terminus(0);

    qdr_terminus_set_address(target, li->adaptor_config.address);
    qdr_terminus_set_dynamic(source);
    
    conn->client_link = qdr_link_first_attach(li->conn, QD_INCOMING, qdr_terminus(0), target, "tcp.lside.in", 0, true, 0, &conn->client_link_id);
    qdr_link_set_context(conn->client_link, conn);
    conn->server_link = qdr_link_first_attach(li->conn, QD_OUTGOING, source, qdr_terminus(0), "tcp.lside.out", 0, true, 0, &conn->server_link_id);
    qdr_link_set_context(conn->server_link, conn);
    qdr_link_flow(tcplite_context->core, conn->server_link, 1, false);
}


static bool try_compose_and_send_client_stream_LSIDE_IO(tcplite_connection_t *conn)
{
    tcplite_listener_t  *li = (tcplite_listener_t*) conn->common.parent;
    qd_composed_field_t *props = 0;

    //
    // The lock is used here to protect access to the reply_to field.  This field is written
    // by an IO thread associated with the core connection, not this raw connection.
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
        //qd_compose_insert_null(props);                              // correlation-id
        //qd_compose_insert_null(props);                              // content-type
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

    conn->client_stream = qd_message();
    qd_message_set_streaming_annotation(conn->client_stream);

    //
    // Add the application properties
    //
    props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, props);
    qd_compose_start_map(props);
    qd_compose_insert_symbol(props, QD_AP_FLOW_ID);
    vflow_serialize_identity(conn->common.vflow, props);
    qd_compose_end_map(props);

    qd_message_compose_2(conn->client_stream, props, false);
    qd_compose_free(props);

    //
    // Start cut-through mode for this stream.
    //
    qd_message_start_unicast_cutthrough(conn->client_stream);

    //
    // Start latency timer for this cross-van connection.
    //
    vflow_latency_start(conn->common.vflow);

    conn->client_delivery = qdr_link_deliver(conn->client_link, conn->client_stream, 0, false, 0, 0, 0, 0);
    qdr_delivery_incref(conn->client_delivery, "TCP_LSIDE_IO");

    qd_log(tcplite_context->log_source, QD_LOG_DEBUG,
            "[C%" PRIu64 "][L%" PRIu64 "] Initiating listener side empty client stream message",
            conn->conn_id, conn->client_link_id);

    return true;
}


static void extract_metadata_from_stream_CSIDE(tcplite_connection_t *conn)
{
    qd_iterator_t *f_iter  = qd_message_field_iterator(conn->client_stream, QD_FIELD_REPLY_TO);
    qd_iterator_t *ap_iter = qd_message_field_iterator(conn->client_stream, QD_FIELD_APPLICATION_PROPERTIES);

    if (!!f_iter) {
        conn->reply_to = (char*) qd_iterator_copy(f_iter);
        qd_iterator_free(f_iter);
    }

    if (!!ap_iter) {
        qd_parsed_field_t *ap = qd_parse(ap_iter);

        if (!!ap) {
            do {
                if (!qd_parse_ok(ap) || !qd_parse_is_map(ap)) {
                    break;
                }

                uint32_t count = qd_parse_sub_count(ap);
                qd_parsed_field_t *id_value = 0;
                for (uint32_t i = 0; i < count; i++) {
                    qd_parsed_field_t *key = qd_parse_sub_key(ap, i);
                    if (key == 0) {
                        break;
                    }
                    qd_iterator_t *key_iter = qd_parse_raw(key);
                    if (!!key_iter && qd_iterator_equal(key_iter, (const unsigned char*) QD_AP_FLOW_ID)) {
                        id_value = qd_parse_sub_value(ap, i);
                    }
                }

                if (!!id_value) {
                    vflow_set_ref_from_parsed(conn->common.vflow, VFLOW_ATTRIBUTE_COUNTERFLOW, id_value);
                }
            } while (false);
            qd_parse_free(ap);
        }

        qd_iterator_free(ap_iter);
    }
}


static void handle_incoming_delivery_CSIDE(tcplite_connector_t *cr, qdr_link_t *link, qdr_delivery_t *delivery)
{
    printf("handle_incoming_delivery_CSIDE\n");
    tcplite_connection_t *conn = new_tcplite_connection_t();
    ZERO(conn);

    qdr_delivery_incref(delivery, "CORE_deliver CSIDE");

    conn->common.context_type = TL_CONNECTION;
    conn->common.parent       = (tcplite_common_t*) cr;

    conn->listener_side   = false;
    conn->state           = CSIDE_RAW_CONNECTION_OPENING;
    conn->client_delivery = delivery;
    conn->client_link     = link;
    conn->client_stream   = qdr_delivery_message(delivery);
    sys_mutex_init(&conn->common.lock);

    extract_metadata_from_stream_CSIDE(conn);

    qd_message_start_unicast_cutthrough(conn->client_stream);

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, cr->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);

    conn->conn_id         = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_CSIDE_IO;

    conn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(conn->raw_conn, &conn->context);
    pn_proactor_raw_connect(tcplite_context->proactor, conn->raw_conn, cr->adaptor_config.host_port);
}


/**
 * Manage the steady-state flow of a bi-directional connection from the listener-side point of view.
 *
 * @param conn Pointer to the TCP connection record
 * @return true if IO processing should be repeated due to state changes
 * @return false if IO processing should suspend until the next external event
 */
static bool manage_flow_LSIDE_IO(tcplite_connection_t *conn)
{
    printf("manage_flow_LSIDE_IO [C%"PRIu64"]\n", conn->conn_id);

    if (!!conn->raw_conn && pn_raw_connection_is_read_closed(conn->raw_conn) && pn_raw_connection_is_write_closed(conn->raw_conn)) {
        drain_read_buffers_IO(conn->raw_conn);
    }

    if (!!conn->client_stream && !!conn->raw_conn) {
        //
        // Respond to closure/half-closure of the raw connection
        //
        if (pn_raw_connection_is_read_closed(conn->raw_conn) && !!conn->client_stream) {
            qd_message_set_send_complete(conn->client_stream);
            qdr_delivery_continue(tcplite_context->core, conn->client_delivery, true);
            qdr_delivery_decref(tcplite_context->core, conn->client_delivery, "TCP_LSIDE_IO");
            conn->client_delivery = 0;
            conn->client_stream   = 0;

            // TEMPORARY - remove once the full loop is complete
            pn_raw_connection_write_close(conn->raw_conn);

            return true;
        }

        //
        // Respond to termination of either of the streams
        //

        //
        // Produce available read buffers into the client stream
        //
        produce_read_buffers_IO(conn->raw_conn, conn->client_stream);

        //
        // Issue read buffers when the client stream is producible and the raw connection has capacity for read buffers
        //
        size_t capacity = pn_raw_connection_read_buffers_capacity(conn->raw_conn);
        if (qd_message_can_produce_buffers(conn->client_stream) && capacity > 0) {
            grant_read_buffers_IO(conn->raw_conn, capacity);
        }
    }

    //
    // Consume write buffers from the inbound stream when the connection is writable
    //

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
        repeat = manage_flow_LSIDE_IO(conn);
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

    switch (conn->state) {
        case CSIDE_RAW_CONNECTION_OPENING:
            break;

        case CSIDE_STREAM_START:
            break;

        case CSIDE_FLOW:
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
    printf("on_connection_event_LSIDE_IO: %s\n", pn_event_type_name(pn_event_type(e)));
    while (connection_run_LSIDE_IO((tcplite_connection_t*) context)) {}
}


static void on_connection_event_CSIDE_IO(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    printf("on_connection_event_CSIDE_IO: %s\n", pn_event_type_name(pn_event_type(e)));
    while (connection_run_CSIDE_IO((tcplite_connection_t*) context)) {}
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

    conn->common.vflow = vflow_start_record(VFLOW_RECORD_FLOW, li->common.vflow);
    vflow_set_uint64(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, 0);
    vflow_add_rate(conn->common.vflow, VFLOW_ATTRIBUTE_OCTETS, VFLOW_ATTRIBUTE_OCTET_RATE);

    conn->conn_id         = qd_server_allocate_connection_id(tcplite_context->server);
    conn->context.context = conn;
    conn->context.handler = on_connection_event_LSIDE_IO;

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
            conn->reply_to = (const char*) qd_iterator_copy(qdr_terminus_get_address(source));
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


static uint64_t CORE_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    printf("CORE_deliver\n");
    tcplite_common_t *common = (tcplite_common_t*) qdr_link_get_context(link);

    if (common->context_type == TL_CONNECTOR) {
        handle_incoming_delivery_CSIDE((tcplite_connector_t*) common, link, delivery);
    }

    return PN_ACCEPTED;
}


static int CORE_get_credit(void *context, qdr_link_t *link)
{
    return 1;
}


static void CORE_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
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
                                                   CORE_deliver,
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
