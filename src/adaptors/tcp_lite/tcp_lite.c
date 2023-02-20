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
#include <qpid/dispatch/alloc_pool.h>
#include <qpid/dispatch/io_module.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/server.h>
#include <qpid/dispatch/log.h>
#include <proton/proactor.h>
#include <proton/raw_connection.h>

#include "dispatch_private.h"
#include "adaptors/adaptor_common.h"


typedef struct {
    qd_adaptor_config_t  adaptor_config;
    qd_timer_t          *activate_timer;
    uint64_t             conn_id;
    qdr_connection_t    *conn;
} tcplite_common_t;


typedef struct tcplite_listener_t {
    tcplite_common_t     common;
    DEQ_LINKS(struct tcplite_listener_t);
    uint64_t             link_id;
    qdr_link_t          *in_link;
} tcplite_listener_t;

ALLOC_DECLARE(tcplite_listener_t);
ALLOC_DEFINE(tcplite_listener_t);


typedef struct tcplite_connector_t {
    tcplite_common_t     common;
    DEQ_LINKS(struct tcplite_connector_t);
    uint64_t             link_id;
    qdr_link_t          *out_link;
} tcplite_connector_t;

ALLOC_DECLARE(tcplite_connector_t);
ALLOC_DEFINE(tcplite_connector_t);


typedef struct tcplist_connection_t {
    DEQ_LINKS(struct tcplite_connection_t);
} tcplite_connection_t;

ALLOC_DECLARE(tcplite_connection_t);
ALLOC_DEFINE(tcplite_connection_t);


DEQ_DECLARE(tcplite_listener_t,   tcplite_listener_list_t);
DEQ_DECLARE(tcplite_connector_t,  tcplite_connector_list_t);
DEQ_DECLARE(tcplite_connection_t, tcplite_connection_list_t);


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
} tcplite_context_t;


static tcplite_context_t *tcplite_context;


//=================================================================================
// Activation Handler
//=================================================================================
static void on_activate(void *context)
{
    tcplite_common_t *common = (tcplite_common_t*) context;
    qdr_connection_process(common->conn);
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


static qdr_connection_t *TL_open_connection(uint64_t conn_id, bool incoming)
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
    li->common.conn_id = qd_server_allocate_connection_id(tcplite_context->server);
    li->common.conn    = TL_open_connection(li->common.conn_id, true);
    qdr_connection_set_context(li->common.conn, li);

    //
    // Attach an in-link to represent the desire to send connection streams to the address
    //
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, li->common.adaptor_config.address);

    li->in_link = qdr_link_first_attach(li->common.conn, QD_INCOMING, 0, target, "tcp.ingress.in", 0, false, 0, &li->link_id);
    qdr_link_set_context(li->in_link, li);
}


static void TL_setup_connector(tcplite_connector_t *cr)
{
    //
    // Set up a core connection to handle all of the links and deliveries for this connector
    //
    cr->common.conn_id = qd_server_allocate_connection_id(tcplite_context->server);
    cr->common.conn    = TL_open_connection(cr->common.conn_id, false);
    qdr_connection_set_context(cr->common.conn, cr);

    //
    // Attach an out-link to represent our desire to receive connection streams for the address
    //
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, cr->common.adaptor_config.address);

    cr->out_link = qdr_link_first_attach(cr->common.conn, QD_OUTGOING, source, 0, "tcp.egress.out", 0, false, 0, &cr->link_id);
    qdr_link_set_context(cr->out_link, cr);
    qdr_link_flow(tcplite_context->core, cr->out_link, 5, false);
}


//=================================================================================
// Handlers for events from the Raw Connections
//=================================================================================



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
}


static void CORE_second_attach(void           *context,
                               qdr_link_t     *link,
                               qdr_terminus_t *source,
                               qdr_terminus_t *target)
{
    printf("CORE_second_attach\n");
}


static void CORE_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
    printf("CORE_detach\n");
}


static void CORE_flow(void *context, qdr_link_t *link, int credit)
{
    printf("CORE_flow credit=%d\n", credit);
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
    return 0;
}


static uint64_t CORE_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    return PN_ACCEPTED;
}


static int CORE_get_credit(void *context, qdr_link_t *link)
{
    return 0;
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

    if (qd_load_adaptor_config(&li->common.adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(tcplite_context->log_source, QD_LOG_ERROR, "Unable to create tcp listener: %s", qd_error_message());
        free_tcplite_listener_t(li);
        return 0;
    }

    qd_log(tcplite_context->log_source, QD_LOG_INFO,
            "Configured TcpListener for %s, %s:%s",
            li->common.adaptor_config.address, li->common.adaptor_config.host, li->common.adaptor_config.port);

    li->common.activate_timer = qd_timer(tcplite_context->qd, on_activate, li);

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
               li->common.adaptor_config.address, li->common.adaptor_config.host, li->common.adaptor_config.port);

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

    if (qd_load_adaptor_config(&cr->common.adaptor_config, entity) != QD_ERROR_NONE) {
        qd_log(tcplite_context->log_source, QD_LOG_ERROR, "Unable to create tcp connector: %s", qd_error_message());
        free_tcplite_connector_t(cr);
        return 0;
    }

    cr->common.activate_timer = qd_timer(tcplite_context->qd, on_activate, cr);

    qd_log(tcplite_context->log_source, QD_LOG_INFO,
            "Configured TcpConnector for %s, %s:%s",
            cr->common.adaptor_config.address, cr->common.adaptor_config.host, cr->common.adaptor_config.port);

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
               cr->common.adaptor_config.address, cr->common.adaptor_config.host, cr->common.adaptor_config.port);

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
