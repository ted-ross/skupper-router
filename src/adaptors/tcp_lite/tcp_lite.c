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
#include <proton/proactor.h>
#include <proton/raw_connection.h>

#include "dispatch_private.h"
#include "adaptors/adaptor_common.h"


typedef struct tcplite_listener_t {
    DEQ_LINKS(struct tcplite_listener_t);
    qd_adaptor_config_t *config;
} tcplite_listener_t;

ALLOC_DECLARE(tcplite_listener_t);
ALLOC_DEFINE(tcplite_listener_t);


typedef struct tcplite_connector_t {
    DEQ_LINKS(struct tcplite_connector_t);
    qd_adaptor_config_t *config;
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
} tcplite_context_t;


static tcplite_context_t *context;


//=================================================================================
// Helper Functions
//=================================================================================



//=================================================================================
// Handlers for events from the Raw Connections
//=================================================================================



//=================================================================================
// Callbacks from the Core Module
//=================================================================================
static void CORE_activate(void *context, qdr_connection_t *conn)
{
}


static void CORE_first_attach(void               *context,
                              qdr_connection_t   *conn,
                              qdr_link_t         *link,
                              qdr_terminus_t     *source,
                              qdr_terminus_t     *target,
                              qd_session_class_t  ssn_class)
{
}


static void CORE_second_attach(void           *context,
                               qdr_link_t     *link,
                               qdr_terminus_t *source,
                               qdr_terminus_t *target)
{
}


static void CORE_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
}


static void CORE_flow(void *context, qdr_link_t *link, int credit)
{
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

    return li;
}


QD_EXPORT void *qd_dispatch_configure_tcp_connector(qd_dispatch_t *qd, qd_entity_t *entity)
{
    tcplite_connector_t *co = new_tcplite_connector_t();
    ZERO(co);

    return co;
}


//=================================================================================
// Interface to Protocol Adaptor registration
//=================================================================================
static void ADAPTOR_init(qdr_core_t *core, void **adaptor_context)
{
    context = NEW(tcplite_context_t);
    ZERO(context);

    context->core   = core;
    context->qd     = qdr_core_dispatch(core);
    context->server = context->qd->server;
    context->pa     = qdr_protocol_adaptor(core, "tcp_lite", (void*) context,
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
}


static void ADAPTOR_final(void *adaptor_context)
{
    qdr_protocol_adaptor_free(context->core, context->pa);
    free(context);
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("tcp-lite", ADAPTOR_init, ADAPTOR_final)
