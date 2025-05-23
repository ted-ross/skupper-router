#ifndef __qd_listener_h__
#define __qd_listener_h__ 1
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

// Proactor Listener for accepting AMQP connection requests

#include "server_config.h"

#include "qpid/dispatch/atomic.h"
#include "qpid/dispatch/server.h"

typedef struct qd_lws_listener_t qd_lws_listener_t;
typedef struct qd_server_t       qd_server_t;
typedef struct pn_listener_t     pn_listener_t;
typedef struct vflow_record_t    vflow_record_t;
typedef struct qd_connection_t   qd_connection_t;
typedef struct qd_tls_config_t   qd_tls_config_t;

/**
 * Listener objects represent the desire to accept incoming AMQP transport connections.
 */
typedef struct qd_listener_t qd_listener_t;
struct qd_listener_t {
    /* May be referenced by connection_manager and pn_listener_t */
    qd_handler_context_t      type;
    sys_atomic_t              ref_count;
    sys_atomic_t              connection_count;
    qd_server_t              *server;
    qd_server_config_t        config;
    pn_listener_t            *pn_listener;
    qd_lws_listener_t        *http;
    DEQ_LINKS(qd_listener_t);
    bool                      exit_on_error;
    vflow_record_t           *vflow_record;

    // TLS Configuration. Keep a local copy of the TLS ordinals to monitor changes by management
    qd_tls_config_t          *tls_config;
    uint64_t                  tls_ordinal;
    uint64_t                  tls_oldest_valid_ordinal;
};

DEQ_DECLARE(qd_listener_t, qd_listener_list_t);

/**
 * Management call to create a qd_listener_t from a configuration entity.
 *
 * This provisions the listener but does not activate it. To start accepting new connections activate it by calling
 * qd_listener_listen().
 *
 * Returns 0 and sets qd_error_code() if creation fails
 */
qd_listener_t *qd_listener_create(qd_dispatch_t *qd, qd_entity_t *entity);

/**
 * Management call to delete a listener.
 *
 * This deactivates the listener preventing it from accepting new incoming connections and drops the reference
 * count. The listener pointer becomes invalid after this call.
 *
 * @param li the listener to delete
 * @param on_shutdown true if the listener is being deleted as part of router shutdown
 */
void qd_listener_delete(qd_listener_t *li, bool on_shutdown);

/**
 * Drop a reference to the listener.
 *
 * The listener pointer becomes invalid on return from this call.
 */
void qd_listener_decref(qd_listener_t *li);

/**
 * Listen for incoming connections, return true if listening succeeded.
 */
bool qd_listener_listen(qd_listener_t *li);

// Read only access to the listeners configuration
//
const qd_server_config_t *qd_listener_config(const qd_listener_t *li);

// add a new child connection to the parent listener
//
void qd_listener_add_connection(qd_listener_t *li, qd_connection_t *qd_conn);

// remove the connection with its parent listener
// NOTE WELL: may free the listener if this connection is holding the last reference
// to it
void qd_listener_remove_connection(qd_listener_t *li, qd_connection_t *qd_conn);

// account for an added link to the listener
void qd_listener_add_link(qd_listener_t *li);

// account for a removed link from the listener
void qd_listener_remove_link(qd_listener_t *li);

#endif
