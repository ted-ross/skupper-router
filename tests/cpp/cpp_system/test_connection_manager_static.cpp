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

#include "cpp_stub.h"
#include "qdr_doctest.hpp"
// helpers.hpp goes after qdr_doctest.hpp
#include "helpers.hpp"

extern "C" {
#include "entity.h"
#include "qpid/dispatch/tls_common.h"
#include "qpid/dispatch/threading.h"
}

#include <Python.h>

#include <sys/mman.h>

#include <set>
#include <string>
#include <thread>
using std::string_literals::operator""s;

extern "C" {
QD_EXPORT void *qd_tls_configure_ssl_profile(qd_dispatch_t *qd, qd_entity_t *entity);
QD_EXPORT void qd_tls_delete_ssl_profile(qd_dispatch_t *qd, void *impl);
}

// uses the python-exposed api to run the code; hopefully this is robust enough
static void check_password(qd_dispatch_t *qd, const char *password, const char *expected, bool expect_success = true)
{
    PyObject *pyObject = PyDict_New();
    PyObject *item     = PyUnicode_FromString(password);
    PyObject *name     = PyUnicode_FromString("profileName");

    PyDict_SetItemString(pyObject, "name", name);
    PyDict_SetItemString(pyObject, "password", item);

    sys_thread_proactor_mode_t old_mode = sys_thread_proactor_set_mode(SYS_THREAD_PROACTOR_MODE_TIMER, 0);

    qd_entity_t *entity = reinterpret_cast<qd_entity_t *>(pyObject);
    void *phandle       = qd_tls_configure_ssl_profile(qd, entity);
    if (expect_success) {
        qd_ssl2_profile_t profile;
        REQUIRE(phandle != nullptr);
        CHECK(qd_tls_read_ssl_profile("profileName", &profile) != nullptr);
        CHECK(profile.password == std::string{expected});
        qd_tls_cleanup_ssl_profile(&profile);
        qd_tls_delete_ssl_profile(qd, phandle);
    } else {
        REQUIRE(phandle == nullptr);
    }
    Py_DECREF(name);
    Py_DECREF(item);
    Py_DECREF(pyObject);

    (void) sys_thread_proactor_set_mode(old_mode, 0);
}

TEST_CASE("qd_dispatch_configure_ssl_profile")
{
    // This initialization would be sufficient, but then there is no way to selectively
    //  free only what has been allocated by this nonstandard approach
    //    qd_dispatch_t *qd = qd_dispatch(NULL, false);
    //    qd_dispatch_prepare(qd);

    std::thread([] {
        QDR qdr{};
        qdr.initialize();
        qdr.wait();
        qd_dispatch_t *qd = qdr.qd;

        // previous functions dropped Python GIL
        PyGILState_Ensure();

        SUBCASE("qd_config_process_password")
        {
            check_password(qd, "", "");
            check_password(qd, "swordfish", "swordfish");

            check_password(qd, "pass:", "");
            check_password(qd, "pass:swordfish", "swordfish");
            check_password(qd, "literal:swordfish", "swordfish");

            SUBCASE("env: (_STUB_)")
            {
                check_password(qd, "env:no_such_env_variable", "", false);

                Stub s{};
                s.set(
                    getenv, +[](const char *name) {
                        // on Ubuntu, these variables are being looked up
                        std::set<std::string> ignored = {"TZ", "TZDIR"};
                        if (ignored.find(name) != ignored.end()) {
                            return "";
                        }

                        CHECK(name == "some_env_variable"s);
                        return "some_password";
                    });
                check_password(qd, "env:some_env_variable", "some_password");
            }

            SUBCASE("file: (_STUB_)")
            {
                // is this behavior intended?
                check_password(qd, "file:/dev/null", "file:/dev/null");

                {
                    Stub s{};
                    s.set(
                        fopen, +[](const char *name, const char *mode) {
                            CHECK(name == "/some/file"s);
                            CHECK(mode == "r"s);

                            // create fake file in memory and return it
                            int fd           = memfd_create("tmpfile", 0);
                            const char *data = "some_file_pass";
                            CHECK(write(fd, data, strlen(data)) == strlen(data));
                            CHECK(lseek(fd, 0, SEEK_SET) == 0);
                            return fdopen(fd, "r");
                        });
                    check_password(qd, "file:/some/file", "some_file_pass");
                }
            }
        }
        qdr.deinitialize();
    }).join();
}
