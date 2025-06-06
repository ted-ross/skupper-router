#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""System test library, provides tools for tests that start multiple processes,
with special support for skupper-router processes.

Features:
- Create separate directories for each test.
- Save logs, sub-process output, core files etc.
- Automated clean-up after tests: kill sub-processes etc.
- Tools to manipulate router configuration files.
- Sundry other tools.
"""

import errno
import fcntl
import hashlib
import json
import logging
import os
import pathlib
import queue as Queue
import random
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import unittest
import uuid
from copy import copy
from datetime import datetime
from subprocess import PIPE, STDOUT, TimeoutExpired
from threading import Event
from threading import Thread
from typing import Callable, TextIO, List, Optional, Tuple, Union, Type, Set, Any, TypeVar, Dict, Mapping
import __main__

import proton
import proton.utils
from proton import Delivery
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import AtLeastOnce, Container
from proton.reactor import AtMostOnce

from skupper_router.management.client import Node
from skupper_router.management.error import NotFoundStatus, BadRequestStatus

current_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
path_parent = os.path.dirname(os.path.join(os.path.dirname(os.path.abspath(__file__))))
expandvars_script_folder = os.path.join(path_parent, 'scripts')
script_file = "expandvars.py"

# Optional modules
MISSING_MODULES = []

# Management entity type names
ALLOCATOR_TYPE = 'io.skupper.router.allocator'
AMQP_CONNECTOR_TYPE = 'io.skupper.router.connector'
AMQP_LISTENER_TYPE = 'io.skupper.router.listener'
CONFIG_ADDRESS_TYPE = 'io.skupper.router.router.config.address'
CONFIG_AUTOLINK_TYPE = 'io.skupper.router.router.config.autoLink'
CONFIG_ENTITY_TYPE = 'io.skupper.router.configurationEntity'
CONNECTION_TYPE = 'io.skupper.router.connection'
DUMMY_TYPE = 'io.skupper.router.dummy'
ENTITY_TYPE = 'io.skupper.router.entity'
HTTP_REQ_INFO_TYPE = 'io.skupper.router.httpRequestInfo'
LOG_STATS_TYPE = 'io.skupper.router.logStats'
LOG_TYPE = 'io.skupper.router.log'
MANAGEMENT_TYPE = 'io.skupper.router.management'
OPER_ENTITY_TYPE = 'io.skupper.router.operationalEntity'
ROUTER_ADDRESS_TYPE = 'io.skupper.router.router.address'
ROUTER_LINK_TYPE = 'io.skupper.router.router.link'
ROUTER_NODE_TYPE = 'io.skupper.router.router.node'
ROUTER_TYPE = 'io.skupper.router.router'
ROUTER_METRICS_TYPE = 'io.skupper.router.routerMetrics'
SSL_PROFILE_TYPE = 'io.skupper.router.sslProfile'
TCP_CONNECTOR_TYPE = 'io.skupper.router.tcpConnector'
TCP_LISTENER_TYPE = 'io.skupper.router.tcpListener'

# The directory where this module lives. Used to locate static configuration files etc.
DIR = os.path.dirname(__file__)


def ssl_file(name):
    return os.path.join(DIR, 'ssl_certs', name)


SERVER_CERTIFICATE = ssl_file('server-certificate.pem')
SERVER_PRIVATE_KEY = ssl_file('server-private-key.pem')
SERVER_PRIVATE_KEY_NO_PASS = ssl_file('server-private-key-no-pass.pem')
SERVER_PRIVATE_KEY_PASSWORD = 'server-password'
CLIENT_CERTIFICATE = ssl_file('client-certificate.pem')
CLIENT_PRIVATE_KEY = ssl_file('client-private-key.pem')
CLIENT_PRIVATE_KEY_NO_PASS = ssl_file('client-private-key-no-pass.pem')
BAD_CA_CERT = ssl_file('bad-ca-certificate.pem')
CLIENT_PRIVATE_KEY_PASSWORD = 'client-password'
CA_CERT = ssl_file('ca-certificate.pem')
CLIENT_PASSWORD_FILE = ssl_file('client-password-file.txt')
SERVER_PASSWORD_FILE = ssl_file('server-password-file.txt')
CHAINED_CERT = ssl_file('chained.pem')

# Alternate CA and certs
CA2_CERT = ssl_file('ca2-certificate.pem')
SERVER2_CERTIFICATE = ssl_file('server2-certificate.pem')
SERVER2_PRIVATE_KEY = ssl_file('server2-private-key.pem')
SERVER2_PRIVATE_KEY_NO_PASS = ssl_file('server2-private-key-no-pass.pem')
SERVER2_PRIVATE_KEY_PASSWORD = 'server2-password'
CLIENT2_CERTIFICATE = ssl_file('client2-certificate.pem')
CLIENT2_PRIVATE_KEY = ssl_file('client2-private-key.pem')
CLIENT2_PRIVATE_KEY_NO_PASS = ssl_file('client2-private-key-no-pass.pem')
CLIENT2_PRIVATE_KEY_PASSWORD = 'client2-password'


try:
    import qpidtoollibs  # pylint: disable=unused-import
except ImportError as err:
    qpidtoollibs = None         # pylint: disable=invalid-name
    MISSING_MODULES.append(str(err))

try:
    import qpid_messaging as qm  # pylint: disable=unused-import
except ImportError as err:
    qm = None  # pylint: disable=invalid-name
    MISSING_MODULES.append(str(err))


def find_exe(program):
    """Find an executable in the system PATH"""
    def is_exe(fpath):
        """True if fpath is executable"""
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
    mydir = os.path.split(program)[0]
    if mydir:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


def _check_requirements():
    """If requirements are missing, return a message, else return empty string."""
    missing = MISSING_MODULES
    required_exes = ['skrouterd']
    missing += ["No exectuable %s" % e for e in required_exes if not find_exe(e)]

    if missing:
        return "%s: %s" % (__name__, ", ".join(missing))


MISSING_REQUIREMENTS = _check_requirements()


def retry_delay(deadline, delay, max_delay):
    """For internal use in retry. Sleep as required
    and return the new delay or None if retry should time out"""
    remaining = deadline - time.time()
    if remaining <= 0:
        return None
    time.sleep(min(delay, remaining))
    return min(delay * 2, max_delay)


# Valgrind significantly slows down the response time of the router, so use a
# long default timeout
TIMEOUT = float(os.environ.get("QPID_SYSTEM_TEST_TIMEOUT", 60))


def retry(function: Callable[[], bool], timeout: float = TIMEOUT, delay: float = .001, max_delay: float = 1):
    """Call function until it returns a true value or timeout expires.
    Double the delay for each retry up to max_delay.
    Returns what function returns or None if timeout expires.
    """
    deadline = time.time() + timeout
    while True:
        ret = function()
        if ret:
            return ret
        else:
            delay = retry_delay(deadline, delay, max_delay)
            if delay is None:
                return None


_T = TypeVar('_T')
"""Type parameter used to declare generic types."""


def retry_exception(
        function: Callable[[], _T],
        timeout: float = TIMEOUT,
        delay: float = .001,
        max_delay: float = 1,
        exception: Union[Type[Exception], Set[Type[Exception]]] = None,
        exception_test: Callable[[Exception], Any] = None,
) -> _T:
    """Call function until it returns without exception or timeout expires.
    Double the delay for each retry up to max_delay.
    Catches and retries when expected exception was raised
    or calls exception_test with any exception raised by function, exception_test
    may itself raise an exception to terminate the retry.
    Parameters exception and exception_test may not be both provided at the same time.
    Returns what function returns if it succeeds before timeout.
    Raises last exception raised by function on timeout.
    """

    def default_exception_test(exception: Exception):
        """Do not permit retry on exceptions usually caused by programmer error. The list might be incomplete."""
        if isinstance(exception, (SyntaxError, UnboundLocalError, RecursionError, ImportError, TypeError)):
            raise exception

    if exception is not None and exception_test is not None:
        raise RuntimeError("Only one of `exception` and `exception_test` arguments may be specified")

    if exception is None and exception_test is None:
        exception_test = default_exception_test

    if exception is None:
        exception = Exception

    deadline = time.time() + timeout
    while True:
        try:
            return function()
        except exception as e:  # type: ignore[misc]  # Exception type must be derived from BaseException
            if exception_test:
                exception_test(e)
            delay = retry_delay(deadline, delay, max_delay)
            if delay is None:
                raise


def retry_assertion(function, timeout=TIMEOUT, delay=.001, max_delay=1):
    """Variant of retry_exception that only retries on AssertionError. Used in tests to
    retry while assertion is failing, but fail immediately if unexpected exception occurs."""
    return retry_exception(function=function, timeout=timeout, delay=delay, max_delay=max_delay,
                           exception=AssertionError)


def get_local_host_socket(socket_address_family='IPv4'):
    if socket_address_family == 'IPv4':
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = '127.0.0.1'
    elif socket_address_family == 'IPv6':
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        host = '::1'
    else:
        raise Exception(f"Invalid socket_address_family: {socket_address_family}")
    return s, host


def check_port_refuses_connection(port, socket_address_family='IPv4'):
    """Return true if connecting to host:port gives 'connection refused'."""
    s, host = get_local_host_socket(socket_address_family)
    try:
        s.connect((host, port))
    except OSError as e:
        return e.errno == errno.ECONNREFUSED
    finally:
        s.close()

    return False


def check_port_permits_binding(port, socket_address_family='IPv4'):
    """Return true if binding to the port succeeds."""
    s, _ = get_local_host_socket(socket_address_family)
    host = ""
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # so that followup binders are not blocked
        s.bind((host, port))
    except OSError:
        return False
    finally:
        s.close()

    return True


def is_port_available(port, socket_address_family='IPv4'):
    """Return true if a new server will be able to bind to the port."""
    return (check_port_refuses_connection(port, socket_address_family)
            and check_port_permits_binding(port, socket_address_family))


def wait_port(port, socket_address_family='IPv4', **retry_kwargs):
    """Wait up to timeout for port (on host) to be connectable.
    Takes same keyword arguments as retry to control the timeout"""
    def check(e):
        """Only retry on connection refused"""
        if isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:
            return  # try again
        raise e

    host = None

    def connect():
        # macOS gives EINVAL for all connection attempts after a ECONNREFUSED
        # man 3 connect: "If connect() fails, the state of the socket is unspecified. [...]"
        s, host = get_local_host_socket(socket_address_family)
        try:
            s.settimeout(retry_kwargs.get('timeout', TIMEOUT))
            s.connect((host, port))
            s.shutdown(socket.SHUT_RDWR)
        finally:
            s.close()

    retry_exception(connect, exception_test=check, **retry_kwargs)


def wait_ports(ports, **retry_kwargs):
    """Wait up to timeout for all ports (on host) to be connectable.
    Takes same keyword arguments as retry to control the timeout"""
    for port, socket_address_family in ports.items():
        wait_port(port=port, socket_address_family=socket_address_family, **retry_kwargs)


def message(**properties):
    """Convenience to create a proton.Message with properties set"""
    m = Message()
    for name, value in properties.items():
        getattr(m, name)        # Raise exception if not a valid message attribute.
        setattr(m, name, value)
    return m


def skip_test_in_ci(environment_var):
    env_var = os.environ.get(environment_var)
    if env_var is not None:
        if env_var.lower() in ['true', '1', 't', 'y', 'yes']:
            return True
    return False


def wait_message(pattern, file_path=None, **retry_kwargs):
    """
    Wait for a message with the passed in pattern to appear in the
    passed in log file.
    :param pattern: The pattern to look for in the file_path
    :param file_path: The full path of the file
    :param retry_kwargs:
    """
    assert retry(lambda: pathlib.Path(file_path).is_file(), **retry_kwargs), \
        f"Outfile {file_path} does not exist or is not a file"
    with open(file_path, 'rt') as out_file:
        assert retry(lambda: is_pattern_present(out_file, pattern), **retry_kwargs), \
            f"'{pattern}' not present in out file {file_path}"


def is_pattern_present(f: TextIO, pattern) -> bool:
    for line in f:
        m = re.search(pattern, line)
        if m:
            return True
    return False


class Process(subprocess.Popen):
    """
    Popen that can be torn down at the end of a TestCase and stores its output.
    """

    # Expected states of a Process at teardown
    RUNNING = -1                # Still running
    EXIT_OK = 0                 # Exit status 0
    EXIT_FAIL = 1               # Exit status 1

    unique_id = 0

    @classmethod
    def unique(cls, name):
        cls.unique_id += 1
        return "%s-%s" % (name, cls.unique_id)

    def __init__(self, args, name=None, expect=EXIT_OK, **kwargs):
        """
        Takes same arguments as subprocess.Popen. Some additional/special args:
        @param expect: Raise error if process status not as expected at end of test:
            L{RUNNING} - expect still running.
            L{EXIT_OK} - expect process to have terminated with 0 exit status.
            L{EXIT_FAIL} - expect process to have terminated with exit status >= 1.
            integer    - expected return code
        @keyword stdout: Defaults to the file name+".out"
        @keyword stderr: Defaults to be the same as stdout
        """
        self.name = name or os.path.basename(args[0])
        self.args = args
        assert expect is not None, "Process (name=%s) argument expect cannot be None" % self.name
        self.expect = expect
        self.outdir = os.getcwd()
        self.outfile = os.path.abspath(self.unique(self.name))
        self.torndown = False
        self._logger = Logger(title="Process logger for name=%s" % self.name,
                              print_to_console=True)
        self.abort_process = kwargs.pop("abort", False)
        kwargs.setdefault('stdin', subprocess.PIPE)
        with open(self.outfile_path, 'w') as out:
            kwargs.setdefault('stdout', out)
            kwargs.setdefault('stderr', subprocess.STDOUT)
            try:
                super(Process, self).__init__(args, **kwargs)
                with open(self.cmdfile_path, 'w') as f:
                    f.write("%s\npid=%s\n" % (' '.join(args), self.pid))
            except Exception as e:
                raise Exception("subprocess.Popen(%s, %s) failed: %s: %s" %
                                (args, kwargs, type(e).__name__, e))

    @property
    def outfile_path(self):
        """Path to the file containing the process output (stdout+stderr)"""
        assert self.outfile
        return self.outfile + ".out"

    @property
    def cmdfile_path(self):
        """Path to the file containing the process command line"""
        assert self.outfile
        return self.outfile + ".cmd"

    def assert_running(self):
        """Assert that the process is still running"""
        assert self.poll() is None, "%s: exited" % ' '.join(self.args)

    def teardown(self):
        """Check process status and stop the process if necessary"""
        if self.torndown:
            return
        self.torndown = True

        def error(msg):
            with open(self.outfile_path) as f:
                raise RuntimeError("Process %s (name=%s) error: %s\n%s\n%s\n>>>>\n%s<<<<" % (
                    self.pid, self.name, msg, ' '.join(self.args),
                    self.cmdfile_path, f.read()))

        state = self.poll()
        if state is None:
            # If status is None, it means that the process is still
            # running. This is cool if expected is Process.RUNNING - we'll
            # check that below
            if self.abort_process:
                self.send_signal(signal.SIGKILL)
                self.returncode = 0
            else:
                self.terminate()  # send SIGTERM
                try:
                    self.wait(TIMEOUT)
                except TimeoutExpired:
                    self.send_signal(signal.SIGABRT)  # so that we may get core dump
                    error("did not terminate properly, required SIGABORT")

        # at this point the process has terminated, either of its own accord or
        # due to the above terminate call.
        # prevent leak of stdin fd
        if self.stdin:
            self.stdin.close()

        if state is None and self.expect != Process.RUNNING:
            error("process was unexpectedly still running")

        # check the process return code
        if self.expect == Process.EXIT_OK:
            # returncode should be zero
            if self.returncode != 0:
                error("returned error code %s" % self.returncode)
        elif self.expect == Process.RUNNING:
            # returncode should be zero or SIGTERM since we just called
            # terminate() on a running process
            if self.returncode not in [0, -signal.SIGTERM]:
                error("returned error code %s" % self.returncode)
        elif self.expect == Process.EXIT_FAIL:
            # expect any nonzero error code
            if self.returncode == 0:
                error("expected error on exit but successful exit occurred")
        elif self.returncode != self.expect:
            # self.expected is the expected return code
            error("expected %s but actual returncode is %s"
                  % (self.expect, self.returncode))

    def wait_out_message(self, pattern, **retry_kwargs):
        """
        Convenience function that looks for the passed in pattern in the process's outfile.
        :param pattern: The pattern to look for in the process's out file.
        :param retry_kwargs: Retry arguments
        """
        wait_message(pattern, self.outfile_path, **retry_kwargs)


class Config:
    """Base class for configuration objects that provide a convenient
    way to create content for configuration files."""

    def write(self, name, suffix=".conf"):
        """Write the config object to file name.suffix. Returns name.suffix."""
        name = name + suffix
        with open(name, 'w') as f:
            f.write(str(self))
        return name


class Http1Server(Process):
    """
    Run the Python library http.server as a background process.
    Default content can be found in the http1-data subdirectory.
    """
    def __init__(self, port, name=None, expect=Process.RUNNING, **kwargs):
        name = name or "http.server"
        kwargs.setdefault('stdin', subprocess.DEVNULL)  # no input accepted
        kwargs.setdefault('directory', os.path.join(current_dir, "http1-data"))

        args = [sys.executable,
                "-m", "http.server",
                "-d", kwargs['directory'],
                "--cgi"]

        protocol_supported = sys.version_info >= (3, 11)
        if protocol_supported:
            kwargs.setdefault('protocol', "HTTP/1.1")
            args.append("-p")
            args.append(kwargs['protocol'])
        elif 'protocol' in kwargs:
            raise NotImplementedError("http.server does not support '--protocol' param")

        args.append(str(port))

        # remove keywords not used by super class
        kwargs.pop('directory')
        if 'protocol' in kwargs:
            kwargs.pop('protocol')
        super(Http1Server, self).__init__(args, name=name, expect=expect, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.teardown()


class Http2Server(Process):
    """A HTTP2 Server that will respond to requests made via the router."""

    def __init__(self, name=None, listen_port=None, wait=True,
                 cl_args=None,
                 server_file=None,
                 expect=Process.RUNNING,
                 env_config=None,
                 **kwargs):
        self.name = name
        self.listen_port = listen_port
        self.ports_family = {self.listen_port: 'IPv4'}
        self.cl_args = cl_args
        self.server_file = server_file
        self._wait_ready = False
        self.args = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), self.server_file)]
        if self.cl_args:
            self.args += self.cl_args
        server_env = dict(os.environ)
        if env_config:
            server_env.update(env_config)
        super(Http2Server, self).__init__(self.args, name=name, expect=expect,
                                          env=server_env, **kwargs)
        if wait:
            self.wait_ready()

    def wait_ready(self, **retry_kwargs):
        """
        Wait for ports to be ready
        """
        if not self._wait_ready:
            self._wait_ready = True
            self.wait_ports(**retry_kwargs)

    def wait_ports(self, **retry_kwargs):
        wait_ports(self.ports_family, **retry_kwargs)


class NginxServer(Process):
    """
    Sets up an ngix server
    """

    # Filesystem paths used by the server content and configuration
    #
    BASE_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'nginx')
    CONFIGS_FOLDER = os.path.join(BASE_FOLDER, 'nginx-configs')
    IMAGES_FOLDER = os.path.join(BASE_FOLDER, 'images')
    HTML_FOLDER = os.path.join(BASE_FOLDER, 'html')
    CONFIG_FILE = os.path.join(CONFIGS_FOLDER, 'nginx.conf')

    def __init__(self,
                 config_path: str,  # Full path of templated config file (string) /blah-blah/nginx/nginx-template.conf
                 env: Dict[str, str],  # A dict of env variables which will be substituted in the config_path file.
                 name: str = "nginx",
                 expect: int = Process.RUNNING,
                 **kwargs):
        input_file = os.path.join(config_path)
        config_file = os.path.join(env['setupclass-folder'], 'nginx.conf')

        # Run the expandvars.py script to replace the shell style variables in the config file with
        # environment variables.
        args = [sys.executable, os.path.join(expandvars_script_folder, script_file), input_file, config_file]
        subprocess.run(args, stderr=STDOUT, check=True, env=env)

        # Pass the config file for nginx to use.
        nginx_cmd = ['nginx', '-c', config_file]
        super().__init__(nginx_cmd, name=name, expect=expect, **kwargs)


class OpenSSLServer(Process):
    """
    Sets up an OpenSSL s_server.
    The router usually stands between an OpenSSL s_client and s_server
    """
    def __init__(self,
                 listening_port,
                 ssl_info,
                 name="OpenSSLServer",
                 cl_args=None,
                 expect=Process.RUNNING,
                 **kwargs):
        self.openssl_server_cmd = ['openssl', 's_server', '-accept', str(listening_port)]
        ca_cert = ssl_info.get('CA_CERT')
        if ca_cert:
            self.openssl_server_cmd.append('-CAfile')
            self.openssl_server_cmd.append(ca_cert)
        server_cert = ssl_info.get('SERVER_CERTIFICATE')
        if server_cert:
            self.openssl_server_cmd.append('-cert')
            self.openssl_server_cmd.append(server_cert)
        server_private_key = ssl_info.get('SERVER_PRIVATE_KEY')
        if server_private_key:
            self.openssl_server_cmd.append('-key')
            self.openssl_server_cmd.append(server_private_key)
        server_private_key_password = ssl_info.get('SERVER_PRIVATE_KEY_PASSWORD')
        if server_private_key_password:
            self.openssl_server_cmd.append('-pass')
            self.openssl_server_cmd.append("pass:" + server_private_key_password)
        if cl_args:
            self.openssl_server_cmd += cl_args
        super(OpenSSLServer, self).__init__(self.openssl_server_cmd, name=name,
                                            expect=expect, **kwargs)
        # avoid startup races: wait until the server prints "ACCEPT" to stdout
        # before returning in order to ensure the server is ready for
        # inbound connections
        wait_message(r'ACCEPT', file_path=self.outfile_path)


class Qdrouterd(Process):
    """Run a Qpid Dispatch Router Daemon"""

    class Config(list, Config):  # type: ignore[misc]  # Cannot resolve name "Config" (possible cyclic definition)  # mypy#10958
        """
        A router configuration.

        The Config class is a list of tuples in the following format:

        [ ('section-name', {attribute-map}), ...]

        where attribute-map is a dictionary of key+value pairs.  Key is an
        attribute name (string), value can be any of [scalar | string | dict]

        When written to a configuration file to be loaded by the router:
        o) there is no ":' between the section-name and the opening brace
        o) attribute keys are separated by a ":" from their values
        o) attribute values that are scalar or string follow the ":" on the
        same line.
        o) attribute values do not have trailing commas
        o) The section-name and attribute keywords are written
        without enclosing quotes
        o) string type attribute values are not enclosed in quotes
        o) attribute values of type dict are written in their JSON representation.

        Fills in some default values automatically, see Qdrouterd.DEFAULTS
        """

        DEFAULTS = {
            'listener': {'host': '0.0.0.0', 'saslMechanisms': 'ANONYMOUS', 'idleTimeoutSeconds': '240',
                         'authenticatePeer': 'no', 'role': 'normal'},
            'connector': {'host': '127.0.0.1', 'saslMechanisms': 'ANONYMOUS', 'idleTimeoutSeconds': '240'},
            'router': {'mode': 'standalone', 'id': 'QDR'}
        }

        def sections(self, name):
            """Return list of sections named name"""
            return [p for n, p in self if n == name]

        @property
        def router_id(self): return self.sections("router")[0]["id"]

        @property
        def router_mode(self): return self.sections("router")[0]["mode"]

        def defaults(self):
            """Fill in default values in gconfiguration"""
            for name, props in self:
                if name in Qdrouterd.Config.DEFAULTS:
                    for n, p in Qdrouterd.Config.DEFAULTS[name].items():
                        props.setdefault(n, p)

        def __str__(self):
            """Generate config file content. Calls default() first."""
            def tabs(level):
                if level:
                    return "    " * level
                return ""

            def value(item, level):
                if isinstance(item, dict):
                    result = "{\n"
                    result += "".join(["%s%s: %s,\n" % (tabs(level + 1),
                                                        json.dumps(k),
                                                        json.dumps(v))
                                       for k, v in item.items()])
                    result += "%s}" % tabs(level)
                    return result
                return "%s" %  item

            def attributes(e, level):
                assert isinstance(e, dict)
                # k = attribute name
                # v = string | scalar | dict
                return "".join(["%s%s: %s\n" % (tabs(level),
                                                k,
                                                value(v, level + 1))
                                for k, v in e.items()])

            self.defaults()
            # top level list of tuples ('section-name', dict)
            return "".join(["%s {\n%s}\n" % (n, attributes(p, 1)) for n, p in self])

    def __init__(self, name=None, config=Config(), pyinclude=None, wait=True,
                 cl_args=None, expect=Process.RUNNING):
        """
        @param name: name used for for output files, default to id from config.
        @param config: router configuration
        @keyword wait: wait for router to be ready (call self.wait_ready())
        """
        cl_args = cl_args or []
        self.config = copy(config)
        if not name:
            name = self.config.router_id
        assert name
        # setup log and debug dump files
        self.dumpfile = os.path.abspath('%s-qddebug.txt' % name)
        self.config.sections('router')[0]['debugDumpFile'] = self.dumpfile
        default_log = [l for l in config if (l[0] == 'log' and l[1]['module'] == 'DEFAULT')]
        if not default_log:
            self.logfile = "%s.log" % name
            config.append(
                ('log', {'module': 'DEFAULT', 'enable': 'debug+',
                         'includeSource': 'true', 'outputFile': self.logfile}))
        else:
            self.logfile = default_log[0][1].get('outputFile')
        args = ['skrouterd', '-c', config.write(name)] + cl_args
        env_home = os.environ.get('QPID_DISPATCH_HOME')
        if pyinclude:
            args += ['-I', pyinclude]
        elif env_home:
            args += ['-I', os.path.join(env_home, 'python')]

        # shlex.split parses -ex 'thread apply all' into two parts, not in 4 words as string split does
        args = shlex.split(os.environ.get('QPID_DISPATCH_RUNNER', '')) + args
        super(Qdrouterd, self).__init__(args, name=name, expect=expect)
        self._management = None
        self._sk_manager = None
        self._wait_ready = False
        if wait:
            self.wait_ready()

    def __enter__(self):
        """Allows using the class in the `with` contextmanager"""
        return self

    def __exit__(self, *_):
        self.teardown()

    @property
    def sk_manager(self):
        if not self._sk_manager:
            try:
                self._sk_manager = SkManager(address=self.addresses[0])
            except Exception as e:
                # As it stands now, not all router configs are expected to have a valid amqp listening
                # port on self.addresses[0]. So for now, it is ok to return None
                # In the future we might require all routers to have a valid amqp listening port available
                # on self.addresses[0] at which point, we should remove this except clause and
                # force a failure if there is no self.addresses[0]
                return None
        return self._sk_manager

    @property
    def management(self):
        """Return a management agent proxy for this router"""
        if not self._management:
            self._management = Node.connect(self.addresses[0], timeout=TIMEOUT)
        return self._management

    def teardown(self):
        if self._management:
            try:
                self._management.close()
            except:
                pass
            self._management = None

        teardown_exc = None

        try:
            if self.sk_manager:
                # Delete all adaptor connectors and listeners before shutting down the router
                long_types = [TCP_LISTENER_TYPE, TCP_CONNECTOR_TYPE]
                for long_type in long_types:
                    self.sk_manager.delete_all_entities(long_type)
                retry_assertion(self.sk_manager.delete_adaptor_connections)
        except (IndexError, TypeError, BadRequestStatus):
            # The router might not have any amqp listeners at all
            # These are known exceptions we can ignore.
            pass
        except Exception as e:
            # SkManager class wraps all exceptions in Exception.
            # BadRequestStatus happens when you are trying to delete a connection that is already deleted.
            # The delete of the connector might delete the connection and we might try to delete that connection
            # again and that is ok.
            exception_string = str(e)
            if "ConnectionException" in exception_string or \
                    "LinkDetached" in exception_string or \
                    "ConnectionClosed" in exception_string or \
                    "BadRequestStatus" in exception_string:
                pass
            else:
                raise e
        try:
            super(Qdrouterd, self).teardown()
        except Exception as exc:
            # re-raise _after_ dumping all the state we can
            teardown_exc = exc

        def check_output_file(filename, description):
            """check router's debug dump file for anything interesting (should be
            empty) and dump it to stderr for perusal by organic lifeforms"""
            try:
                if os.stat(filename).st_size > 0:
                    with open(filename) as f:
                        sys.stderr.write("\nRouter %s %s:\n>>>>\n" %
                                         (self.config.router_id, description))
                        sys.stderr.write(f.read())
                        sys.stderr.write("\n<<<<\n")
                        sys.stderr.flush()
            except OSError:
                # failed to open file.  This can happen when an individual test
                # spawns a temporary router (i.e. not created as part of the
                # TestCase setUpClass method) that gets cleaned up by the test.
                pass

        check_output_file(filename=self.outfile_path, description="output file")
        check_output_file(filename=self.dumpfile, description="debug dump file")

        if teardown_exc:
            # teardown failed - possible router crash?
            # dump extra stuff (command line, output, log)

            def tail_file(fname, line_count=50):
                """Tail a file to a list"""
                out = []
                with open(fname) as f:
                    line = f.readline()
                    while line:
                        out.append(line)
                        if len(out) > line_count:
                            out.pop(0)
                        line = f.readline()
                return out

            try:
                for fname in [("output", self.outfile_path),
                              ("command", self.cmdfile_path)]:
                    with open(fname[1]) as f:
                        sys.stderr.write("\nRouter %s %s file:\n>>>>\n" %
                                         (self.config.router_id, fname[0]))
                        sys.stderr.write(f.read())
                        sys.stderr.write("\n<<<<\n")

                if self.logfile:
                    sys.stderr.write("\nRouter %s log file tail:\n>>>>\n" %
                                     self.config.router_id)
                    tail = tail_file(self.logfile_path)
                    for ln in tail:
                        sys.stderr.write("%s" % ln)
                    sys.stderr.write("\n<<<<\n")
                sys.stderr.flush()
            except OSError:
                # ignore file not found in case test never opens these
                pass

            raise teardown_exc

    @property
    def ports_family(self):
        """
        Return a dict of listener ports and the respective port family
        Example -
        { 23456: 'IPv4', 243455: 'IPv6' }
        """
        ports_fam = {}
        for l in self.config.sections('listener'):
            if l.get('socketAddressFamily'):
                ports_fam[l['port']] = l['socketAddressFamily']
            else:
                ports_fam[l['port']] = 'IPv4'

        return ports_fam

    @property
    def ports(self):
        """Return list of configured ports for all listeners"""
        return [l['port'] for l in self.config.sections('listener')]

    def _cfg_2_host_port(self, c):
        host = c['host']
        port = c['port']
        socket_address_family = c.get('socketAddressFamily', 'IPv4')
        if socket_address_family == 'IPv6':
            return "[%s]:%s" % (host, port)
        elif socket_address_family == 'IPv4':
            return "%s:%s" % (host, port)
        raise Exception("Unknown socket address family: %s" % socket_address_family)

    def _get_all_addresses(self, cfg):
        ret_val = []
        for listener in cfg:
            require_tls = listener.get("sslProfile")
            if require_tls:
                ret_val.append("https://%s" % self._cfg_2_host_port(listener))
            else:
                ret_val.append("http://%s" % self._cfg_2_host_port(listener))
        return ret_val

    @property
    def tcp_addresses(self):
        """Return http(s)://host:port addresses for all tcp listeners"""
        cfg = self.config.sections('tcpListener')
        return self._get_all_addresses(cfg)

    @property
    def addresses(self):
        """Return amqp://host:port addresses for all listeners"""
        cfg = self.config.sections('listener')
        return ["amqp://%s" % self._cfg_2_host_port(l) for l in cfg]

    @property
    def connector_addresses(self):
        """Return list of amqp://host:port for all connectors"""
        cfg = self.config.sections('connector')
        return ["amqp://%s" % self._cfg_2_host_port(c) for c in cfg]

    @property
    def hostports(self):
        """Return host:port for all listeners"""
        return [self._cfg_2_host_port(l) for l in self.config.sections('listener')]

    def is_connected(self, port, host='127.0.0.1'):
        """If router has a connection to host:port:identity return the management info.
        Otherwise return None"""
        try:
            ret_val = False
            response = self.management.query(type=CONNECTION_TYPE)
            index_host = response.attribute_names.index('host')
            for result in response.results:
                outs = '%s:%s' % (host, port)
                if result[index_host] == outs:
                    ret_val = True
                    break
            return ret_val
        except:
            return False

    def wait_address(self, address, subscribers=0, remotes=0, count=1, **retry_kwargs):
        """
        Wait for an address to be visible on the router.
        @keyword subscribers: Wait till subscriberCount >= subscribers
        @keyword remotes: Wait till remoteCount >= remotes
        @keyword count: Wait until >= count matching addresses are found
        @param retry_kwargs: keyword args for L{retry}
        """
        def check():
            # TODO aconway 2014-06-12: this should be a request by name, not a query.
            # Need to rationalize addresses in management attributes.
            # endswith check is because of M/L/R prefixes
            addrs = self.management.query(
                type=ROUTER_ADDRESS_TYPE,
                attribute_names=['name', 'subscriberCount', 'remoteCount']).get_entities()

            addrs = [a for a in addrs if a['name'].endswith(address)]

            return (len(addrs) >= count
                    and addrs[0]['subscriberCount'] >= subscribers
                    and addrs[0]['remoteCount'] >= remotes)
        assert retry(check, **retry_kwargs)

    def wait_address_unsubscribed(self, address, **retry_kwargs):
        """
        Block until address has no subscribers
        """
        a_type = ROUTER_ADDRESS_TYPE

        def check():
            addrs = self.management.query(a_type).get_dicts()
            rc = [a for a in addrs if a['name'].endswith(address)]
            count = 0
            for a in rc:
                count += a['subscriberCount']
                count += a['remoteCount']

            return count == 0
        assert retry(check, **retry_kwargs)

    def get_host(self, socket_address_family):
        if socket_address_family == 'IPv4':
            return '127.0.0.1'
        elif socket_address_family == 'IPv6':
            return '::1'
        else:
            return '127.0.0.1'

    def wait_ports(self, **retry_kwargs):
        wait_ports(self.ports_family, **retry_kwargs)

    def wait_connectors(self, **retry_kwargs):
        """
        Wait for all connectors to be connected
        @param retry_kwargs: keyword args for L{retry}
        """
        for c in self.config.sections('connector'):
            assert retry(lambda c=c: self.is_connected(port=c['port'], host=c.get('host') if c.get('host') else self.get_host(c.get('socketAddressFamily'))),
                         **retry_kwargs), "Port not connected %s" % c['port']

    def wait_log_message(self, pattern, logfile_path=None, **retry_kwargs):
        """Wait for a log message matching the pattern to appear in the routers
        log file. The log file for the DEFAULT log module is used unless
        overridden via the (fully qualified) logfile_path parameter
        """
        logfile_path = logfile_path or self.logfile_path
        assert logfile_path
        wait_message(pattern, logfile_path, **retry_kwargs)

    def wait_startup_message(self, **retry_kwargs):
        """Wait for router startup message to be printed into logfile

        This ensures that the router installs its signal handlers, avoiding
        a router failure with return code -15 upon premature SIGTERM (DISPATCH-1689)

        e.g. 2022-03-03 19:08:13.608655 +0100 SERVER (info) Operational, 4 Threads Running (process ID 2190110)
        """
        # system_tests_log_level_update filters SERVER module logs to a
        # separate file
        logfile_path = self.logfile_path
        for log in self.config.sections('log'):
            if log['module'] == 'SERVER':
                logfile_path = os.path.join(self.outdir, log.get('outputFile'))
                break

        self.wait_log_message(r'SERVER \(info\) Operational, (\d+) Threads Running \(process ID (\d+)\)',
                              logfile_path=logfile_path)

    def wait_ready(self, **retry_kwargs):
        """Wait for ports and connectors to be ready"""
        if not self._wait_ready:
            self._wait_ready = True
            self.wait_ports(**retry_kwargs)
            self.wait_connectors(**retry_kwargs)
            self.wait_startup_message(**retry_kwargs)
        return self

    def is_router_connected(self, router_id, **retry_kwargs):
        try:
            self.management.read(identity="router.node/%s" % router_id)
        except Exception:
            # router_id is not yet seen
            return False

        # TODO aconway 2015-01-29: The above check should be enough, we
        # should not advertise a remote router in management till it is fully
        # connected. However we still get a race where the router is not
        # actually ready for traffic. Investigate.
        # Meantime the following actually tests send-thru to the router.
        try:
            with Node.connect(self.addresses[0], router_id) as node:
                return node.query(ROUTER_TYPE)
        except (proton.ConnectionException, proton.Timeout,
                NotFoundStatus, proton.utils.LinkDetached,
                proton.utils.SendException) as exc:
            # router_id not completely done initializing
            return False

    def wait_router_connected(self, router_id, **retry_kwargs):
        retry(lambda: self.is_router_connected(router_id), **retry_kwargs)

    def is_edge_routers_connected(self, num_edges=1, role='edge', num_meshes=None, **retry_kwargs):
        """
        Checks the number of edge uplink connections equals the passed in num_edges
        :param num_edges:
        :param is_tls:
        :return:
        """
        def is_edges_connected(edges=num_edges, meshes=num_meshes):
            node = None
            try:
                node = Node.connect(self.addresses[0], timeout=1)
                out = retry_exception(lambda: node.query(CONNECTION_TYPE), delay=1)
                if out:
                    role_index = out.attribute_names.index("role")
                    dir_index = out.attribute_names.index("dir")
                    meshid_index = out.attribute_names.index("meshId")
                    edges_num = 0
                    mesh_list = []
                    for conn in out.results:
                        if role == conn[role_index] and conn[dir_index] == "in":
                            edges_num += 1
                            if meshes is not None:
                                meshid = conn[meshid_index]
                                if meshid not in mesh_list:
                                    mesh_list.append(meshid)
                    if edges_num == edges and (meshes is None or meshes == len(mesh_list)):
                        return True
                return False
            except (proton.ConnectionException, NotFoundStatus, proton.utils.LinkDetached):
                return False
            finally:
                if node:
                    node.close()

        retry(lambda: is_edges_connected(num_edges, num_meshes), **retry_kwargs)

    def wait_http_server_connected(self, is_tls=False, **retry_kwargs):
        """
        Looks for an outbound http connection in the list of connection objects.
        Checks if the server connection is a TLS connection if is_tls=True
        """
        def is_server_connected(check_tls=is_tls):
            node = None
            try:
                node = Node.connect(self.addresses[0], timeout=1)
                out = retry_exception(lambda: node.query(CONNECTION_TYPE))
                if out:
                    role_index = out.attribute_names.index("role")
                    dir_index = out.attribute_names.index("dir")
                    protocol_index = out.attribute_names.index("protocol")

                    for conn in out.results:
                        if "inter-router" != conn[role_index] and \
                                conn[dir_index] == "out" and \
                                "http" in conn[protocol_index]:
                            if check_tls:
                                ssl_proto_index = out.attribute_names.index("sslProto")
                                if conn[ssl_proto_index] and "TLS" in conn[ssl_proto_index]:
                                    return True
                                return False
                            return True
                return False
            except (proton.ConnectionException, NotFoundStatus, proton.utils.LinkDetached):
                # proton.ConnectionException: the router is not yet accepting connections
                # NotFoundStatus: the queried router is not yet connected
                return False
            finally:
                if node:
                    node.close()

        retry(lambda: is_server_connected(is_tls), **retry_kwargs)

    @property
    def logfile_path(self):
        """Path to a DEFAULT logfile"""
        return os.path.join(self.outdir, self.logfile)

    def get_inter_router_conns(self):
        """
        Query the router for a list of all connections of role
        inter-router/inter-router-data
        """
        conns = self.management.query(type=CONNECTION_TYPE).get_dicts()
        return [c for c in conns if 'inter-router' in c['role']]

    def get_edge_router_conns(self):
        """
        Query the router for a list of all connections of role
        edge/inter-edge
        """
        conns = self.management.query(type=CONNECTION_TYPE).get_dicts()
        return [c for c in conns if 'edge' in c['role']]

    def get_inter_router_data_conns(self):
        """
        Return a list of all inter-router-data connections present
        """
        dconns = self.get_inter_router_conns()
        return [c for c in dconns if c['role'] == 'inter-router-data']

    def get_inter_router_control_conns(self):
        """
        Return a list of all inter-router control connections present
        """
        dconns = self.get_inter_router_conns()
        return [c for c in dconns if c['role'] == 'inter-router']

    def get_links_by_conn_id(self, connection_id):
        """
        Return a list of all active AMQP links for the given connection
        """
        links = self.management.query(type=ROUTER_LINK_TYPE).get_dicts()
        return [link for link in links if link['connectionId'] == connection_id]

    def get_active_inter_router_data_links(self):
        # Get the list of active inter-router-data links for the router. These
        # are the dynamically provisioned links for passing streaming data
        # between routers. For example there will be at least one streaming
        # data link for every TCP session passing through the router.
        ir_conns = self.get_inter_router_data_conns()
        links = []
        for conn in ir_conns:
            links.extend([link for link in self.get_links_by_conn_id(conn['identity'])
                          if link['linkType'] == 'endpoint' and
                          link['operStatus'] == 'up'])
        return links

    def get_active_inter_router_control_links(self):
        # Get the list of active inter-router control links for the
        # router. These are the links that carry the inter-router Hello message
        # traffic as well as priority-based non-streaming messages.
        # There will be two for each inter-router path (1 incoming link, 1
        # outgoing link)
        ir_conns = self.get_inter_router_control_conns()
        links = []
        for conn in ir_conns:
            links.extend([link for link in self.get_links_by_conn_id(conn['identity'])
                          if link['linkType'] == 'router-control' and
                          link['operStatus'] == 'up'])
        return links

    def get_last_topology_change(self):
        """
        Get the timestamp when this router last re-computed topology. Returns
        None if the router is not interior.
        """
        try:
            node_state = self.management.read(type=ROUTER_NODE_TYPE,
                                              identity=f"router.node/{self.config.router_id}")
            return node_state["lastTopoChange"]
        except:
            return None


class NcatException(Exception):
    def __init__(self, error=None):
        super(NcatException, self).__init__(error)


class Tester:
    """Tools for use by TestCase
- Create a directory for the test.
- Utilities to create processes and servers, manage ports etc.
- Clean up processes on teardown"""

    # Top level directory above any Tester directories.
    # CMake-generated configuration may be found here.
    top_dir = os.getcwd()

    # The root directory for Tester directories, under top_dir
    root_dir = os.path.abspath(__name__ + '.dir')

    # Minimum and maximum port number for free port searches
    port_range = (20000, 30000)

    def __init__(self, id):
        """
        @param id: module.class.method or False if no directory should be created
        """
        self.directory = os.path.join(self.root_dir, *id.split('.')) if id else None
        self.cleanup_list = []
        self.port_file = pathlib.Path(self.top_dir, "next_port.lock").open("a+t")
        self.cleanup(self.port_file)

    def rmtree(self):
        """Remove old test class results directory"""
        if self.directory:
            shutil.rmtree(os.path.dirname(self.directory), ignore_errors=True)

    def setup(self):
        """Called from test setup and class setup."""
        if self.directory:
            os.makedirs(self.directory, exist_ok=True)
            os.chdir(self.directory)

    def _next_port(self) -> int:
        """Reads and increments value stored in self.port_file, under an exclusive file lock.

        When a lock cannot be acquired immediately, fcntl.flock blocks.

        Failure possibilities:
            File locks may not work correctly on network filesystems. We still should be no worse off than we were.

            This method always unlocks the lock file, so it should not ever deadlock other tests running in parallel.
            Even if that happened, the lock is unlocked by the OS when the file is closed, which happens automatically
            when the process that opened and locked it ends.

            Invalid content in the self.port_file will break this method. Manual intervention is then required.
        """
        try:
            fcntl.flock(self.port_file, fcntl.LOCK_EX)

            # read old value
            self.port_file.seek(0, os.SEEK_END)
            if self.port_file.tell() != 0:
                self.port_file.seek(0)
                port = int(self.port_file.read())
            else:
                # file is empty
                port = random.randint(self.port_range[0], self.port_range[1])

            next_port = port + 1
            if next_port >= self.port_range[1]:
                next_port = self.port_range[0]

            # write new value
            self.port_file.seek(0)
            self.port_file.truncate(0)
            self.port_file.write(str(next_port))
            self.port_file.flush()

            return port
        finally:
            fcntl.flock(self.port_file, fcntl.LOCK_UN)

    def teardown(self):
        """Clean up (tear-down, stop or close) objects recorded via cleanup()"""
        self.cleanup_list.reverse()
        errors = []
        for obj in self.cleanup_list:
            try:
                for method in ["teardown", "tearDown", "stop", "close"]:
                    cleanup = getattr(obj, method, None)
                    if cleanup:
                        cleanup()
                        break
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("Errors during teardown: \n\n%s" % "\n\n".join([str(e) for e in errors])) from errors[0]

    def cleanup(self, x):
        """Record object x for clean-up during tear-down.
        x should have on of the methods teardown, tearDown, stop or close"""
        self.cleanup_list.append(x)
        return x

    def popen(self, *args, **kwargs) -> Process:
        """Start a Process that will be cleaned up on teardown"""
        return self.cleanup(Process(*args, **kwargs))

    def qdrouterd(self, *args, **kwargs) -> Qdrouterd:
        """Return a Qdrouterd that will be cleaned up on teardown"""
        return self.cleanup(Qdrouterd(*args, **kwargs))

    def http2server(self, *args, **kwargs):
        return self.cleanup(Http2Server(*args, **kwargs))

    def nginxserver(self, *args, **kwargs):
        return self.cleanup(NginxServer(*args, **kwargs))

    def openssl_server(self, *args, **kwargs):
        return self.cleanup(OpenSSLServer(*args, **kwargs))

    def opensslclient(self,
                      port,
                      ssl_info,
                      data=None,
                      name="opensslsclient",
                      cl_args=None,
                      expect=Process.RUNNING,
                      timeout=TIMEOUT):
        s_client = ['openssl', 's_client', '-connect', 'localhost:' + str(port), '-servername', 'localhost']
        if ssl_info:
            ca_cert = ssl_info.get('CA_CERT')
            if ca_cert:
                s_client.append('-CAfile')
                s_client.append(ca_cert)
            client_cert = ssl_info.get('CLIENT_CERTIFICATE')
            if client_cert:
                s_client.append('-cert')
                s_client.append(client_cert)
            client_cert_key = ssl_info.get('CLIENT_PRIVATE_KEY')
            if client_cert_key:
                s_client.append('-key')
                s_client.append(client_cert_key)
            client_password = ssl_info.get('CLIENT_PRIVATE_KEY_PASSWORD')
            if client_password:
                s_client.append('-pass')
                s_client.append("pass:" + client_password)
        if cl_args:
            s_client += cl_args
        p = self.popen(s_client,
                       stdin=PIPE,
                       stdout=PIPE,
                       stderr=PIPE,
                       expect=expect,
                       name=name)
        # We are using self signed certificates here.
        # Generally the error returned if self signed certificates are used is the following -
        # error= b"Can't use SSL_get_servername\ndepth=1 CN = Trusted.CA.com, O = Trust Me Inc.\nverify return:1\ndepth=0 CN = localhost, O = Server\nverify return:1\nDONE\n"
        out, error = p.communicate(input=data, timeout=timeout)
        if expect == Process.EXIT_OK:
            assert p.returncode == 0, \
                f"openssl s_client failed with returncode {p.returncode} {error}"
        return out, error

    def ncat(self,
             port,
             logger,
             name="ncat",
             expect=Process.EXIT_OK,
             timeout=TIMEOUT,
             data=b'abcd',
             ssl_info=None):
        ncat_cmd = ['ncat', 'localhost', str(port)]
        if ssl_info:
            ca_cert = ssl_info.get('CA_CERT')
            if ca_cert:
                ncat_cmd.append('--ssl-trustfile')
                ncat_cmd.append(ca_cert)
            client_cert = ssl_info.get('CLIENT_CERTIFICATE')
            if client_cert:
                ncat_cmd.append('--ssl-cert')
                ncat_cmd.append(client_cert)
            client_cert_key = ssl_info.get('CLIENT_PRIVATE_KEY')
            if client_cert_key:
                ncat_cmd.append('--ssl-key')
                ncat_cmd.append(client_cert_key)
        logger.log(f"Starting ncat {ncat_cmd} and input len={len(data)}, name={name}")
        p = self.popen(ncat_cmd,
                       stdin=PIPE,
                       stdout=PIPE,
                       stderr=PIPE,
                       expect=expect,
                       name=name)
        out, err = p.communicate(input=data, timeout=timeout)
        try:
            p.teardown()
        except Exception as e:
            if err and b'Ncat: Input/output error' in err:
                # For now, we are ignoring the case where
                # ncat produces a specific Input/output error.
                self.logger.log(f"_ncat_runner err={err}")
            else:
                raise NcatException("ncat failed:"
                                    " stdout='%s' stderr='%s' returncode=%d" % (out, err, p.returncode)
                                    if out or err else str(e))
        return out, err

    def get_port(self, socket_address_family: str = 'IPv4') -> int:
        """Get an unused port"""
        p = self._next_port()
        start = p
        while not is_port_available(p, socket_address_family):
            p = self._next_port()
            if p == start:
                raise Exception(f"No available ports in range {self.port_range}")
        return p


class TestCase(unittest.TestCase, Tester):  # pylint: disable=too-many-public-methods
    """A TestCase that sets up its own working directory and is also a Tester."""

    tester: Tester

    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName)
        Tester.__init__(self, self.id())

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.maxDiff = None
        cls.tester = Tester('.'.join([cls.__module__, cls.__name__, 'setUpClass']))
        cls.tester.rmtree()
        cls.tester.setup()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'tester'):
            cls.tester.teardown()
            del cls.tester
        super().tearDownClass()

    def setUp(self):
        super().setUp()
        Tester.setup(self)

    def tearDown(self):
        Tester.teardown(self)
        super().tearDown()

    def shortDescription(self) -> Optional[str]:
        """Returns None to disable showing first line of docstring in unittest logs.

        https://stackoverflow.com/questions/12962772/how-to-stop-python-unittest-from-printing-test-docstring"""
        return None

    def assert_fair(self, seq):
        avg = sum(seq) / len(seq)
        for i in seq:
            assert i > avg / 2, "Work not fairly distributed: %s" % seq

    if not hasattr(unittest.TestCase, 'assertRegex'):
        def assertRegex(self, text, expected_regex, msg=None):
            assert re.search(expected_regex, text), msg or "Can't find %r in '%s'" % (expected_regex, text)

    if not hasattr(unittest.TestCase, 'assertNotRegex'):
        def assertNotRegex(self, text, unexpected_regex, msg=None):
            assert not re.search(unexpected_regex, text), msg or "Found %r in '%s'" % (unexpected_regex, text)


def main_module():
    """
    Return the module name of the __main__ module - i.e. the filename with the
    path and .py extension stripped. Useful to run the tests in the current file but
    using the proper module prefix instead of '__main__', as follows:
        if __name__ == '__main__':
            unittest.main(module=main_module())
    """
    return os.path.splitext(os.path.basename(__main__.__file__))[0]


class AsyncTestReceiver(MessagingHandler):
    """
    A simple receiver that runs in the background and queues any received
    messages.  Messages can be retrieved from this thread via the queue member.
    :param wait: block the constructor until the link has been fully
                 established.
    :param recover_link: restart on remote link detach
    """
    Empty = Queue.Empty

    class TestReceiverException(Exception):
        def __init__(self, error=None):
            super(AsyncTestReceiver.TestReceiverException, self).__init__(error)

    class MyQueue(Queue.Queue):
        def __init__(self, receiver):
            self._async_receiver = receiver
            super(AsyncTestReceiver.MyQueue, self).__init__()

        def get(self, timeout=TIMEOUT):
            self._async_receiver.num_queue_gets += 1
            msg = super(AsyncTestReceiver.MyQueue, self).get(timeout=timeout)
            self._async_receiver._logger.log("message %d get"
                                             % self._async_receiver.num_queue_gets)
            return msg

        def put(self, msg):
            self._async_receiver.num_queue_puts += 1
            super(AsyncTestReceiver.MyQueue, self).put(msg)
            self._async_receiver._logger.log("message %d put"
                                             % self._async_receiver.num_queue_puts)

    def __init__(self, address, source, conn_args=None, container_id=None,
                 wait=True, recover_link=False, msg_args=None, print_to_console=False):
        if msg_args is None:
            msg_args = {}
        super(AsyncTestReceiver, self).__init__(**msg_args)
        self.address = address
        self.source = source
        self.conn_args = conn_args
        self.queue = AsyncTestReceiver.MyQueue(self)
        self._conn = None
        self._container = Container(self)
        cid = container_id or "ATR-%s:%s" % (source, uuid.uuid4())
        self._container.container_id = cid
        self._ready = Event()
        self._recover_link = recover_link
        self._recover_count = 0
        self._stop_thread = False
        self._thread = Thread(target=self._main)
        self._logger = Logger(title="AsyncTestReceiver %s" % cid, print_to_console=print_to_console)
        self._thread.daemon = True
        self._thread.start()
        self.num_queue_puts = 0
        self.num_queue_gets = 0
        self.queue_stats = "self.num_queue_puts=%d, self.num_queue_gets=%d"
        self._error = None
        if wait:
            ready = self._ready.wait(timeout=TIMEOUT)
            if ready is False:
                self._logger.log("AsyncTestReceiver connection attempt timed out")
                raise AsyncTestReceiver.TestReceiverException("Timed out waiting for receiver start")
            elif self._error is not None:
                self._logger.log("AsyncTestReceiver connection attempt failed, error=%s" % self._error)
                raise AsyncTestReceiver.TestReceiverException(self._error)
        self._logger.log("AsyncTestReceiver thread running")

    def get_queue_stats(self):
        return self.queue_stats % (self.num_queue_puts, self.num_queue_gets)

    def _main(self):
        self._container.timeout = 0.5
        self._container.start()
        self._logger.log("AsyncTestReceiver Starting reactor")
        while self._container.process():
            if self._stop_thread:
                if self._conn:
                    self._conn.close()
                    self._conn = None
        self._ready.set()
        self._logger.log("AsyncTestReceiver reactor thread done")

    def on_transport_error(self, event):
        self._logger.log("AsyncTestReceiver on_transport_error=%s" % event.transport.condition.description)
        self._error = f"Transport Error: {event.transport.condition.description}"
        self._stop_thread = True

    def on_connection_error(self, event):
        self._logger.log("AsyncTestReceiver on_connection_error=%s" % event.connection.remote_condition.description)
        self._error = f"Connection Error: {event.connection.remote_condition.description}"
        self._stop_thread = True

    def on_link_error(self, event):
        self._logger.log("AsyncTestReceiver on_link_error=%s" % event.link.remote_condition.description)
        self._error = f"Link Error: {event.link.remote_condition.description}"
        self._stop_thread = True

    def stop(self, timeout=TIMEOUT):
        self._logger.log("AsyncTestReceiver stopping")
        self._stop_thread = True
        self._container.wakeup()
        self._thread.join(timeout=TIMEOUT)
        if self._thread.is_alive():
            self._logger.log("AsyncTestReceiver did not stop!")
            raise AsyncTestReceiver.TestReceiverException("AsyncTestReceiver did not exit")
        del self._conn
        del self._container
        if self._error is not None:
            self._logger.log("AsyncTestReceiver failed with error=%s" % self._error)
            raise AsyncTestReceiver.TestReceiverException(self._error)
        self._logger.log("AsyncTestReceiver stopped!")

    def on_start(self, event):
        kwargs = {'url': self.address}
        if self.conn_args:
            kwargs.update(self.conn_args)
        self._conn = event.container.connect(**kwargs)

    def on_connection_opened(self, event):
        self._logger.log("Connection opened")
        kwargs = {'source': self.source}
        event.container.create_receiver(event.connection, **kwargs)

    def on_link_opened(self, event):
        self._logger.log("link opened")
        self._ready.set()

    def on_link_closing(self, event):
        self._logger.log("link closing")
        event.link.close()
        if self._recover_link and not self._stop_thread:
            # lesson learned: the generated link name will be the same as the
            # old link (which is bad) so we specify a new one
            self._recover_count += 1
            kwargs = {'source': self.source,
                      'name': "%s:%s" % (event.link.name, self._recover_count)}
            rcv = event.container.create_receiver(event.connection,
                                                  **kwargs)

    def on_message(self, event):
        self.queue.put(event.message)

    def on_disconnected(self, event):
        # if remote terminates the connection kill the thread else it will spin
        # on the cpu
        self._logger.log("Disconnected")
        if self._conn:
            self._conn.close()
            self._conn = None

    def dump_log(self):
        self._logger.dump()


class AsyncTestSender(MessagingHandler):
    """
    A simple sender that runs in the background and sends 'count' messages to a
    given target.
    """
    class TestSenderException(Exception):
        def __init__(self, error=None):
            super(AsyncTestSender.TestSenderException, self).__init__(error)

    def __init__(self, address, target, count=1, message=None,
                 container_id=None, presettle=False, print_to_console=False,
                 conn_args=None, get_link_info=True):
        super(AsyncTestSender, self).__init__(auto_accept=False,
                                              auto_settle=False)
        self.address = address
        self.target = target
        self.total = count
        self.presettle = presettle
        self.accepted = 0
        self.released = 0
        self.modified = 0
        self.rejected = 0
        self.sent = 0
        self.error = None
        self.link_stats = None
        self._conn = None
        self.conn_args = conn_args
        self._get_link_info = get_link_info
        self._sender = None
        self._message = message or Message(body="test")
        self._container = Container(self)
        cid = container_id or "ATS-%s:%s" % (target, uuid.uuid4())
        self._container.container_id = cid
        self._link_name = "%s-%s" % (cid, "tx")
        self._thread = Thread(target=self._main)
        self._thread.daemon = True
        self._logger = Logger(title="AsyncTestSender %s" % cid, print_to_console=print_to_console)
        self._thread.start()
        self.msg_stats = "self.sent=%d, self.accepted=%d, self.released=%d, self.modified=%d, self.rejected=%d"

    def _main(self):
        self._container.timeout = 0.5
        self._container.start()
        self._logger.log("AsyncTestSender Starting reactor")
        while self._container.process():
            self._check_if_done()
        self._logger.log("AsyncTestSender reactor thread done")

    def get_msg_stats(self):
        return self.msg_stats % (self.sent, self.accepted, self.released, self.modified, self.rejected)

    def wait(self):
        # don't stop it - wait until everything is sent
        self._logger.log("AsyncTestSender wait: about to join thread")
        self._thread.join(timeout=TIMEOUT)
        self._logger.log("AsyncTestSender wait: thread done")
        assert not self._thread.is_alive(), "sender did not complete"
        if self.error is not None:
            raise AsyncTestSender.TestSenderException(self.error)
        del self._sender
        del self._conn
        del self._container
        self._logger.log("AsyncTestSender wait: no errors in wait")

    def on_start(self, event):
        kwargs = {'url': self.address}
        if self.conn_args is not None:
            kwargs.update(self.conn_args)
        self._conn = self._container.connect(**kwargs)

    def on_transport_error(self, event):
        self._logger.log("AsyncTestSender on_transport_error=%s" % event.transport.condition.description)
        self.error = f"Connection Error: {event.transport.condition.description}"

    def on_connection_error(self, event):
        self._logger.log("AsyncTestSender on_connection_error=%s" % event.connection.remote_condition.description)
        self.error = f"Connection Error: {event.connection.remote_condition.description}"

    def on_link_error(self, event):
        self._logger.log("AsyncTestSender on_link_error=%s" % event.link.remote_condition.description)
        self.error = f"Link Error: {event.link.remote_condition.description}"

    def on_connection_opened(self, event):
        self._logger.log("Connection opened")
        option = AtMostOnce if self.presettle else AtLeastOnce
        self._sender = self._container.create_sender(self._conn,
                                                     target=self.target,
                                                     options=option(),
                                                     name=self._link_name)

    def on_sendable(self, event):
        if self.sent < self.total:
            self._sender.send(self._message)
            self.sent += 1
            self._logger.log("message %d sent" % self.sent)

    def _check_if_done(self):
        done = (self.sent == self.total
                and (self.presettle
                     or (self.accepted + self.released + self.modified
                         + self.rejected == self.sent)))
        done = done or self.error is not None
        if done and self._conn:
            if self._get_link_info:
                self.link_stats = get_link_info(self._link_name,
                                                self.address)
            self._conn.close()
            self._conn = None
            self._logger.log("Connection closed")

    def on_accepted(self, event):
        self.accepted += 1
        event.delivery.settle()
        self._logger.log("message %d accepted" % self.accepted)

    def on_released(self, event):
        # for some reason Proton 'helpfully' calls on_released even though the
        # delivery state is actually MODIFIED
        if event.delivery.remote_state == Delivery.MODIFIED:
            return self.on_modified(event)
        self.released += 1
        event.delivery.settle()
        self._logger.log("message %d released" % self.released)

    def on_modified(self, event):
        self.modified += 1
        event.delivery.settle()
        self._logger.log("message %d modified" % self.modified)

    def on_rejected(self, event):
        self.rejected += 1
        event.delivery.settle()
        self._logger.log("message %d rejected" % self.rejected)

    def on_disconnected(self, event):
        # if remote terminates the connection kill the thread else it will spin
        # on the cpu
        self.error = "connection to remote dropped"
        self._logger.log(self.error)
        if self._conn:
            self._conn.close()
            self._conn = None

    def dump_log(self):
        self._logger.dump()


class SkManager:
    """
    A means to invoke skmanage during a testcase
    """

    def __init__(self, address: Optional[str] = None,
                 timeout: Optional[float] = TIMEOUT,
                 router_id: Optional[str] = None,
                 edge_router_id: Optional[str] = None) -> None:
        self._timeout = timeout
        self._address = address
        self.router_id = router_id
        self.edge_router_id = edge_router_id
        self.router: List[str] = []
        if self.router_id:
            self.router = self.router + ['--router', self.router_id]
        elif self.edge_router_id:
            self.router = self.router + ['--edge-router', self.edge_router_id]
        self.returncode = 0  # the returncode from skmanage call
        self.stdout = ''  # of skmanage
        self.stderr = ''  # of skmanage

    def __call__(self, cmd: str,
                 address: Optional[str] = None,
                 input: Optional[str] = None,
                 timeout: Optional[float] = None) -> Optional[str]:
        addr = address or self._address
        assert addr, "address missing"
        with subprocess.Popen(['skmanage'] + cmd.split(' ') + self.router
                              + ['--bus', addr, '--indent=-1', '--timeout',
                                 str(timeout or self._timeout)], stdin=PIPE,
                              stdout=PIPE, stderr=STDOUT,
                              universal_newlines=True) as p:
            self.stdout, self.stderr = p.communicate(input)
            self.returncode = p.returncode
            if self.returncode != 0:
                raise Exception(f"{self.stdout} {self.stderr}")
            return self.stdout

    def create(self, long_type, kwargs):
        cmd = "CREATE --type=%s" % long_type
        for k, v in kwargs.items():
            cmd += " %s=%s" % (k, v)
        return json.loads(self(cmd))

    # According to the AMQP spec, the READ operation does not require a type, requires either a name or identity.
    # But the C management agent requires a long type to be specified (see management_agent.c)
    def read(self, long_type=None, name=None, identity=None):
        if name is not None and identity is not None:
            return json.loads(self(f"READ --type={long_type} --name {name} --identity {identity}"))
        if long_type is not None:
            if name is not None:
                return json.loads(self(f"READ --type={long_type} --name {name}"))
            if identity is not None:
                return json.loads(self(f"READ --type={long_type} --identity {identity}"))
        else:
            if identity is not None:
                return json.loads(self(f"READ --identity {identity}"))
            return json.loads(self(f"READ --name {name}"))

    def update(self, long_type, kwargs, name=None, identity=None):
        cmd = 'UPDATE --type=%s' % long_type
        if identity is not None:
            cmd += " --identity=%s" % identity
        elif name is not None:
            cmd += " --name=%s" % name
        for k, v in kwargs.items():
            cmd += " %s=%s" % (k, v)
        return json.loads(self(cmd))

    def delete(self, long_type, name=None, identity=None):
        cmd = 'DELETE --type=%s' %  long_type
        if identity is not None:
            cmd += " --identity=%s" % identity
        elif name is not None:
            cmd += " --name=%s" % name
        else:
            assert False, "name or identity not supplied!"
        self(cmd)

    def query(self, long_type):
        return json.loads(self('QUERY --type=%s' % long_type))

    def get_log(self, limit=None):
        cmd = 'GET-LOG'
        if limit:
            cmd += " limit=%s" % limit
        return json.loads(self(cmd))

    def delete_all_entities(self, long_entity_type):
        # First, query the router to get all entities of entity type
        results = self.query(long_entity_type)
        for result in results:
            entity_name = result.get('name')
            if entity_name:
                self.delete(long_entity_type, name=entity_name)

    def delete_adaptor_connections(self):
        results = self.query(CONNECTION_TYPE)
        for result in results:
            if result['protocol'] != 'amqp':
                identity = result.get("identity")
                self.update(CONNECTION_TYPE, {"adminStatus": "deleted"}, identity=identity)
        results = self.query(CONNECTION_TYPE)
        num_adaptor_connections = 0
        for result in results:
            if result['protocol'] != 'amqp' and result['host'] != 'egress-dispatch':
                num_adaptor_connections += 1
        assert num_adaptor_connections == 0


class MgmtMsgProxy:
    """
    Utility for creating and inspecting management messages
    """
    class _Response:
        def __init__(self, status_code, status_description, body):
            self.status_code        = status_code
            self.status_description = status_description
            if body.__class__ == dict and len(body.keys()) == 2 and 'attributeNames' in body.keys() and 'results' in body.keys():
                results = []
                names   = body['attributeNames']
                for result in body['results']:
                    result_map = {}
                    for i in range(len(names)):
                        result_map[names[i]] = result[i]
                    results.append(MgmtMsgProxy._Response(status_code, status_description, result_map))
                self.attrs = {'results': results}
            else:
                self.attrs = body

        def __getattr__(self, key):
            return self.attrs[key]

    def __init__(self, reply_addr):
        self.reply_addr = reply_addr

    def response(self, msg):
        ap = msg.properties
        return self._Response(ap['statusCode'], ap['statusDescription'], msg.body)

    def query_router(self):
        ap = {'operation': 'QUERY', 'type': ROUTER_METRICS_TYPE}
        return Message(properties=ap, reply_to=self.reply_addr)

    def query_connections(self):
        ap = {'operation': 'QUERY', 'type': CONNECTION_TYPE}
        return Message(properties=ap, reply_to=self.reply_addr)

    def query_links(self):
        ap = {'operation': 'QUERY', 'type': ROUTER_LINK_TYPE}
        return Message(properties=ap, reply_to=self.reply_addr)

    def query_addresses(self):
        ap = {'operation': 'QUERY',
              'type': ROUTER_ADDRESS_TYPE}
        return Message(properties=ap, reply_to=self.reply_addr)

    def create_connector(self, name, **kwargs):
        ap = {'operation': 'CREATE',
              'type': AMQP_CONNECTOR_TYPE,
              'name': name}
        return Message(properties=ap, reply_to=self.reply_addr,
                       body=kwargs)

    def delete_connector(self, name):
        ap = {'operation': 'DELETE',
              'type': AMQP_CONNECTOR_TYPE,
              'name': name}
        return Message(properties=ap, reply_to=self.reply_addr)


class TestTimeout:
    """
    A callback object for MessagingHandler class
    parent: A MessagingHandler with a timeout() method
    """
    __test__ = False

    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.timeout()


class PollTimeout:
    """
    A callback object for MessagingHandler scheduled timers
    parent: A MessagingHandler with a poll_timeout() method
    """

    def __init__(self, parent):
        self.parent = parent

    def on_timer_task(self, event):
        self.parent.poll_timeout()


def get_link_info(name, address):
    """
    Query the router at address for the status and statistics of the named link
    """
    skm = SkManager(address=address)
    rc = skm.query(ROUTER_LINK_TYPE)
    for item in rc:
        if item.get('name') == name:
            return item
    return None


def has_mobile_dest_in_address_table(address, dest):
    skm = SkManager(address=address)
    rc = skm.query(ROUTER_ADDRESS_TYPE)
    has_dest = False
    for item in rc:
        if dest in item.get("name"):
            has_dest = True
            break
    return has_dest


def get_inter_router_links(address):
    """
    Return a list of all links with type="inter-router" or that are on an inter-router-data connection
    :param address:
    """
    inter_router_links = []
    inter_router_data_ids = []
    skm = SkManager(address=address)
    conns = skm.query(CONNECTION_TYPE)
    for item in conns:
        if item.get("role") == "inter-router-data":
            inter_router_data_ids.append(item.get("identity"))
    rc = skm.query(ROUTER_LINK_TYPE)
    for item in rc:
        if item.get("linkType") == "inter-router" or item.get("connectionId") in inter_router_data_ids:
            inter_router_links.append(item)

    return inter_router_links


class Timestamp:
    """
    Time stamps for logging.
    """

    def __init__(self):
        self.ts = datetime.now()

    def __str__(self):
        return self.ts.strftime("%Y-%m-%d %H:%M:%S.%f")


class Logger:
    """
    Record an event log for a self test.
    May print per-event or save events to be printed later.
    Pytest will automatically collect the logs and will dump them for a failed test
    Optional file opened in 'append' mode to which each log line is written.
    """

    def __init__(self,
                 title: str = "Logger",
                 print_to_console: bool = False,
                 save_for_dump: bool = True,
                 python_log_level: Optional[int] = logging.DEBUG,
                 ofilename: Optional[str] = None) -> None:
        self.title = title
        self.print_to_console = print_to_console
        self.save_for_dump = save_for_dump
        self.python_log_level = python_log_level
        self.ofilename = ofilename
        self.logs: List[Tuple[Timestamp, str]] = []

    def log(self, msg):
        ts = Timestamp()
        if self.save_for_dump:
            self.logs.append((ts, msg))
        if self.print_to_console:
            print(f"{ts} {self.title}: {msg}")
            sys.stdout.flush()
        if self.python_log_level is not None:
            logline = f"{ts} {self.title}: {msg}"
            logging.log(self.python_log_level, logline)
        if self.ofilename is not None:
            with open(self.ofilename, 'a') as f_out:
                f_out.write("%s %s\n" % (ts, msg))
                f_out.flush()

    def dump(self):
        print(self)
        sys.stdout.flush()

    def __str__(self):
        lines = [self.title]
        for ts, msg in self.logs:
            lines.append("%s %s" % (ts, msg))
        res = str('\n'.join(lines))
        return res


def curl_available():
    """
    Check if the curl command line tool is present on the system.
    Return a tuple containing the version if found, otherwise
    return false.
    """
    try:
        returncode, out, err = run_curl(['--version'])
        if returncode == 0:
            # return curl version as a tuple (major, minor[,fix])
            # expects --version outputs "curl X.Y.Z ..."
            return tuple(int(x) for x in out.split()[1].split('.'))
    except:
        pass
    return False


def nginx_available():
    """
    Check if the nginx http server is present on the system.
    Return a tuple containing the version if found, otherwise
    return false.
    """
    try:
        popen_args = ['nginx', '-version']
        with subprocess.Popen(popen_args,
                              stdout=PIPE,
                              stderr=PIPE) as p:
            out = p.communicate()
            return p.returncode, out[0], out[1]
    except:
        pass
    return False


def openssl_available():
    """
    Check if the openssl command line tool is installed. Return a tuple
    containing the version if found, otherwise return False.
    """
    try:
        args = ['openssl', 'version']
        with subprocess.Popen(args, stdout=PIPE, stderr=PIPE,
                              universal_newlines=True) as p:
            out, err = p.communicate()
            if p.returncode == 0:
                return tuple([int(x) for x in out.split()[1].split('.')])
    except Exception:
        pass
    return False


def run_curl(args, input=None, timeout=TIMEOUT):
    """
    Run the curl command with the given argument list.
    Pass optional input to curls stdin.
    Return tuple of (return code, stdout, stderr)
    """
    popen_args = ['curl', '-q'] + args
    if timeout is not None:
        popen_args = popen_args + ["--max-time", str(timeout)]
    stdin_value = PIPE if input is not None else None
    with subprocess.Popen(popen_args, stdin=stdin_value, stdout=PIPE,
                          stderr=PIPE, universal_newlines=True) as p:
        out = p.communicate(input, timeout)
        return p.returncode, out[0], out[1]


def get_digest(file_path):
    "Compute a sha256 hash on file_path"
    h = hashlib.sha256()

    with open(file_path, 'rb') as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def _wait_adaptor_listeners_oper_status(listener_type,
                                        mgmt_address: str,
                                        oper_status: str,
                                        l_filter: Optional[Mapping[str, Any]] = None,
                                        timeout: float = TIMEOUT):
    """
    Wait until the selected listener socket operStatus has reached 'oper_status'
    """
    mgmt = Node.connect(mgmt_address, timeout=timeout)
    l_filter = l_filter or {}
    attributes = set(l_filter.keys())
    attributes.add("name")  # required for query() to work
    attributes.add("operStatus")

    def _filter_listener(listener):
        for key, value in l_filter.items():
            if listener[key] != value:
                return False
        return True

    def _check():
        listeners = mgmt.query(type=listener_type,
                               attribute_names=list(attributes)).get_dicts()
        listeners = filter(_filter_listener, listeners)
        assert listeners, "Filter error: no listeners matched"
        for listener in listeners:
            if listener['operStatus'] != oper_status:
                return False
        return True
    assert retry(_check, timeout=timeout, delay=0.25), \
        f"Timed out waiting for {listener_type} operStatus {oper_status}"


def wait_tcp_listeners_up(mgmt_address: str,
                          l_filter: Optional[Mapping[str, Any]] = None,
                          timeout: float = TIMEOUT):
    """
    Wait until the configured TCP listener sockets have come up. Optionally
    filter the set of configured listeners using attribute names and values
    """
    return _wait_adaptor_listeners_oper_status(TCP_LISTENER_TYPE, mgmt_address,
                                               'up', l_filter, timeout)
