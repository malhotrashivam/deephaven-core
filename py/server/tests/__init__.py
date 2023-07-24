#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from test_helper import start_jvm_for_tests

# start_jvm_for_tests()

from deephaven_server import Server

s = Server(jvm_args=["-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler", "-Dprocess.info.system-info.enabled=false"])

s.start()