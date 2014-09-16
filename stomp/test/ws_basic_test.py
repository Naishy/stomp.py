import os
import signal
import time
import unittest

import stomp
from stomp import exception

from stomp.test.testutils import *


class WSTestBasicSend(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(funcName)-20s %(message)s', level=logging.DEBUG)
        conn = stomp.connect.WebSocketStompConnection12("ws://10.221.4.51:8080/uta-eventing/api/stomp", heartbeats=(200, 300))
        listener = TestListener('123')
        conn.set_listener('', listener)
        conn.start()
        conn.connect('admin', 'password', wait=True)
        self.conn = conn
        self.listener = listener
        self.timestamp = time.strftime('%Y%m%d%H%M%S')
        
    def tearDown(self):
        if self.conn:
            self.conn.disconnect()
            self.conn = None

    def test_subscribe_unsubscribe(self):
        queuename = '/enterprises/ECB/clusters/DEVEND/services/tep-eventing/tep-feed'
        self.conn.subscribe(destination=queuename, id=1, ack='auto')
        time.sleep(60)
        self.conn.unsubscribe(id=1)

        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')

    def test_invalid_unsubscribe(self):
        self.conn.unsubscribe(id=99)

        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')

    def test_invalid_subscribe(self):
        queuename = '/this/is/an/invalid/destination'
        self.conn.subscribe(destination=queuename, id=1, ack='auto')

        self.listener.wait_for_error()

        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.errors == 1, 'should have received 1 error')

    def test_subscribe_to_turret_status(self):
        queuename = '/enterprises/ECB/clusters/DEVEND/services/turret-eventing/turret-notifications'
        self.conn.subscribe(destination=queuename, id=1, ack='auto')

        time.sleep(10)
        self.conn.unsubscribe(id=1)

        self.assert_(self.listener.connections == 1, 'should have received 1 connection acknowledgement')
        self.assert_(self.listener.errors == 0, 'should not have received any errors')