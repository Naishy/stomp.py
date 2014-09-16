import threading
from urlparse import urlparse
import websocket
from websocket import _exceptions
from stomp import listener
import exception
import listener
from constants import *
import utils
from backward import decode, encode, get_errno, pack
import sys

__author__ = 'naisht2'

import logging

log = logging.getLogger('stomp.py')


class WSTransport(listener.Publisher):
    def __init__(self,
                 url,
                 reconnect_sleep_initial=0.1,
                 reconnect_sleep_increase=0.5,
                 reconnect_sleep_jitter=0.1,
                 reconnect_sleep_max=60.0,
                 reconnect_attempts_max=3,
                 wait_on_receipt=False,
                 timeout=None,
                 keepalive=None,
                 vhost=None,
                 http_proxy=None,
                 http_proxy_port=0
    ):
        """
        \param host_and_ports
            a list of (host, port) tuples.

        \param prefer_localhost
            if True and the local host is mentioned in the (host,
            port) tuples, try to connect to this first

        \param try_loopback_connect
            if True and the local host is found in the host
            tuples, try connecting to it using loopback interface
            (127.0.0.1)

        \param reconnect_sleep_initial
            initial delay in seconds to wait before reattempting
            to establish a connection if connection to any of the
            hosts fails.

        \param reconnect_sleep_increase
            factor by which the sleep delay is increased after
            each connection attempt. For example, 0.5 means
            to wait 50% longer than before the previous attempt,
            1.0 means wait twice as long, and 0.0 means keep
            the delay constant.

        \param reconnect_sleep_max
            maximum delay between connection attempts, regardless
            of the reconnect_sleep_increase.

        \param reconnect_sleep_jitter
            random additional time to wait (as a percentage of
            the time determined using the previous parameters)
            between connection attempts in order to avoid
            stampeding. For example, a value of 0.1 means to wait
            an extra 0%-10% (randomly determined) of the delay
            calculated using the previous three parameters.

        \param reconnect_attempts_max
            maximum attempts to reconnect

        \param wait_on_receipt
            if a receipt is specified, then the send method should wait
            (block) for the server to respond with that receipt-id
            before continuing

        \param timeout
            the timeout value to use when connecting the stomp socket

        \param keepalive
            some operating systems support sending the occasional heart
            beat packets to detect when a connection fails.  This
            parameter can either be set set to a boolean to turn on the
            default keepalive options for your OS, or as a tuple of
            values, which also enables keepalive packets, but specifies
            options specific to your OS implementation

        \param vhost
            specify a virtual hostname to provide in the 'host' header of the connection
        """

        self.listeners = {}

        self.__reconnect_sleep_initial = reconnect_sleep_initial
        self.__reconnect_sleep_increase = reconnect_sleep_increase
        self.__reconnect_sleep_jitter = reconnect_sleep_jitter
        self.__reconnect_sleep_max = reconnect_sleep_max
        self.__reconnect_attempts_max = reconnect_attempts_max
        self.__timeout = timeout

        self.socket = None
        self.__socket_semaphore = threading.BoundedSemaphore(1)
        self.current_host_and_port = None

        self.__receiver_thread_exit_condition = threading.Condition()
        self.__receiver_thread_exited = False
        self.__send_wait_condition = threading.Condition()
        self.__connect_wait_condition = threading.Condition()

        self.running = False
        self.blocking = None
        self.connected = False
        self.connection_error = False

        self.__url = url

        self.__receipts = {}
        self.__wait_on_receipt = wait_on_receipt

        # flag used when we receive the disconnect receipt
        self.__disconnect_receipt = None

        # function for creating threads used by the connection
        self.create_thread_fc = utils.default_create_thread

        self.__keepalive = keepalive
        self.vhost = vhost

        self.__http_proxy = http_proxy
        self.__http_proxy_port = http_proxy_port

    def override_threading(self, create_thread_fc):
        """
        Override for thread creation. Use an alternate threading library by
        setting this to a function with a single argument (which is the receiver loop callback).
        The thread which is returned should be started (ready to run)
        """
        self.create_thread_fc = create_thread_fc

    #
    # Manage the connection
    #

    def start(self):
        """
        Start the connection. This should be called after all
        listeners have been registered. If this method is not called,
        no frames will be received by the connection.
        """
        self.running = True
        self.attempt_connection()
        thread = self.create_thread_fc(self.__receiver_loop)
        self.notify('connecting')

    def stop(self):
        """
        Stop the connection. Performs a clean shutdown by waiting for the
        receiver thread to exit.
        """
        self.__receiver_thread_exit_condition.acquire()
        while not self.__receiver_thread_exited:
            self.__receiver_thread_exit_condition.wait()
        self.__receiver_thread_exit_condition.release()

    def is_connected(self):
        """
        Return true if the socket managed by this connection is connected
        """

        return self.socket is not None and self.socket.connected and self.connected

    def set_connected(self, connected):
        self.__connect_wait_condition.acquire()
        self.connected = connected
        if connected:
            self.__connect_wait_condition.notify()
        self.__connect_wait_condition.release()

    #
    # Manage objects listening to incoming frames
    #

    def set_listener(self, name, listener):
        """
        Set a named listener to use with this connection

        \see listener::ConnectionListener

        \param name
            the name of the listener
        \param listener
            the listener object
        """
        self.listeners[name] = listener

    def remove_listener(self, name):
        """
        Remove a listener according to the specified name

        \param name the name of the listener to remove
        """
        del self.listeners[name]

    def get_listener(self, name):
        """
        Return the named listener

        \param name the listener to return
        """
        if name in self.listeners:
            return self.listeners[name]
        else:
            return None

    def disconnect_socket(self):
        """
        Disconnect the underlying socket connection
        """
        self.running = False
        if self.socket is not None:
            self.socket.close()
        self.current_host_and_port = None

    def transmit(self, frame):
        """
        Convert a frame object to a frame string and transmit to the server.
        """
        for listener in self.listeners.values():
            if not listener: continue
            if not hasattr(listener, 'on_send'):
                continue
            listener.on_send(frame)

        if frame.cmd is CMD_DISCONNECT and 'receipt' in frame.headers:
            self.__disconnect_receipt = frame.headers.get('receipt', None)

        lines = utils.convert_frame_to_lines(frame)

        packed_frame = pack(lines)

        log.info("Sending frame %s", lines)

        if self.socket is not None:
            try:
                self.__socket_semaphore.acquire()
                try:
                    self.socket.send(encode(packed_frame))
                finally:
                    self.__socket_semaphore.release()
            except Exception:
                _, e, _ = sys.exc_info()
                log.error("Error sending frame", exc_info=1)
                raise e
        else:
            raise exception.NotConnectedException()

    def process_frame(self, f, frame_str):
        frame_type = f.cmd.lower()
        if frame_type in ['connected', 'message', 'receipt', 'error', 'heartbeat']:
            if frame_type == 'message':
                (f.headers, f.body) = self.notify('before_message', f.headers, f.body)
            self.notify(frame_type, f.headers, f.body)
            log.info("Received frame: %r, headers=%r, body=%r", f.cmd, f.headers, f.body)
        else:
            log.warning("Unknown response frame type: '%s' (frame length was %d)", frame_type, len(frame_str))

    def notify(self, frame_type, headers=None, body=None):
        """
        Utility function for notifying listeners of incoming and outgoing messages

        \param frame_type
            the type of message

        \param headers
            the map of headers associated with the message

        \param body
            the content of the message
        """
        if frame_type == 'receipt':
            # logic for wait-on-receipt notification
            receipt = headers['receipt-id']
            self.__send_wait_condition.acquire()
            try:
                self.__receipts[receipt] = None
                self.__send_wait_condition.notify()
            finally:
                self.__send_wait_condition.release()

            # received a stomp 1.1+ disconnect receipt
            if receipt == self.__disconnect_receipt:
                self.disconnect_socket()
                self.set_connected(False)
                frame_type = 'disconnected'

        elif frame_type == 'connected':
            self.set_connected(True)

        elif frame_type == 'disconnected':
            self.set_connected(False)

        rtn = None
        for listener in self.listeners.values():
            if not listener:
                continue
            if not hasattr(listener, 'on_%s' % frame_type):
                log.debug("listener %s has no method on_%s", listener, frame_type)
                continue

            if frame_type == 'connecting':
                listener.on_connecting([self.__url, 0])
                continue
            elif frame_type == 'disconnected':
                listener.on_disconnected()
                continue
            elif frame_type == 'heartbeat':
                listener.on_heartbeat()
                continue

            if frame_type == 'error' and self.connected is False:
                self.__connect_wait_condition.acquire()
                self.connection_error = True
                self.__connect_wait_condition.notify()
                self.__connect_wait_condition.release()

            notify_func = getattr(listener, 'on_%s' % frame_type)
            rtn = notify_func(headers, body)
            if rtn:
                (headers, body) = rtn
        if rtn:
            return rtn

    def __receiver_loop(self):
        """
        Main loop listening for incoming data.
        """
        log.debug("Starting receiver loop")
        try:
            while self.running:
                if self.socket is None:
                    break

                try:
                    while self.running:
                        frame = self.__read()
                        f = utils.parse_frame(frame)
                        self.process_frame(f, frame)
                except _exceptions.WebSocketConnectionClosedException:
                    if self.running:
                        self.notify('disconnected')
                        self.running = False
                    break
                finally:
                    try:
                        self.socket.close()
                    except:
                        pass  # ignore errors when attempting to close socket
                    self.socket = None
                    self.current_host_and_port = None
        finally:
            self.__receiver_thread_exit_condition.acquire()
            self.__receiver_thread_exited = True
            self.__receiver_thread_exit_condition.notifyAll()
            self.__receiver_thread_exit_condition.release()
            log.debug("Receiver loop ended")

    def __read(self):
        """
        Read the next frame(s) from the socket.
        """

        result = []
        if self.socket:
            result = self.socket.recv()
        return result

    def attempt_connection(self):
        """
        Try connecting to the (host, port) tuples specified at construction time.
        """
        self.connection_error = False
        sleep_exp = 1
        connect_count = 0
        while self.running and self.socket is None and connect_count < self.__reconnect_attempts_max:
            parsed = urlparse(self.__url)
            log.info("Attempting connection to url {0}".format(self.__url))
            self.socket = websocket.WebSocket()
            self.socket.connect(self.__url,
                                timeout=self.__timeout,
                                http_proxy_host=self.__http_proxy,
                                http_proxy_port=self.__http_proxy_port)
            self.current_host_and_port = [parsed.hostname, parsed.port]

            # if self.socket is None:
            # sleep_duration = (min(self.__reconnect_sleep_max,
            # ((self.__reconnect_sleep_initial / (1.0 + self.__reconnect_sleep_increase))
            #                            * math.pow(1.0 + self.__reconnect_sleep_increase, sleep_exp)))
            #                       * (1.0 + random.random() * self.__reconnect_sleep_jitter))
            #     sleep_end = time.time() + sleep_duration
            #     log.debug("Sleeping for %.1f seconds before attempting reconnect", sleep_duration)
            #     while self.running and time.time() < sleep_end:
            #         time.sleep(0.2)
            #
            #     if sleep_duration < self.__reconnect_sleep_max:
            #         sleep_exp += 1

        if not self.socket:
            raise exception.ConnectFailedException()

    def wait_for_connection(self, timeout=None):
        """
        Wait until we've established a connection with the server.
        """
        if timeout is not None:
            wait_time = timeout / 10.0
        else:
            wait_time = None
        self.__connect_wait_condition.acquire()
        while not self.is_connected() and not self.connection_error:
            self.__connect_wait_condition.wait(wait_time)

        self.__connect_wait_condition.release()
