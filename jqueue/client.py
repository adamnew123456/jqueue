"""
jqueue Client
-------------

This forms the infrastructure needed to run the client, which is essentially
a thread which is in communication with the server.

The main class here is ``ClientThread``, which is generally used as follows::

    >>> client_handler = ClientThread()
    >>> client_handler.start()
    >>> while True:
    ...     job = client_handler.get_job()
    ...     if job is None:
    ...         break
    ...     result = process(job.data)
    ...     client_handler.submit_result(result)
    ...
"""
from collections import namedtuple
import queue
import socket
import threading

from jqueue import protocol, utils

RECV_SIZE = 1024 * 1024

class ClientThread(threading.Thread):
    """
    Acts as a bridge between a program which processes jqueue jobs, and the
    jqueue server.

    This takes care of acquiring a job from the server, keeping it alive so
    that the program can continue to work on it, and finally submitting
    results back to the server.
    """

    # These are the different types of messages which can be exchanged between
    # the main thread and the client thread:
    Get = namedtuple('Get', ['ttl'])
    Job = namedtuple('Job', ['data'])
    Submit = namedtuple('Submit', ['data'])
    Quit = namedtuple('Quit', [])

    def __init__(self, host, port=2267, **kwargs):
        kwargs.update({'name': 'ClientThread'})
        threading.Thread.__init__(self, **kwargs)

        self.has_started = threading.Event()
        self.server = (host, port)
        self.actions = queue.Queue()
        self.results = queue.Queue()

    def has_job(self):
        """
        Returns ``True`` if a job is currently reserved by this object, or
        ``False`` otherwise.
        """
        return self.current_job is not None

    def get_job(self, ttl):
        """
        Submits a request to get a new job - note that this only works if
        no current job is running.

        :param int ttl: How long to wait before sending a ping to the server.
            Note that, in high workloads, this should be higher (30 seconds is
            the current maximum) to avoid robbing the main thread of the ability
            to do work.
        """
        self.actions.put(ClientThread.Get(ttl))
        return self.results.get()

    def submit_result(self, data):
        """
        Submits the currently outstanding job to the server.

        :param bytes data: The raw data to submit.
        """
        self.actions.put(ClientThread.Submit(data))

    def terminate(self):
        """
        Terminates the running ClientThread.
        """
        self.actions.put(ClientThread.Quit())

    def get_server_connection(self):
        """
        Creates a socket and connects it to the server.
        """
        sock = socket.socket()
        try:
            sock.connect(self.server)
            return sock
        except OSError:
            return None

    def send_and_get_response(self, sock, message):
        """
        Sends a message and gets the server's response to it.
        """
        request_bytes = protocol.serialize(message)
        sock.sendall(request_bytes)

        response_bytes = b''
        while True:
            try:
                chunk = sock.recv(RECV_SIZE)
            except OSError:
                chunk = b''

            if not chunk:
                break
            response_bytes += chunk

        response, _ = protocol.message_from_bytes(response_bytes)
        return response

    def send_ping(self):
        """
        Sends a ping to the server, to ensure that our current message isn't
        taken from us.
        """
        if self.current_job is None:
            return

        sock = self.get_server_connection()
        if sock is None:
            # If the server is gone, than there's nothing we can do
            self.current_job = None
            self.current_job_ttl = None
            return

        response = self.send_and_get_response(sock, protocol.Ping(self.current_job))

        # Kill the current job if the server doesn't think we have it
        if not isinstance(response, protocol.Ok):
            self.current_job = None
            self.current_job_ttl = None

    @utils.dispatchmethod
    def handle_request(self, message):
        """
        The fallback request handler does nothing, and is only here to comply
        with the requirements of ``dispatchmethod``
        """
        raise ValueError("{} is an invalid message".format(message))

    @handle_request.register(Get)
    def _(self, message):
        """
        Tries to get a job, and put in the result queue.
        """
        if self.current_job is not None:
            self.results.put(None)

        sock = self.get_server_connection()
        if sock is None:
            self.results.put(None)
            return

        response = self.send_and_get_response(sock, protocol.RequestJob(message.ttl))
        if isinstance(response, protocol.Job):
            self.current_job = response.id
            self.current_job_ttl = message.ttl
            self.results.put(ClientThread.Job(response.data))
        else:
            self.results.put(None)

    @handle_request.register(Submit)
    def _(self, message):
        """
        Tries to send data back to the server as a result to the current job.
        """
        if self.current_job is None:
            return

        sock = self.get_server_connection()
        if sock is None:
            self.current_job = None
            self.current_job_ttl = None
            return

        response = self.send_and_get_response(sock, 
            protocol.SubmitResult(self.current_job, message.data))
        self.current_job = None
        self.current_job_ttl = None

    @handle_request.register(Quit)
    def _(self, message):
        """
        The main loop in ``ClientThread.run`` expects a truthy return value
        from the handler to indicate that it (the main loop) should exit.
        """
        return True

    def run(self):
        """
        Runs the thread, which involves three main responsibilities:

        - Getting a job from the server, and passing it back to the caller.
        - Pinging the server for the current live job, to ensure that it isn't
          dropped.
        - Sending the result back to the server.
        """
        self.current_job = None
        self.current_job_ttl = None

        self.has_started.set()
        is_done = False
        while not is_done:
            if self.current_job_ttl is None:
                request = self.actions.get()
                is_done = self.handle_request(request)
            else:
                try:
                    # The reasoning behind halving the TTL to get our ping time,
                    # is that we leave ourselves a 'safety zone' in case this
                    # thread doesn't get run exactly after the timeout expires
                    request = self.actions.get(timeout=self.current_job_ttl // 2)
                    is_done = self.handle_request(request)
                except queue.Empty:
                    pass

                self.send_ping()
