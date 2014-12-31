"""
jqueue Server
-------------

The job of the server is, when given a list of files, it will save each of those
files as jobs, and then wait for all the results of those jobs to come back, and
store those in files beside the originals.
"""
from functools import singledispatch
import logging
import socket
import threading
import time

from jqueue import protocol, reactor, utils

LOGGER = logging.getLogger('jqueue.server')

# Messages shouldn't be too large, to ensure that transfer overhead isn't
# an issue
RECV_SIZE = 1024 * 1024

# Clients must be able to Ping us every 30 seconds, as the *worst case*
TTL_LIMIT = 30

# How long to offset the timeout, to let the other end download the message
# (Assumes at least 250 kilobytes per second)
DOWNLOAD_OFFSET_BYTESPERSEC = (1 / (250 * 1024))

class JobDispatcher:
    """
    This keeps track of which jobs are available, and those which are currently
    being worked on. This class's primary responsibility is to ensure that jobs
    which are 'abandoned' (that is, their ping time expires) are returned to the
    list of inactive jobs so they can be picked up by another client.
    """
    def __init__(self, jobs):
        self.inactive_jobs = jobs
        self.active_job_timeouts = {}
        self.active_job_ttls = {}

    def time_until_next_timeout(self):
        """
        Returns the number of seconds until the next timeout transpires.
        """
        # If no jobs are running, then we don't care how long we wait, since
        # this function will never have to run
        if not self.active_job_timeouts:
            return None

        nearest_timeout = min(self.active_job_timeouts.values())
        LOGGER.info('Waiting for %d seconds', nearest_timeout - time.time())
        return nearest_timeout - time.time()

    def handle_job_ping(self, job):
        """
        Handles a ping on a job, increasing its timeout by the TTL originally
        marked on the job.
        """
        if job not in self.active_job_timeouts:
            raise ValueError('The job {} is not active'.format(job))

        LOGGER.info('Updated timeout for job %s', job)
        self.active_job_timeouts[job] = time.time() + self.active_job_ttls[job]

    def handle_job_completion(self, job):
        """
        Handles the completion of a job, by removing its associated data.
        """
        if job not in self.active_job_timeouts:
            raise ValueError('The job {} is not active'.format(job))

        del self.active_job_timeouts[job]
        del self.active_job_ttls[job]

    def handle_job_timeouts(self):
        """
        Removes any active jobs which have timed out.
        """
        now = time.time()

        dead_jobs = set()
        for job, timeout in self.active_job_timeouts.items():
            if timeout < now:
                dead_jobs.add(job)

        for dead_job in dead_jobs:
            LOGGER.info('Job %s timed out', dead_job)
            del self.active_job_timeouts[dead_job]
            del self.active_job_ttls[dead_job]

            self.inactive_jobs.append(dead_job)

    def get_job(self, ttl):
        """
        Gets the next job, producing a ``protocol.Job`` to send to the client,
        or ``None``.
        """
        if not self.inactive_jobs:
            return None

        job_filename = self.inactive_jobs.pop()
        with open(job_filename, 'rb') as job_file_stream:
            job_content = job_file_stream.read()

        content_size = len(job_content)
        file_size_adjustment = int(DOWNLOAD_OFFSET_BYTESPERSEC * content_size)
        LOGGER.info('Adjusting timeout for job %s by %d seconds', 
            job_filename, file_size_adjustment)

        self.active_job_timeouts[job_filename] = ttl + time.time() + file_size_adjustment
        self.active_job_ttls[job_filename] = ttl
        return protocol.Job(job_filename, job_content)

    def has_jobs(self, active=True):
        """
        Returns ``True`` if there are jobs that need doing, or ``False`` 
        otherwise.

        Note that, if ``active`` is ``False``, then only inactive jobs are
        checked.
        """
        if active:
            return bool(self.inactive_jobs) or bool(self.active_job_timeouts)
        else:
            return bool(self.inactive_jobs)

class Server:
    """
    Waits for client requests and then handles them.
    """
    def __init__(self):
        self.server_launched = threading.Event()
        self.clients = {}
        self.client_buffers = {}
        self.send_buffers = {}

    def queue_unsent_data(self, sock, data):
        """
        Queues up data which needs to be sent to a particular socket, and
        registers the sending function to ensure that it gets sent.
        """
        self.send_buffers[sock.fileno()] = data
        self.my_reactor.bind(sock, reactor.WRITABLE, self.handle_unsent_data)

    def handle_error(self, fd_event):
        """
        Handles errors on a socket.
        """
        fd, event = fd_event
        LOGGER.info('Error on FD %d', fd)
        if fd in self.clients:
            self.close_client(self.clients[fd])
        else:
            self.close()

    def handle_connect(self, _):
        """
        Handles a connection to the server socket, establishing a new client
        to get a command from.
        """
        client_sock, _ = self.server_sock.accept()
        LOGGER.info('Connection from %s', _)

        self.clients[client_sock.fileno()] = client_sock
        self.client_buffers[client_sock.fileno()] = b''
        self.my_reactor.bind(client_sock, reactor.READABLE, self.handle_data)
        self.my_reactor.bind(client_sock, reactor.ERROR, self.handle_error)

    def handle_unsent_data(self, fd_event):
        """
        Sends any unsent data across a socket, closing the socket if all the 
        data has been sent.
        """
        fd, event = fd_event
        sock = self.clients[fd]
        buffer = self.send_buffers[fd]

        sent = sock.send(buffer)
        if len(buffer) == sent:
            # When finished, close the socket, since this server allows only
            # one request per connection (and thus, we can only send one
            # response per connection)
            self.close_client(sock)
            del self.send_buffers[fd]
        else:
            self.send_buffers[fd] = buffer[sent:]

    def handle_data(self, fd_event):
        """
        Handles incoming data from a client, chunking it into messages and
        passing them onto the message handler.
        """
        fd, event = fd_event
        client_sock = self.clients[fd]
        client_buff = self.client_buffers[fd]

        data = client_sock.recv(RECV_SIZE)
        if not data:
            self.close_client(client_sock)
        else:
            client_buff += data
            try:
                while True:
                    message, client_buff = protocol.message_from_bytes(client_buff)
                    if message is not None:
                        LOGGER.info('Got message %s from client', type(message))
                        self.handle_message(message, client_sock)
                    else:
                        break

                if fd in self.client_buffers:
                    # It may be that the socket is closed now, since they are
                    # discarded after a single request
                    LOGGER.info('%d bytes unprocessed', len(self.client_buffers[fd]))
                    self.client_buffers[fd] = client_buff
            except ValueError as exn:
                # If we get any malformed data, kill the connection
                LOGGER.exception(exn)
                self.close_client(client_sock)

    @utils.dispatchmethod
    def handle_message(self, message, client):
        """
        The fallback handler simply sends out a DO_NOT_UNDERSTAND error
        to messages the server is not prepared to handle.
        """
        LOGGER.info('Unknown message: %s', message)
        raw_message = protocol.serialize(
            protocol.Error(protocol.Errors.DO_NOT_UNDERSTAND))
        self.queue_unsent_data(client, raw_message)

    @handle_message.register(protocol.RequestJob)
    def _(self, message, client):
        """
        Job requests can be handled in three ways:

        1. A job is available, and it is sent back
        2. The TTL is too high, and an error is generated
        3. No jobs are available, and an errors is generated
        """
        LOGGER.info('Got job request: %s', message)

        if message.ttl > TTL_LIMIT:
            LOGGER.info('|| TTL too high')
            raw_message = protocol.serialize(
                protocol.Error(protocol.Errors.TTL_TOO_HIGH))
        elif not self.job_dispatcher.has_jobs(active=False):
            LOGGER.info('|| No jobs')
            raw_message = protocol.serialize(
                protocol.Error(protocol.Errors.NO_JOB_AVAIL))
        else:
            job = self.job_dispatcher.get_job(message.ttl)
            LOGGER.info('|| Got job %s of %d bytes', job.id, len(job.data))
            raw_message = protocol.serialize(job)

        self.queue_unsent_data(client, raw_message)

    @handle_message.register(protocol.Ping)
    def _(self, message, client):
        """
        Pings can either succeed and update the job's timeout, or fail and
        produce an error.
        """
        LOGGER.info('Got ping: %s', message)

        try:
            LOGGER.info('|| No such job')
            self.job_dispatcher.handle_job_ping(message.id)
            raw_message = protocol.serialize(protocol.Ok())
        except ValueError:
            LOGGER.info('|| Ping successful')
            raw_message = protocol.serialize(
                protocol.Error(protocol.Errors.NOT_RESERVED))

        self.queue_unsent_data(client, raw_message)

    @handle_message.register(protocol.SubmitResult)
    def _(self, message, client):
        """
        Results can either be ignored, or accepted and written to the result
        file.
        """
        LOGGER.info('Got result for %s with %d bytes', message.id, len(message.data))

        try:
            self.job_dispatcher.handle_job_completion(message.id)
            with open(message.id + b'.result', 'wb') as response_stream:
                response_stream.write(message.data)
            raw_message = protocol.serialize(protocol.Ok())
            LOGGER.info('|| Job completion successful')
        except ValueError:
            LOGGER.info('|| No such job')
            raw_message = protocol.serialize(
                protocol.Error(protocol.Errors.NOT_RESERVED))

        self.queue_unsent_data(client, raw_message)

    def open(self, port):
        """
        Opens the server socket used by the server
        """
        self.server_sock = socket.socket()
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(('0.0.0.0', port))
        self.server_sock.listen(5)
        self.server_sock.setblocking(False)

    def close_client(self, client):
        """
        Closes a single client.
        """
        fd = client.fileno()
        self.my_reactor.unbind(client)
        client.close()

        del self.clients[fd]
        del self.client_buffers[fd]

    def close(self):
        """
        Closes the sockets used by the server.
        """
        for client in list(self.clients.values()):
            client.close()

        self.server_sock.close()

    def run(self, file_list, port=2267):
        """
        Runs the server, on the given port, until no jobs remain.
        """
        self.open(port)

        self.my_reactor = reactor.Reactor()
        self.my_reactor.bind(self.server_sock, reactor.ERROR, self.handle_error)
        self.my_reactor.bind(self.server_sock, reactor.READABLE, self.handle_connect)

        self.job_dispatcher = JobDispatcher(file_list)
        self.my_reactor.add_step_callback(self.job_dispatcher.handle_job_timeouts)

        self.server_launched.set()
        while self.job_dispatcher.has_jobs():
            timeout = self.job_dispatcher.time_until_next_timeout()
            self.my_reactor.poll(timeout)

            if not self.job_dispatcher.has_jobs():
                break

        self.close()
