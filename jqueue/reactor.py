"""
Reactor
-------

This module defines a reactor, useful for doing non-blocking network
IO operations. It is an abstraction on top of select/poll/epoll, and
enhances that system with simple to use callbacks.

The core class provided by this module is the :class:`Reactor`, which
provides the layer on top of select/poll/epoll and attaches callbacks to
individual file descriptors. It is also capable of attacking *step callbacks*
which are run after each poll of the reactor completes, which can be used for
maintenance functions.

The default :class:`Reactor` that is provided is different for each platform.
The :class:`LinuxReactor` is preferred, followed by :class:`PollReactor` and
finally :class:`SelectReactor`. Note, however, that whatever is chosen is
bound to the name :class:`Reactor` - all platforms are provided with the
same interface.

    >>> import reactor
    >>> r = reactor.Reactor()

On Linux::

    >>> type(r)
    <class 'reactor.LinuxReactor'>

On POSIX systems::

    >>> type(r)
    <class 'reactor.PollReactor'>

On systems supporing only select::

    >>> type(r)
    <class 'reactor.SelectReactor'>
"""
import logging
import select
import time

logger = logging.getLogger('reactor')

class Token:
    """
    A unique value, like ``object()``, but which has a nice string 
    representation.

        >>> A = Token('A')
        >>> A
        <A>
        >>> str(A)
        '<A>'
        >>> repr(A)
        '<A>'
    """
    def __init__(self, name):
        self.name = name

    def __str__(self):
        return '<' + self.name + '>'

    def __repr__(self):
        return '<' + self.name + '>'

def to_iterable(x, preferred_type=list):
    """
    Converts an object into an object which can be iterated (or, if a
    preferred type is given, into an object of the preferred type).

        >>> to_iterable(1)
        [1]
        >>> to_iterable(1, preferred_type=set)
        {1}
        >>> to_iterable(1, preferred_type=tuple)
        (1,)
        >>> to_iterable([1])
        [1]
        >>> to_iterable((1,))
        [1]
        >>> to_iterable({1})
        [1]
    """
    try:
        iter(x)
        return preferred_type(x)
    except TypeError:
        return preferred_type([x])

def to_file_descriptor(fobj):
    """
    Converts an object to a file descriptor. The given object can be either:

     - An integer file descriptor
     - An object with a ``fileno()`` method.

    :raises ValueError: If the given object cannot be converted.

        >>> class Descriptable:
        ...     def fileno(self):
        ...         return 42
        ...
        >>> to_file_descriptor(Descriptable())
        42
        >>> to_file_descriptor(42)
        42
        >>> try:
        ...     to_file_descriptor(object())
        ... except ValueError:
        ...     print('Failure')
        ...
        Failure
    """
    if isinstance(fobj, int):
        return fobj
    elif hasattr(fobj, 'fileno'):
        return fobj.fileno()
    else:
        raise ValueError(
            '{} cannot be converted to a file descriptor'.format(fobj))


# select.epoll takes time in seconds as a float, while select.poll takes an
# integer counting milliseconds. All reactors here use the epoll pattern, so
# we have to convert float seconds into integer milliseconds.
MSECS_PER_SECOND = 1000

READABLE = Token('READABLE')
WRITABLE = Token('WRITABLE')
ERROR = Token('ERROR')

class StepCallbackProcessor:
    """
    The base of all reactors. This provides a *very* minimal interface for
    having a list of callback functions, which can be called all at once.
    """
    def __init__(self):
        self.step_callbacks = set()

    def add_step_callback(self, func):
        """
        Adds a stepper function which runs after :meth:`poll`. The stepper
        function should take no arguments, and its return value is ignored.
        """
        self.step_callbacks.add(func)

    def run_step_callbacks(self):
        """
        Runs all the step callbacks.
        """
        for callback in self.step_callbacks:
            callback()

# This is the callback used if an error happens, and a file descriptor is lost.
# This shouldn't happen, but to avoid crashing the program, this is used as a
# default callback.
EMPTY_CALLBACK = lambda _: None

class PollLikeReactor(StepCallbackProcessor):
    """
    This serves as a skeleton which proviedes a basic reactor API for systems
    which support poll or similar APIs.
    """
    def __init__(self, poll_factory, read_flag, write_flag, err_flags):
        super().__init__()
        self.pollster = poll_factory()

        # Maps (file descriptor, event) pairs to a callback to call when that
        # particular event occurs.
        self.callbacks = {}

        # Maps each file descriptor to a set of events which have attached
        # callbacks on that file descriptor.
        self.fd_events = {}

        self.read_flag = read_flag
        self.write_flag = write_flag
        self.err_flags = err_flags

    def has_clients(self):
        """
        Returns True if any sockets have attached callbacks, or False
        otherwise.
        """
        return self.callbacks != {}

    def poll(self, timeout=None):
        """
        Gets any outstanding events, and runs all the callbacks bound to
        those events.

        The timeout is a ``float`` (or an ``int``), and can be separated 
        into three different cases:

        - If not given, or negative, then this method will wait indefinitely 
          for events.
        - If zero, this method will return immediately, regardless of
          whether or not it processed any events.
        - If greater than zero, this method will wait that many seconds
          for events before returning.
        """
        timeout = self._convert_timeout(timeout)
        events = self.pollster.poll(timeout)

        for fd, event_flag in events:
            event_set = self._flags_to_event_set(event_flag)

            # Since the write method may end up raising an exception (writing
            # to a closed sockets raises exceptions), make a best effort to
            # ensure that all data is read by running the read callback first.
            if READABLE in event_set:
                callback = self.callbacks.get((fd, READABLE), EMPTY_CALLBACK)
                callback((fd, READABLE))

            if WRITABLE in event_set:
                callback = self.callbacks.get((fd, WRITABLE), EMPTY_CALLBACK)
                callback((fd, WRITABLE))

            if ERROR in event_set:
                callback = self.callbacks.get((fd, ERROR), EMPTY_CALLBACK)
                callback((fd, ERROR))

        self.run_step_callbacks()

    def _convert_timeout(self, timeout):
        """
        All reactors take timeouts as floats, like select.epoll does. If any
        conversion is necessary, than this method should convert a float
        second into whatever kind of time the poll used in the subclass does.
        """
        if timeout is None:
            return -1
        else:
            return timeout

    def _event_set_to_flags(self, events):
        """
        Converts a set of events to a single flag.
        """
        flag = 0
        for event in events:
            if event is READABLE:
                flag = flag | self.read_flag
            elif event is WRITABLE:
                flag = flag | self.write_flag
            elif event is ERROR:
                for err_flag in self.err_flags:
                    flag |= err_flag

        return flag

    def _flags_to_event_set(self, flag):
        """
        Converts a single flag into a set of events.
        """
        events = set()
        if flag & self.read_flag:
            events.add(READABLE)
        if flag & self.write_flag:
            events.add(WRITABLE)

        for err_flag in self.err_flags:
            if flag & err_flag:
                events.add(ERROR)
                break

        return events

    def bind(self, fobj, events, callback):
        """
        Binds a callback, to be called when one of a list of events occurs
        on a file object.

        :param fobj: A file descriptor or an object with ``fileno()``.
        :param events: Either a single event, or a list of events.
        :param callback: A callback taking a tuple, such as the following:

            def callback(event_tuple):
                file_descriptor, event = event_tuple

        Note that the availalbe events are :const:`READABLE`,
        :const:`WRITABLE`, and :const:`ERROR`.
        """
        fd = to_file_descriptor(fobj)
        events = to_iterable(events, set)

        if fd not in self.fd_events:
            self.fd_events[fd] = events
            self.pollster.register(fd, self._event_set_to_flags(events))
        else:
            self.fd_events[fd] |= events

            flags = self._event_set_to_flags(self.fd_events[fd])
            self.pollster.modify(fd, flags)

        for event in events:
            self.callbacks[fd, event] = callback

    def unbind(self, fobj, events=None):
        """
        Stops watching the given file for the given events, or for all
        events if no events are given.

        :param fobj: A file descriptor or an object with ``fileno()``.
        :param events: Either a single event, or a list of events.

        Note that the availalbe events are :const:`READABLE`,
        :const:`WRITABLE`, and :const:`ERROR`.
        """
        fd = to_file_descriptor(fobj)
        if events is None:
            events = self.fd_events[fd]
        else:
            events = to_iterable(events, set)

        remaining_events = self.fd_events[fd] - events
        self.fd_events[fd] = remaining_events

        if not remaining_events:
            self.pollster.unregister(fd)
            del self.fd_events[fd]
        else:
            self.pollster.modify(fd, 
                self._event_set_to_flags(remaining_events))

        for event in events:
            del self.callbacks[fd, event]

if hasattr(select, 'epoll'):
    logger.debug('Linux platform detected - using epoll() reactor')
    class LinuxReactor(PollLikeReactor):
        """
        This provides a reactor for the epoll function available on Linux.
        """
        def __init__(self):
            super().__init__(select.epoll, select.EPOLLIN, select.EPOLLOUT,
                             (select.EPOLLHUP, select.EPOLLERR))

    Reactor = LinuxReactor
elif hasattr(select, 'poll'):
    logger.debug('Non-Linux POSIX platform detected - using poll() reactor')
    class PollReactor(PollLikeReactor):
        """
        This provides a reactor for the poll functions available under POSIX.
        """
        def __init__(self):
            super().__init__(select.poll, select.POLLIN, select.POLLOUT,
                             (select.POLLHUP, select.POLLERR))

        def _convert_timeout(self, timeout):
            """
            select.poll takes timeouts in integer milliseconds.
            """
            if timeout is None:
                return -1
            elif timeout < 0:
                return -1
            else:
                return int(timeout * MSECS_PER_SECOND)

    Reactor = PollReactor
else:
    logger.debug('Detected weakly-POSIX platform - using select() reactor')
    class SelectReactor(StepCallbackProcessor):
        """
        This provides a reactor for systems lacking a poll-equivalent API.
        """
        def __init__(self):
            super().__init__()
            # Maps each file descriptor to a callback which should be run
            # when a socket gets a reader, a writer, or an error.
            self.readers = {}
            self.writers = {}
            self.errors = {}

        def has_clients(self):
            """
            Returns True if any sockets have attached callbacks, or False
            otherwise.
            """
            return (self.readers != {} or
                    self.writers != {} or
                    self.errors != {})

        def poll(self, timeout=None):
            """
            Gets any outstanding events, and runs all the callbacks bound to
            those events.

            The timeout is a ``float`` (or an ``int``), and can be separated 
            into three different cases:

            - If not given, then this method will wait indefinitely for
              events.
            - If zero, this method will return immediately, regardless of
              whether or not it processed any events.
            - If greater than zero, this method will wait that many seconds
              for events before returning.
            """
            # If you try to run select() on Windows without any args, it
            # gives up and raises an exception. We have to catch this
            # condition before Windows does and kills us.
            timeout = self._convert_timeout(timeout)
            if not self.has_clients():
                # So, if we don't have any sockets we care about, then
                # this should block forever and stall the process. This
                # is bad, so just return instead.
                if timeout is None:
                    self.run_step_callbacks()
                    return
                else:
                    time.sleep(timeout)
                    self.run_step_callbacks()
                    return

            rlist, wlist, xlist = select.select(list(self.readers),
                list(self.writers), list(self.errors),
                timeout)

            for reader in rlist:
                callback = self.readers.get(reader, EMPTY_CALLBACK)
                callback((reader, READABLE))
            
            for writer in wlist:
                callback = self.writers.get(writer, EMPTY_CALLBACK)
                callback((writer, WRITABLE))

            for error in xlist:
                callback = self.errors.get(error, EMPTY_CALLBACK)
                callback((error, ERROR))

            self.run_step_callbacks()

        def _convert_timeout(self, timeout):
            """
            The select function manages its timeouts like select.epoll does,
            but doesn't accept negative timeouts (which are equivalent to
            None).
            """
            if timeout is None:
                return None
            elif timeout < 0:
                return None
            else:
                return timeout

        def bind(self, fobj, events, callback):
            """
            Binds a callback, to be called when one of a list of events occurs
            on a file object.

            :param fobj: A file descriptor or an object with ``fileno()``.
            :param events: Either a single event, or a list of events.
            :param callback: A callback taking a tuple, such as the following:

                def callback(event_tuple):
                    file_descriptor, event = event_tuple

            Note that the availalbe events are :const:`READABLE`,
            :const:`WRITABLE`, and :const:`ERROR`.
            """
            fd = to_file_descriptor(fobj)
            events = to_iterable(events, set)

            if READABLE in events:
                self.readers[fd] = callback

            if WRITABLE in events:
                self.writers[fd] = callback

            if ERROR in events:
                self.errors[fd] = callback

        def unbind(self, fobj, events=None):
            """
            Stops watching the given file for the given events, or for all
            events if no events are given.

            :param fobj: A file descriptor or an object with ``fileno()``.
            :param events: Either a single event, or a list of events.

            Note that the availalbe events are :const:`READABLE`,
            :const:`WRITABLE`, and :const:`ERROR`.
            """
            fd = to_file_descriptor(fobj)
            if events is None:
                events = {READABLE, WRITABLE, ERROR}
            else:
                events = to_iterable(events, set)

            if READABLE in events and fd in self.readers:
                del self.readers[fd]
            if WRITABLE in events and fd in self.writers:
                del self.writers[fd]
            if ERROR in events and fd in self.errors:
                del self.errors[fd]

    Reactor = SelectReactor
