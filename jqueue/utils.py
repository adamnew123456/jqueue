"""
Various Utilities
-----------------

These are various functions which don't have any direct dependency upon the
state of the rest of the programs.
"""
from functools import singledispatch, update_wrapper

def dispatchmethod(func):
    """
    This provides for a way to use ``functools.singledispatch`` inside of a class. It has the same
    basic interface that ``singledispatch`` does:
    
    >>> class A:
    ...     @dispatchmethod
    ...     def handle_message(self, message):
    ...         # Fallback code...
    ...         pass
    ...     @handle_message.register(int)
    ...     def _(self, message):
    ...         # Special int handling code...
    ...         pass
    ...
    >>> a = A()
    >>> a.handle_message(42)
    # Runs "Special int handling code..."
    
    Note that using ``singledispatch`` in these cases is impossible, since it tries to dispatch
    on the ``self`` argument, not the ``message`` argument. This is technically a double
    dispatch, since both the type of ``self`` and the type of the second argument are used to
    determine what function to call - for example:
    
    >>> class A:
    ...     @dispatchmethod
    ...     def handle_message(self, message):
    ...         print('A other', message)
    ...         pass
    ...     @handle_message.register(int)
    ...     def _(self, message):
    ...         print('A int', message)
    ...         pass
    ...
    >>> class B:
    ...     @dispatchmethod
    ...     def handle_message(self, message):
    ...         print('B other', message)
    ...         pass
    ...     @handle_message.register(int)
    ...     def _(self, message):
    ...         print('B int', message)
    ...
    >>> def do_stuff(A_or_B):
    ...     A_or_B.handle_message(42)
    ...     A_or_B.handle_message('not an int')
    
    On one hand, either the ``dispatchmethod`` defined in ``A`` or ``B`` is used depending
    upon what object one passes to ``do_stuff()``, but on the other hand, ``do_stuff()``
    causes different versions of the dispatchmethod (found in either ``A`` or ``B``) 
    to be called (both the fallback and the ``int`` versions are implicitly called).
    
    Note that this should be fully compatable with ``singledispatch`` in any other respects
    (that is, it exposes the same attributes and methods).
    """
    dispatcher = singledispatch(func)

    def register(type):
        def _register(func):
            return dispatcher.register(type)(func)
        return _register

    def dispatch(type):
        def wrapper(inst, dispatch_data, *args, **kwargs):
            impl = dispatcher.dispatch(type)
            return impl(inst, dispatch_data, *args, **kwargs)
        return wrapper

    def wrapper(inst, dispatch_data, *args, **kwargs):
        cls = type(dispatch_data)
        impl = dispatch(cls)
        return impl(inst, dispatch_data, *args, **kwargs)

    wrapper.register = register
    wrapper.dispatch = dispatch
    wrapper.registry = dispatcher.registry
    wrapper._clear_cache = dispatcher._clear_cache
    update_wrapper(wrapper, func)
    return wrapper

def to_netstring(buff):
    """
    Converts a buffer into a netstring.

        >>> to_netstring(b'hello')
        b'5:hello,'
        >>> to_netstring(b'with a nul\\x00')
        b'11:with a nul\\x00,'
    """
    bytes_length = str(len(buff)).encode('ascii')
    return bytes_length + b':' + buff + b','

def netstring_from_buffer(buff):
    """
    Reads a netstring from a buffer, returning a pair containing the
    content of the netstring, and all unread data in the buffer.

    Note that this assumes that the buffer starts with a netstring - if it
    does not, then this function returns ``(b'', buff)``.

        >>> netstring_from_buffer(b'0:,')
        (b'', b'')
        >>> netstring_from_buffer(b'5:hello,')
        (b'hello', b'')
        >>> netstring_from_buffer(b'5:hello,this is some junk')
        (b'hello', b'this is some junk')
        >>> netstring_from_buffer(b'11:with a nul\\x00,junk')
        (b'with a nul\\x00', b'junk')
        >>> netstring_from_buffer(b'5:hello no comma')
        (b'', b'5:hello no comma')
        >>> netstring_from_buffer(b'x:malformed length')
        (b'', b'x:malformed length')
        >>> netstring_from_buffer(b'500:wrong size,')
        (b'', b'500:wrong size,')
        >>> netstring_from_buffer(b'999:incomplete')
        (b'', b'999:incomplete')
    """
    length = b''
    colon_idx = buff.find(b':')
    if colon_idx == -1:
        return (b'', buff)
    else:
        str_size, data = buff[:colon_idx], buff[colon_idx + 1:]

        try:
            size = int(str_size)

            netstring = data[:size]
            if len(netstring) < size:
                # If the length specifier is wrong, then it isn't a valid
                # netstring
                return (b'', buff)

            if data[size] != ord(b','):
                # If the netstring lacks the trailing comma, then it isn't a
                # valid netstring
                return (b'', buff)
            
            # The extra 1 skips over the comma that terminates the netstring
            rest = data[size + 1:]
            return (netstring, rest)
        except ValueError:
            # If we can't parse the numerals, then it isn't a valid netstring
            return (b'', buff)
