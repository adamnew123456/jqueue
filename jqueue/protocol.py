"""
jqueue Protocol
---------------

The jqueue protocol consists of 6 different types of messages, which have a
particular wire protocol as well as a logical form - this module implements
both the wire protocol as well as the logical form, so that messages can not
only be serialized and unserialzed, they can also be manipulated as Python
objects.

These types of messages are:

- RequestJob(ttl)
- Job(id, data)
- Ping(id)
- SubmitResult(id, data)
- Ok()
- Error(type)
"""
from collections import namedtuple
from enum import Enum
from functools import singledispatch
import struct

from jqueue import utils

RequestJob = namedtuple('RequestJob', ['ttl'])
Job = namedtuple('Job', ['id', 'data'])
Ping = namedtuple('Ping', ['id'])
SubmitResult = namedtuple('SubmitResult', ['id', 'data'])
Ok = namedtuple('Ok', [])
Error = namedtuple('Error', ['type'])

class Errors(Enum):
    NO_JOB_AVAIL = 1
    TTL_TOO_HIGH = 2
    NOT_RESERVED = 3
    DO_NOT_UNDERSTAND = 4

class MessageTypes(Enum):
    RequestJob = 1
    Job = 2
    Ping = 3
    SubmitResult = 4
    Ok = 5
    Error = 6

@singledispatch
def serialize(message):
    raise ValueError('Cannot serialize a {}'.format(message))

@serialize.register(RequestJob)
def _(message):
    """
    Job requests are serialized according to the following format:

        <Type>  <TTL>
        
        byte    32-bit unsigned integer
    """
    return struct.pack('<bI', MessageTypes.RequestJob.value, message.ttl)

@serialize.register(Job)
def _(message):
    """
    Job replies are serialized according to the following format:

        <Type>  <ID>        <Data>
        
        byte    netstring   netstring
    """
    if not isinstance(message.id, bytes) or not message.id:
        raise ValueError('{}.id must be nonempty a bytes object'.format(message))

    if not isinstance(message.data, bytes) or not message.data:
        raise ValueError('{}.data must be a nonempty bytes object'.format(message))

    return (struct.pack('<b', MessageTypes.Job.value) + 
            utils.to_netstring(message.id) + utils.to_netstring(message.data))

@serialize.register(Ping)
def _(message):
    """
    Ping messages are serialized according to the following format:

        <Type>  <ID>
        
        byte    netstring
    """
    if not isinstance(message.id, bytes) or not message.id:
        raise ValueError('{}.id must be nonempty a bytes object'.format(message))

    return struct.pack('<b', MessageTypes.Ping.value) + utils.to_netstring(message.id)

@serialize.register(SubmitResult)
def _(message):
    """
    Submission messages are serialized according to the following format:

        <Type>  <ID>        <Data>
        
        byte    netstring   netstring
    """
    if not isinstance(message.id, bytes) or not message.id:
        raise ValueError('{}.id must be nonempty a bytes object'.format(message))

    if not isinstance(message.data, bytes) or not message.data:
        raise ValueError('{}.data must be a nonempty bytes object'.format(message))

    return (struct.pack('<b', MessageTypes.SubmitResult.value) + 
            utils.to_netstring(message.id) + utils.to_netstring(message.data))

@serialize.register(Ok)
def _(message):
    """
    Ok messages are serialized according to the following format:

        <Type>
        
        byte
    """
    return struct.pack('<b', MessageTypes.Ok.value)

@serialize.register(Error)
def _(message):
    """
    Error messages are serialized according to the following format:

        <Type>  <Error>

        byte    byte
    """
    return struct.pack('<bb', MessageTypes.Error.value, message.type.value)

def message_from_bytes(bytestring):
    """
    Decodes a bytestring into a message, returning the decoded message as well
    as any bytes left over after the decoding. Note that if the given data is
    not enough to decode a complete message, than ``(None, bytestring)`` is
    returned.
    """
    # If we fail at decoding at any point, then this is what is returned
    fail_return = (None, bytestring)

    if not bytestring:
        return fail_return

    try:
        msg_type = MessageTypes(bytestring[0])
    except ValueError:
        # The message byte is invalid, and we can't do anything that will make
        # it valid in the future, since it is corrupted somehow.
        raise ValueError('Incorrect header byte in message')

    payload = bytestring[1:]
    if msg_type == MessageTypes.RequestJob:
        if len(payload) < 4:
            return fail_return

        (ttl,) = struct.unpack('<I', payload[:4])
        return RequestJob(ttl), payload[4:]
    elif msg_type == MessageTypes.Job:
        id, payload_rest = utils.netstring_from_buffer(payload)
        if not id:
            return fail_return

        data, rest = utils.netstring_from_buffer(payload_rest)
        if not data:
            return fail_return

        return Job(id, data), rest
    elif msg_type == MessageTypes.Ping:
        id, rest = utils.netstring_from_buffer(payload)
        if not id:
            return fail_return

        return Ping(id), rest
    elif msg_type == MessageTypes.SubmitResult:
        id, payload_rest = utils.netstring_from_buffer(payload)
        if not id:
            return fail_return

        data, rest = utils.netstring_from_buffer(payload_rest)
        if not data:
            return fail_return

        return SubmitResult(id, data), rest
    elif msg_type == MessageTypes.Ok:
        return Ok(), payload
    elif msg_type == MessageTypes.Error:
        if len(payload) < 1:
            return fail_return

        return Error(Errors(payload[0])), payload[1:]
