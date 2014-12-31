"""
These tests ensure that different messages sent over the wire come back 
correctly after being serialized and unserialized.
"""
import unittest

from jqueue import protocol

class TestProtocol(unittest.TestCase):
    def roundtrip(self, message):
        msg_bytes = protocol.serialize(message)
        out_message, buffer = protocol.message_from_bytes(msg_bytes)
        self.assertEqual(buffer, b'')
        self.assertEqual(out_message, message)

    def test_request_job(self):
        TTL = 30
        self.roundtrip(protocol.RequestJob(TTL))

    def test_job_reply(self):
        ID = b'42'
        CONTENT = b'this is a sample job'
        self.roundtrip(protocol.Job(ID, CONTENT))

    def test_ping(self):
        ID = b'42'
        self.roundtrip(protocol.Ping(ID))

    def test_submit_resut(self):
        ID = b'42'
        CONTENT = b'this is a sample result'
        self.roundtrip(protocol.SubmitResult(ID, CONTENT))

    def test_ok(self):
        self.roundtrip(protocol.Ok())

    def test_error(self):
        ERRORS = {protocol.Errors.NO_JOB_AVAIL, protocol.Errors.TTL_TOO_HIGH,
            protocol.Errors.NOT_RESERVED, protocol.Errors.DO_NOT_UNDERSTAND}

        for err in ERRORS:
            self.roundtrip(protocol.Error(err))

if __name__ == '__main__':
    unittest.main()
