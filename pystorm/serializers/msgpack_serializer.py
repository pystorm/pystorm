"""Messagepack implementation of pystorm serializer"""

from __future__ import absolute_import, print_function, unicode_literals

import io
import os

import msgpack

from ..exceptions import StormWentAwayError
from .serializer import Serializer


class MsgpackSerializer(Serializer):

    CHUNK_SIZE = 1024 ** 2

    def __init__(self, input_stream, output_stream, reader_lock, writer_lock):
        super(MsgpackSerializer, self).__init__(input_stream,
                                                self._raw_stream(output_stream),
                                                reader_lock, writer_lock)
        self._messages = self._messages_generator()

    @staticmethod
    def _raw_stream(stream):
        """Returns the raw buffer used by stream, so we can write bytes."""
        if hasattr(stream, 'buffer'):
            return stream.buffer
        else:
            return stream

    def _messages_generator(self):
        unpacker = msgpack.Unpacker()
        while True:
            # f.read(n) on sys.stdin blocks until n bytes are read, causing
            # serializer to hang.
            # os.read(fileno, n) will block if there is nothing to read, but will
            # return as soon as it is able to read at most n bytes.
            with self._reader_lock:
                try:
                    line = os.read(self.input_stream.fileno(), self.CHUNK_SIZE)
                except io.UnsupportedOperation:
                    line = self.input_stream.read(self.CHUNK_SIZE)
            if not line:
                # Handle EOF, which usually means Storm went away
                raise StormWentAwayError()
            # As python-msgpack docs suggest, we feed data to the unpacker
            # internal buffer in order to let the unpacker deal with message
            # boundaries recognition and uncomplete messages. In case input ends
            # with a partial message, unpacker raises a StopIteration and will be
            # able to continue after being feeded with the rest of the message.
            unpacker.feed(line)
            for i in unpacker:
                yield i

    def read_message(self):
        """"Messages are delimited by msgpack itself, no need for Storm
        multilang end line.
        """
        return next(self._messages)

    def serialize_dict(self, msg_dict):
        """"Messages are delimited by msgpack itself, no need for Storm
        multilang end line.
        """
        # TODO: Determine if we need use_bin_type here
        return msgpack.packb(msg_dict)
