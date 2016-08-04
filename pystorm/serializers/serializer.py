"""Base class for all serialziers used by Storm component. Please note that for
each serializer a Java counterpart needs to exist.
"""

from __future__ import absolute_import, print_function, unicode_literals

import logging

from ..exceptions import StormWentAwayError


log = logging.getLogger(__name__)


class Serializer(object):

    def __init__(self, input_stream, output_stream, reader_lock, writer_lock):
        self._reader_lock = reader_lock
        self._writer_lock = writer_lock
        self.input_stream = input_stream
        self.output_stream = output_stream

    def read_message(self):
        """Return the dictionary message received on the input stream.
        raises: StormWentAwayError if EOF is reached."""
        raise NotImplementedError

    def send_message(self, msg_dict):
        """Serialize a message dictionary and write it to the output stream."""
        with self._writer_lock:
            try:
                self.output_stream.flush()
                self.output_stream.write(self.serialize_dict(msg_dict))
                self.output_stream.flush()
            except IOError:
                raise StormWentAwayError()
            except:
                log.exception('Failed to send message: %r', msg_dict)

    def serialize_dict(self, msg_dict):
        """Convert a message dictionary to bytes.  Used by send_message"""
        raise NotImplementedError
