"""JSON implementation of pystorm serializer"""

from __future__ import absolute_import, print_function, unicode_literals

import io
import logging

import simplejson as json
from six import PY2

from ..exceptions import StormWentAwayError
from .serializer import Serializer


log = logging.getLogger(__name__)


class JSONSerializer(Serializer):

    def __init__(self, input_stream, output_stream, reader_lock, writer_lock):
        super(JSONSerializer, self).__init__(input_stream, output_stream,
                                             reader_lock, writer_lock)
        self.input_stream = self._wrap_stream(input_stream)
        self.output_stream = self._wrap_stream(output_stream)

    @staticmethod
    def _wrap_stream(stream):
        """Returns a TextIOWrapper around the given stream that handles UTF-8
        encoding/decoding.
        """
        if hasattr(stream, 'buffer'):
            return io.TextIOWrapper(stream.buffer, encoding='utf-8')
        elif hasattr(stream, 'readable'):
            return io.TextIOWrapper(stream, encoding='utf-8')
        # Python 2.x stdin and stdout are just files
        else:
            return io.open(stream.fileno(), mode=stream.mode, encoding='utf-8')

    def read_message(self):
        """The Storm multilang protocol consists of JSON messages followed by
        a newline and "end\n".

        All of Storm's messages (for either bolts or spouts) should be of the
        form::

            '<command or task_id form prior emit>\\nend\\n'

        Command example, an incoming Tuple to a bolt::

            '{ "id": "-6955786537413359385",  "comp": "1", "stream": "1", "task": 9, "tuple": ["snow white and the seven dwarfs", "field2", 3]}\\nend\\n'

        Command example for a spout to emit its next Tuple::

            '{"command": "next"}\\nend\\n'

        Example, the task IDs a prior emit was sent to::

            '[12, 22, 24]\\nend\\n'

        The edge case of where we read ``''`` from ``input_stream`` indicating
        EOF, usually means that communication with the supervisor has been
        severed.
        """
        msg = ""
        num_blank_lines = 0
        while True:
            # readline will return trailing \n so that output is unambigious, we
            # should only have line == '' if we're at EOF
            with self._reader_lock:
                line = self.input_stream.readline()
            if line == 'end\n':
                break
            elif line == '':
                raise StormWentAwayError()
            elif line == '\n':
                num_blank_lines += 1
                if num_blank_lines % 1000 == 0:
                    log.warn("While trying to read a command or pending task "
                             "ID, Storm has instead sent %s '\\n' messages.",
                             num_blank_lines)
                continue

            msg = '{}{}\n'.format(msg, line[0:-1])

        try:
            return json.loads(msg)
        except Exception:
            log.error("JSON decode error for message: %r", msg, exc_info=True)
            raise

    def serialize_dict(self, msg_dict):
        """Serialize to JSON a message dictionary."""
        serialized = json.dumps(msg_dict, namedtuple_as_object=False)
        if PY2:
            serialized = serialized.decode('utf-8')
        serialized = '{}\nend\n'.format(serialized)
        return serialized
