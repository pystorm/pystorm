"""
Tests for basic IPC stuff via Component class
"""

from __future__ import absolute_import, print_function, unicode_literals

# Monkey patch everything for gevent if provided
try:
    from gevent import monkey

    monkey.patch_all(sys=True)
except (AttributeError, ImportError):
    pass

import logging
import os
import sys
import time
import unittest
from io import BytesIO

import simplejson as json
import pytest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from pystorm import AsyncComponent, Component
from pystorm.exceptions import StormWentAwayError


log = logging.getLogger(__name__)


class BlockingBytesIO(BytesIO):
    """Class that will block when empty instead of returning empty string"""

    def read(self, *args, **kwargs):
        super_self = super(BlockingBytesIO, self)
        buf = super_self.read(*args, **kwargs)
        while not buf:
            time.sleep(0.001)
            buf = super_self.read(*args, **kwargs)
        return buf

    def read1(self, *args, **kwargs):
        super_self = super(BlockingBytesIO, self)
        buf = super_self.read1(*args, **kwargs)
        while not buf:
            time.sleep(0.001)
            buf = super_self.read1(*args, **kwargs)
        return buf

    def readall(self, *args, **kwargs):
        super_self = super(BlockingBytesIO, self)
        buf = super_self.readall(*args, **kwargs)
        while not buf:
            time.sleep(0.001)
            buf = super_self.readall(*args, **kwargs)
        return buf

    def readinto(self, *args, **kwargs):
        super_self = super(BlockingBytesIO, self)
        bytes_read = super_self.readinto(*args, **kwargs)
        while not bytes_read:
            time.sleep(0.001)
            bytes_read = super_self.readinto(*args, **kwargs)
        return bytes_read

    def readline(self, *args, **kwargs):
        super_self = super(BlockingBytesIO, self)
        line = super_self.readline(*args, **kwargs)
        while not line:
            time.sleep(0.001)
            line = super_self.readline(*args, **kwargs)
        return line


class ComponentTests(unittest.TestCase):
    conf = {
        "topology.message.timeout.secs": 3,
        "topology.tick.tuple.freq.secs": 1,
        "topology.debug": True,
        "topology.name": "foo",
    }
    context = {
        "task->component": {
            "1": "example-spout",
            "2": "__acker",
            "3": "example-bolt1",
            "4": "example-bolt2",
        },
        "taskid": 3,
        # Everything below this line is only available in Storm 0.11.0+
        "componentid": "example-bolt1",
        "stream->target->grouping": {"default": {"example-bolt2": {"type": "SHUFFLE"}}},
        "streams": ["default"],
        "stream->outputfields": {"default": ["word"]},
        "source->stream->grouping": {
            "example-spout": {"default": {"type": "FIELDS", "fields": ["word"]}}
        },
        "source->stream->fields": {
            "example-spout": {"default": ["sentence", "word", "number"]}
        },
    }

    def test_read_handshake(self):
        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        pid_dir = handshake_dict["pidDir"]
        expected_conf = handshake_dict["conf"]
        expected_context = handshake_dict["context"]
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        given_conf, given_context = component.read_handshake()
        pid_path = os.path.join(pid_dir, str(component.pid))
        self.assertTrue(os.path.exists(pid_path))
        os.remove(pid_path)
        self.assertEqual(given_conf, expected_conf)
        self.assertEqual(given_context, expected_context)
        self.assertEqual(
            component.serializer.serialize_dict({"pid": component.pid}).encode("utf-8"),
            component.serializer.output_stream.buffer.getvalue(),
        )

    def test_setup_component(self):
        conf = self.conf
        component = Component(input_stream=BytesIO(), output_stream=BytesIO())
        component._setup_component(conf, self.context)
        self.assertEqual(component.topology_name, conf["topology.name"])
        self.assertEqual(component.task_id, self.context["taskid"])
        self.assertEqual(
            component.component_name,
            self.context["task->component"][str(self.context["taskid"])],
        )
        self.assertEqual(component.storm_conf, conf)
        self.assertEqual(component.context, self.context)

    def test_read_message(self):
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # empty message, which should trigger sys.exit (end ignored)
            "",
            "",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2] if msg]
        outputs.append("")
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        for output in outputs:
            log.info("Checking msg for %r", output)
            if output:
                msg = component.read_message()
                self.assertEqual(output, msg)
            else:
                with self.assertRaises(StormWentAwayError):
                    component.read_message()

    def test_read_message_unicode(self):
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white \uFFE6 the seven dwarfs"'
                ', "field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # empty message, which should trigger sys.exit (end ignored)
            "",
            "",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2] if msg]
        outputs.append("")
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf8")),
            output_stream=BytesIO(),
        )
        for output in outputs:
            log.info("Checking msg for %r", output)
            if output:
                msg = component.read_message()
                self.assertEqual(output, msg)
            else:
                with self.assertRaises(StormWentAwayError):
                    component.read_message()

    def test_read_split_message(self):
        # Make sure we can read something that's broken up into many "lines"
        inputs = [
            '{ "id": "-6955786537413359385", ',
            '"comp": "1", "stream": "1"\n',
            "\n",
            ', "task": 9, "tuple": ["snow white and the seven dwarfs", ',
            '"field2", 3]}\n',
            "end\n",
        ]
        output = json.loads("".join(inputs[:-1]))

        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        msg = component.read_message()
        self.assertEqual(output, msg)

    def test_read_command(self):
        # Check that we properly queue task IDs and return only commands
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2]]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )

        # Skip first output, because it's a task ID, and won't be returned by
        # read_command
        for output in outputs[1:]:
            log.info("Checking msg for %r", output)
            msg = component.read_command()
            self.assertEqual(output, msg)
        self.assertEqual(component._pending_task_ids.pop(), outputs[0])

    def test_read_task_ids(self):
        # Check that we properly queue commands and return only task IDs
        inputs = [  # Task IDs
            "[4, 8, 15]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # Task IDs
            "[16, 23, 42]\n",
            "end\n",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2]]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )

        # Skip middle outputs, because they're commands and won't be returned by
        # read_task_ids
        for output in (outputs[0], outputs[-1]):
            log.info("Checking msg for %r", output)
            msg = component.read_task_ids()
            self.assertEqual(output, msg)
        for output in outputs[1:-1]:
            self.assertEqual(component._pending_commands.popleft(), output)

    def test_send_message(self):
        component = Component(input_stream=BytesIO(), output_stream=BytesIO())
        inputs = [
            {
                "command": "emit",
                "id": 4,
                "stream": "",
                "task": 9,
                "tuple": ["field1", 2, 3],
            },
            {"command": "log", "msg": "I am a robot monkey."},
            {"command": "next"},
            {"command": "sync"},
        ]
        for cmd in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BytesIO()
            )
            component.send_message(cmd)
            self.assertEqual(
                component.serializer.serialize_dict(cmd).encode("utf-8"),
                component.serializer.output_stream.buffer.getvalue(),
            )

        # Check that we properly skip over invalid input
        self.assertIsNone(component.send_message(["foo", "bar"]))

    def test_send_message_unicode(self):
        component = Component(input_stream=BytesIO(), output_stream=BytesIO())
        inputs = [
            {
                "command": "emit",
                "id": 4,
                "stream": "",
                "task": 9,
                "tuple": ["field\uFFE6", 2, 3],
            },
            {"command": "log", "msg": "I am a robot monkey."},
            {"command": "next"},
            {"command": "sync"},
        ]
        for cmd in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BytesIO()
            )
            component.send_message(cmd)
            self.assertEqual(
                component.serializer.serialize_dict(cmd).encode("utf-8"),
                component.serializer.output_stream.buffer.getvalue(),
            )

        # Check that we properly skip over invalid input
        self.assertIsNone(component.send_message(["foo", "bar"]))

    @patch.object(Component, "send_message", autospec=True)
    def test_log(self, send_message_mock):
        component = Component(input_stream=BytesIO(), output_stream=BytesIO())
        inputs = [
            ("I am a robot monkey.", None, 2),
            ("I am a monkey who learned to talk.", "warning", 3),
        ]
        for msg, level, storm_level in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BytesIO()
            )
            component.log(msg, level=level)
            send_message_mock.assert_called_with(
                component, {"command": "log", "msg": msg, "level": storm_level}
            )

    def test_exit_on_exception_true(self):
        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        component.exit_on_exception = True
        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
        assert raises_fixture.exception.code == 1

<<<<<<< HEAD
    @patch.object(Component, "_run", autospec=True)
=======
    @patch.object(Component, '_exit', new=lambda self, code: sys.exit(code))
    @patch.object(Component, '_run', autospec=True)
>>>>>>> Try to fix some async test failures
    def test_exit_on_exception_false(self, _run_mock):
        # Make sure _run raises an exception
        def raiser(self):  # lambdas can't raise
            raise StormWentAwayError if _run_mock.called else NotImplementedError

        _run_mock.side_effect = raiser

        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        component.exit_on_exception = False
        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
        assert raises_fixture.exception.code == 2

    @patch.object(Component, "_handle_run_exception", autospec=True)
    @patch("pystorm.component.log", autospec=True)
    def test_nested_exception(self, log_mock, _handle_run_exception_mock):
        # Make sure self._handle_run_exception raises an exception
        def raiser(self):  # lambdas can't raise
            raise Exception("Oops")

        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        component.exit_on_exception = True
        _handle_run_exception_mock.side_effect = raiser

        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
        assert log_mock.error.call_count == 2
        assert raises_fixture.exception.code == 1

    @patch.object(Component, "_handle_run_exception", autospec=True)
    @patch("pystorm.component.log", autospec=True)
    def test_nested_went_away_exception(self, log_mock, _handle_run_exception_mock):
        # Make sure self._handle_run_exception raises an exception
        def raiser(*args):  # lambdas can't raise
            raise StormWentAwayError

        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = Component(
            input_stream=BytesIO("".join(inputs).encode("utf-8")),
            output_stream=BytesIO(),
        )
        component.exit_on_exception = True
        _handle_run_exception_mock.side_effect = raiser

        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
        assert log_mock.error.call_count == 1
        assert log_mock.info.call_count == 1
        assert raises_fixture.exception.code == 2


class AsyncComponentTests(unittest.TestCase):
    conf = {
        "topology.message.timeout.secs": 3,
        "topology.tick.tuple.freq.secs": 1,
        "topology.debug": True,
        "topology.name": "foo",
    }
    context = {
        "task->component": {
            "1": "example-spout",
            "2": "__acker",
            "3": "example-bolt1",
            "4": "example-bolt2",
        },
        "taskid": 3,
        # Everything below this line is only available in Storm 0.11.0+
        "componentid": "example-bolt1",
        "stream->target->grouping": {"default": {"example-bolt2": {"type": "SHUFFLE"}}},
        "streams": ["default"],
        "stream->outputfields": {"default": ["word"]},
        "source->stream->grouping": {
            "example-spout": {"default": {"type": "FIELDS", "fields": ["word"]}}
        },
        "source->stream->fields": {
            "example-spout": {"default": ["sentence", "word", "number"]}
        },
    }

    def test_read_handshake(self):
        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        pid_dir = handshake_dict["pidDir"]
        expected_conf = handshake_dict["conf"]
        expected_context = handshake_dict["context"]
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()
        component._writer.start()
        given_conf, given_context = component.read_handshake()
        pid_path = os.path.join(pid_dir, str(component.pid))
        self.assertTrue(os.path.exists(pid_path))
        os.remove(pid_path)
        self.assertEqual(given_conf, expected_conf)
        self.assertEqual(given_context, expected_context)
<<<<<<< HEAD
        self.assertEqual(
            component.serializer.serialize_dict({"pid": component.pid}).encode("utf-8"),
            component.serializer.output_stream.buffer.getvalue(),
        )
=======
        self.assertEqual(component.serializer.serialize_dict({"pid": component.pid}).encode('utf-8'),
                         component.serializer.output_stream.buffer.getvalue())
        component.stop()
>>>>>>> Replace while True with while not self._stopped to make thread stoppable

    def test_setup_component(self):
        conf = self.conf
        component = AsyncComponent()
        component._setup_component(conf, self.context)
        self.assertEqual(component.topology_name, conf["topology.name"])
        self.assertEqual(component.task_id, self.context["taskid"])
        self.assertEqual(
            component.component_name,
            self.context["task->component"][str(self.context["taskid"])],
        )
        self.assertEqual(component.storm_conf, conf)
        self.assertEqual(component.context, self.context)

    def test_read_message(self):
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # empty message, which should trigger sys.exit (end ignored)
            "",
            "",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2] if msg]
        outputs.append("")
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()
        component._writer.start()
        for output in outputs:
            log.info("Checking msg for %r", output)
            if output:
                msg = component.read_message()
                self.assertEqual(output, msg)
            else:
                with self.assertRaises(StormWentAwayError):
                    component.read_message()
        component.stop()

    def test_read_message_unicode(self):
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white \uFFE6 the seven dwarfs"'
                ', "field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # empty message, which should trigger sys.exit (end ignored)
            "",
            "",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2] if msg]
        outputs.append("")
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()
        component._writer.start()
        for output in outputs:
            log.info("Checking msg for %r", output)
            if output:
                msg = component.read_message()
                self.assertEqual(output, msg)
            else:
                with self.assertRaises(StormWentAwayError):
                    component.read_message()
        component.stop()

    def test_read_split_message(self):
        # Make sure we can read something that's broken up into many "lines"
        inputs = [
            '{ "id": "-6955786537413359385", ',
            '"comp": "1", "stream": "1"\n',
            "\n",
            ', "task": 9, "tuple": ["snow white and the seven dwarfs", ',
            '"field2", 3]}\n',
            "end\n",
        ]
        output = json.loads("".join(inputs[:-1]))

        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()
        msg = component.read_message()
        self.assertEqual(output, msg)
        component.stop()

    def test_read_command(self):
        # Check that we properly queue task IDs and return only commands
        inputs = [  # Task IDs
            "[12, 22, 24]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2]]
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()

        # Skip first output, because it's a task ID, and won't be returned by
        # read_command
        for output in outputs[1:]:
            log.info("Checking msg for %r", output)
            msg = component.read_command()
            self.assertEqual(output, msg)
        self.assertEqual(component._pending_task_ids.get(), outputs[0])
        component.stop()

    def test_read_task_ids(self):
        # Check that we properly queue commands and return only task IDs
        inputs = [  # Task IDs
            "[4, 8, 15]\n",
            "end\n",
            # Incoming Tuple for bolt
            (
                '{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                '"field2", 3]}\n'
            ),
            "end\n",
            # next command for spout
            '{"command": "next"}\n',
            "end\n",
            # Task IDs
            "[16, 23, 42]\n",
            "end\n",
        ]
        outputs = [json.loads(msg) for msg in inputs[::2]]
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component._reader.start()

        # Skip middle outputs, because they're commands and won't be returned by
        # read_task_ids
        for output in (outputs[0], outputs[-1]):
            log.info("Checking msg for %r", output)
            msg = component.read_task_ids()
            self.assertEqual(output, msg)
        for output in outputs[1:-1]:
            self.assertEqual(component._pending_commands.get(), output)
        component.stop()

    def test_send_message(self):
        component = AsyncComponent(
            input_stream=BlockingBytesIO(), output_stream=BlockingBytesIO()
        )
        component._reader.start()
        component._writer.start()
        inputs = [
            {
                "command": "emit",
                "id": 4,
                "stream": "",
                "task": 9,
                "tuple": ["field1", 2, 3],
            },
            {"command": "log", "msg": "I am a robot monkey."},
            {"command": "next"},
            {"command": "sync"},
        ]
        for cmd in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BlockingBytesIO()
            )
            component.send_message(cmd)
            self.assertEqual(
                component.serializer.serialize_dict(cmd).encode("utf-8"),
                component.serializer.output_stream.buffer.getvalue(),
            )

        # Check that we properly skip over invalid input
<<<<<<< HEAD
        self.assertIsNone(component.send_message(["foo", "bar"]))
=======
        self.assertIsNone(component.send_message(['foo', 'bar']))
        component.stop()
>>>>>>> Replace while True with while not self._stopped to make thread stoppable

    def test_send_message_unicode(self):
        component = AsyncComponent(
            input_stream=BlockingBytesIO(), output_stream=BlockingBytesIO()
        )
        component._reader.start()
        component._writer.start()
        inputs = [
            {
                "command": "emit",
                "id": 4,
                "stream": "",
                "task": 9,
                "tuple": ["field\uFFE6", 2, 3],
            },
            {"command": "log", "msg": "I am a robot monkey."},
            {"command": "next"},
            {"command": "sync"},
        ]
        for cmd in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BlockingBytesIO()
            )
            component.send_message(cmd)
            self.assertEqual(
                component.serializer.serialize_dict(cmd).encode("utf-8"),
                component.serializer.output_stream.buffer.getvalue(),
            )

        # Check that we properly skip over invalid input
        self.assertIsNone(component.send_message(['foo', 'bar']))
        component.stop()

    @patch.object(AsyncComponent, "send_message", autospec=True)
    def test_log(self, send_message_mock):
        component = AsyncComponent(
            input_stream=BlockingBytesIO(), output_stream=BlockingBytesIO()
        )
        component._reader.start()
        component._writer.start()
        inputs = [
            ("I am a robot monkey.", None, 2),
            ("I am a monkey who learned to talk.", "warning", 3),
        ]
        for msg, level, storm_level in inputs:
            component.serializer.output_stream.close()
            component.serializer.output_stream = component.serializer._wrap_stream(
                BlockingBytesIO()
            )
            component.log(msg, level=level)
            send_message_mock.assert_called_with(component, {'command': 'log',
                                                             'msg': msg,
                                                             'level': storm_level})
        component.stop()

    @patch.object(Component, '_exit', new=lambda self, code: sys.exit(code))
    def test_exit_on_exception_true(self):
        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
            assert raises_fixture.exception.value == 1
        component.stop()

    @patch.object(AsyncComponent, '_run', autospec=True)
    @patch.object(Component, '_exit', new=lambda self, code: sys.exit(code))
    def test_exit_on_exception_false(self, _run_mock):
        # Make sure _run raises an exception
        def raiser(self):  # lambdas can't raise
            raise StormWentAwayError if _run_mock.called else NotImplementedError

        _run_mock.side_effect = raiser

        handshake_dict = {"conf": self.conf, "pidDir": ".", "context": self.context}
        inputs = ["{}\n".format(json.dumps(handshake_dict)), "end\n"]
        component = AsyncComponent(
            input_stream=BlockingBytesIO("".join(inputs).encode("utf-8")),
            output_stream=BlockingBytesIO(),
        )
        component.exit_on_exception = False
        with self.assertRaises(SystemExit) as raises_fixture:
            component.run()
            assert raises_fixture.exception.value == 2
        component.stop()


if __name__ == "__main__":
    unittest.main()
