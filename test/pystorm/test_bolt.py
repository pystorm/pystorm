"""
Tests for Bolt and its subclasses
"""

from __future__ import absolute_import, print_function, unicode_literals

import logging
import unittest
from collections import namedtuple
from io import BytesIO

import simplejson as json

try:
    from unittest import mock
    from unittest.mock import patch
except ImportError:
    import mock
    from mock import patch

from pystorm import BatchingBolt, Bolt, Tuple
from pystorm.exceptions import StormWentAwayError


log = logging.getLogger(__name__)


class BoltTests(unittest.TestCase):

    def setUp(self):
        self.conf = {"topology.message.timeout.secs": 3,
                     "topology.tick.tuple.freq.secs": 1,
                     "topology.debug": True,
                     "topology.name": "foo"}
        self.context = {
            "task->component": {
                "1": "example-spout",
                "2": "__acker",
                "3": "example-bolt1",
                "4": "example-bolt2"
            },
            "taskid": 3,
            # Everything below this line is only available in Storm 0.11.0+
            "componentid": "example-bolt1",
            "stream->target->grouping": {
                "default": {
                    "example-bolt2": {
                        "type": "SHUFFLE"
                    }
                }
            },
            "streams": ["default"],
            "stream->outputfields": {"default": ["word"]},
            "source->stream->grouping": {
                "example-spout": {
                    "default": {
                        "type": "FIELDS",
                        "fields": ["word"]
                    }
                }
            },
            "source->stream->fields": {
                "example-spout": {
                    "default": ["sentence", "word", "number"]
                }
            }
        }
        self.tup_dict = {'id': 14,
                         'comp': 'some_spout',
                         'stream': 'default',
                         'task': 'some_bolt',
                         'tuple': [1, 2, 3]}
        tup_json = "{}\nend\n".format(json.dumps(self.tup_dict)).encode('utf-8')
        self.tup = Tuple(self.tup_dict['id'], self.tup_dict['comp'],
                         self.tup_dict['stream'], self.tup_dict['task'],
                         tuple(self.tup_dict['tuple']),)
        self.bolt = Bolt(input_stream=BytesIO(tup_json),
                         output_stream=BytesIO())
        self.bolt.initialize(self.conf, self.context)

    def test_setup_component(self):
        conf = self.conf
        self.bolt._setup_component(conf, self.context)
        self.assertEqual(self.bolt._source_tuple_types['example-spout']['default'].__name__,
                         'Example_SpoutDefaultTuple')

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_emit_basic(self, send_message_mock):
        # A basic emit
        self.bolt.emit([1, 2, 3], need_task_ids=False)
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'anchors': [],
                                                         'tuple': [1, 2, 3],
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_emit_stream_anchors(self, send_message_mock):
        # Emit with stream and anchors
        self.bolt.emit([1, 2, 3], stream='foo', anchors=[4, 5],
                       need_task_ids=False)
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'stream': 'foo',
                                                         'anchors': [4, 5],
                                                         'tuple': [1, 2, 3],
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_emit_direct(self, send_message_mock):
        # Emit as a direct task
        self.bolt.emit([1, 2, 3], direct_task='other_bolt')
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'anchors': [],
                                                         'tuple': [1, 2, 3],
                                                         'task': 'other_bolt',
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_ack_id(self, send_message_mock):
        # ack an ID
        self.bolt.ack(42)
        send_message_mock.assert_called_with(self.bolt, {'command': 'ack',
                                                         'id': 42})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_ack_tuple(self, send_message_mock):
        # ack a Tuple
        self.bolt.ack(self.tup)
        send_message_mock.assert_called_with(self.bolt, {'command': 'ack',
                                                         'id': 14})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_fail_id(self, send_message_mock):
        # fail an ID
        self.bolt.fail(42)
        send_message_mock.assert_called_with(self.bolt, {'command': 'fail',
                                                         'id': 42})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_fail_tuple(self, send_message_mock):
        # fail a Tuple
        self.bolt.ack(self.tup)
        send_message_mock.assert_called_with(self.bolt, {'command': 'ack',
                                                         'id': 14})

    @patch.object(Bolt, 'process', autospec=True)
    @patch.object(Bolt, 'ack', autospec=True)
    def test_run(self, ack_mock, process_mock):
        self.bolt._run()
        process_mock.assert_called_with(self.bolt, self.tup)
        self.assertListEqual(self.bolt._current_tups, [])

    @patch.object(Bolt, 'process', autospec=True)
    @patch.object(Bolt, 'ack', autospec=True)
    def test_auto_ack_on(self, ack_mock, process_mock):
        # test auto-ack on (the default)
        self.bolt._run()
        ack_mock.assert_called_with(self.bolt, self.tup)
        self.assertEqual(ack_mock.call_count, 1)

    @patch.object(Bolt, 'process', autospec=True)
    @patch.object(Bolt, 'ack', autospec=True)
    def test_auto_ack_off(self, ack_mock, process_mock):
        self.bolt.auto_ack = False
        self.bolt._run()
        # Assert that this wasn't called, and print out what it was called with
        # otherwise.
        self.assertListEqual(ack_mock.call_args_list, [])

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_auto_anchor_on(self, send_message_mock):
        self.bolt._current_tups = [self.tup]
        # Test auto-anchor on (the default)
        self.bolt.emit([1, 2, 3], need_task_ids=False)
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'anchors': [14],
                                                         'tuple': [1, 2, 3],
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_auto_anchor_off(self, send_message_mock):
        # Test auto-anchor off
        self.bolt.auto_anchor = False
        self.bolt.emit([1, 2, 3], need_task_ids=False)
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'anchors': [],
                                                         'tuple': [1, 2, 3],
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'send_message', autospec=True)
    def test_auto_anchor_override(self, send_message_mock):
        # Test overriding auto-anchor
        self.bolt.auto_anchor = True
        self.bolt.emit([1, 2, 3], anchors=[42], need_task_ids=False)
        send_message_mock.assert_called_with(self.bolt, {'command': 'emit',
                                                         'anchors': [42],
                                                         'tuple': [1, 2, 3],
                                                         'need_task_ids': False})

    @patch.object(Bolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(Bolt, 'fail', autospec=True)
    @patch.object(Bolt, '_run', autospec=True)
    def test_auto_fail_on(self, _run_mock, fail_mock):
        self.bolt._current_tups = [self.tup]
        # Make sure _run raises an exception
        def raiser(): # lambdas can't raise
            raise Exception('borkt')
        _run_mock.side_effect = raiser

        # test auto-fail on (the default)
        with self.assertRaises(SystemExit):
            self.bolt.run()
        fail_mock.assert_called_with(self.bolt, self.tup)
        self.assertEqual(fail_mock.call_count, 1)

    @patch.object(Bolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(Bolt, 'raise_exception', new=lambda *a: None)
    @patch.object(Bolt, 'fail', autospec=True)
    @patch.object(Bolt, '_run', autospec=True)
    def test_auto_fail_off(self, _run_mock, fail_mock):
        self.bolt._current_tups = [self.tup]
        # Make sure _run raises an exception
        def raiser(): # lambdas can't raise
            log.info('Raised borkt')
            raise Exception('borkt')
        _run_mock.side_effect = raiser

        # test auto-fail off
        self.bolt.auto_fail = False
        with self.assertRaises(SystemExit):
            self.bolt.run()
        # Assert that this wasn't called, and print out what it was called with
        # otherwise.
        self.assertListEqual(fail_mock.call_args_list, [])

    @patch.object(Bolt, 'read_tuple', autospec=True)
    @patch.object(Bolt, 'send_message', autospec=True)
    def test_heartbeat_response(self, send_message_mock, read_tuple_mock):
        # Make sure we send sync for heartbeats
        read_tuple_mock.return_value = Tuple(id='foo', task=-1,
                                             stream='__heartbeat', values=(),
                                             component='__system')
        self.bolt._run()
        send_message_mock.assert_called_with(self.bolt, {'command': 'sync'})

    @patch.object(Bolt, 'read_tuple', autospec=True)
    @patch.object(Bolt, 'process_tick', autospec=True)
    def test_process_tick(self, process_tick_mock, read_tuple_mock):
        # Make sure we send sync for heartbeats
        read_tuple_mock.return_value = Tuple(id=None, task=-1,
                                             component='__system',
                                             stream='__tick', values=(50,))
        self.bolt._run()
        process_tick_mock.assert_called_with(self.bolt,
                                             read_tuple_mock.return_value)

    def test_read_tuple(self):
        inputs = [# Tuple with all values
                  ('{ "id": "-6955786537413359385", "comp": "1", "stream": "1"'
                   ', "task": 9, "tuple": ["snow white and the seven dwarfs", '
                   '"field2", 3]}\n'), 'end\n',
                  # Tick Tuple
                  ('{ "id": null, "task": -1, "comp": "__system", "stream": '
                   '"__tick", "tuple": [50]}\n'), 'end\n',
                  # Heartbeat Tuple
                  ('{ "id": null, "task": -1, "comp": "__system", "stream": '
                   '"__heartbeat", "tuple": []}\n'), 'end\n',
                  ]
        outputs = []
        for msg in inputs[::2]:
            output = json.loads(msg)
            output['component'] = output['comp']
            output['values'] = tuple(output['tuple'])
            del output['comp']
            del output['tuple']
            outputs.append(Tuple(**output))

        self.bolt = Bolt(input_stream=BytesIO(''.join(inputs).encode('utf-8')),
                         output_stream=BytesIO())

        for output in outputs:
            log.info('Checking Tuple for %r', output)
            tup = self.bolt.read_tuple()
            self.assertEqual(output, tup)

    def test_read_tuple_named_fields(self):
        inputs = [('{ "id": "-6955786537413359385", "comp": "example-spout", '
                   '"stream": "default", "task": 9, "tuple": ["snow white and '
                   'the seven dwarfs", "field2", 3]}\n'), 'end\n']

        Example_SpoutDefaultTuple = namedtuple('Example_SpoutDefaultTuple',
                                               field_names=['sentence', 'word',
                                                            'number'])

        self.bolt = Bolt(input_stream=BytesIO(''.join(inputs).encode('utf-8')),
                         output_stream=BytesIO())
        self.bolt._setup_component(self.conf, self.context)

        outputs = []
        for msg in inputs[::2]:
            output = json.loads(msg)
            output['component'] = output['comp']
            output['values'] = Example_SpoutDefaultTuple(*output['tuple'])
            del output['comp']
            del output['tuple']
            outputs.append(Tuple(**output))


        for output in outputs:
            log.info('Checking Tuple for %r', output)
            tup = self.bolt.read_tuple()
            self.assertEqual(output.values.sentence, tup.values.sentence)
            self.assertEqual(output.values.word, tup.values.word)
            self.assertEqual(output.values.number, tup.values.number)
            self.assertEqual(output, tup)


class BatchingBoltTests(unittest.TestCase):

    def setUp(self):
        self.ticks_between_batches = 1
        self.tup_dicts = [{'id': 14,
                           'comp': 'some_spout',
                           'stream': 'default',
                           'task': 'some_bolt',
                           'tuple': [1, 2, 3]},
                          {'id': 15,
                           'comp': 'some_spout',
                           'stream': 'default',
                           'task': 'some_bolt',
                           'tuple': [4, 5, 6]},
                          {'id': None,
                           'comp': '__system',
                           'stream': '__tick',
                           'task': -1,
                           'tuple': [1]},
                          {'id': 16,
                           'comp': 'some_spout',
                           'stream': 'default',
                           'task': 'some_bolt',
                           'tuple': [7, 8, 9]},
                          {'id': None,
                           'comp': '__system',
                           'stream': '__tick',
                           'task': -1,
                           'tuple': [2]}]
        tups_json = '\nend\n'.join([json.dumps(tup_dict) for tup_dict in
                                    self.tup_dicts] + [''])
        self.tups = [Tuple(tup_dict['id'], tup_dict['comp'], tup_dict['stream'],
                           tup_dict['task'], tuple(tup_dict['tuple']))
                     for tup_dict in self.tup_dicts]
        self.nontick_tups = [tup for tup in self.tups if tup.stream != '__tick']
        self.bolt = BatchingBolt(input_stream=BytesIO(tups_json.encode('utf-8')),
                                 output_stream=BytesIO())
        self.bolt.initialize({}, {})

    @patch.object(BatchingBolt, 'process_batch', autospec=True)
    def test_batching(self, process_batch_mock):
        # Add a bunch of Tuples
        for __ in self.tups:
            self.bolt._run()

        process_batch_mock.assert_called_with(self.bolt, None,
                                              self.nontick_tups)

    @patch.object(BatchingBolt, 'process_batch', autospec=True)
    def test_group_key(self, process_batch_mock):
        # Change the group key to even/odd grouping
        self.bolt.group_key = lambda t: sum(t.values) % 2

        # Add a bunch of Tuples
        for __ in self.tups:
            self.bolt._run()

        process_batch_mock.assert_has_calls([mock.call(self.bolt, 0,
                                                       [self.nontick_tups[0],
                                                        self.nontick_tups[2]]),
                                             mock.call(self.bolt, 1,
                                                       [self.nontick_tups[1]])],
                                            any_order=True)

    @patch.object(BatchingBolt, 'ack', autospec=True)
    @patch.object(BatchingBolt, 'process_batch', new=lambda *args: None)
    def test_auto_ack_on(self, ack_mock):
        # Test auto-ack on (the default)
        for __ in self.tups:
            self.bolt._run()
        ack_mock.assert_has_calls([mock.call(self.bolt, tup)
                                   for tup in self.tups],
                                  any_order=True)
        self.assertEqual(ack_mock.call_count, 5)

    @patch.object(BatchingBolt, 'ack', autospec=True)
    @patch.object(BatchingBolt, 'process_batch', new=lambda *args: None)
    def test_auto_ack_off(self, ack_mock):
        # Test auto-ack off
        self.bolt.auto_ack = False
        for __ in self.tups:
            self.bolt._run()
        # Assert that this wasn't called, and print out what it was called with
        # otherwise.
        ack_mock.assert_has_calls([mock.call(self.bolt, tup)
                                   for tup in self.tups
                                   if self.bolt.is_tick(tup)],
                                  any_order=True)
        self.assertEqual(ack_mock.call_count, 2)

    @patch.object(BatchingBolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(BatchingBolt, 'raise_exception', new=lambda *a: None)
    @patch.object(BatchingBolt, 'fail', autospec=True)
    def test_auto_fail_on(self, fail_mock):
        # Need to re-register signal handler with mocked version, because
        # mock gets created after handler was originally registered.
        self.setUp()
        # Test auto-fail on (the default)
        with self.assertRaises(SystemExit):
            self.bolt.run()

        # All waiting Tuples should have failed at this point
        fail_mock.assert_has_calls([mock.call(self.bolt, self.nontick_tups[0]),
                                    mock.call(self.bolt, self.nontick_tups[1]),
                                    mock.call(self.bolt, self.nontick_tups[2])],
                                   any_order=True)
        self.assertEqual(fail_mock.call_count, 3)

    @patch.object(BatchingBolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(BatchingBolt, 'raise_exception', new=lambda *a: None)
    @patch.object(BatchingBolt, 'fail', autospec=True)
    def test_auto_fail_off(self, fail_mock):
        # Need to re-register signal handler with mocked version, because
        # mock gets created after handler was originally registered.
        self.setUp()
        # Test auto-fail off
        self.bolt.auto_fail = False
        with self.assertRaises(SystemExit):
            self.bolt.run()

        # All waiting Tuples should have failed at this point
        self.assertListEqual(fail_mock.call_args_list, [])

    @patch.object(BatchingBolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(BatchingBolt, 'process_batch', autospec=True)
    @patch.object(BatchingBolt, 'fail', autospec=True)
    def test_auto_fail_partial_exit_on_exception_true(self, fail_mock, process_batch_mock):
        # Need to re-register signal handler with mocked version, because
        # mock gets created after handler was originally registered.
        self.setUp()
        # Change the group key just be the sum of values, which makes 3 separate
        # batches
        self.bolt.group_key = lambda t: sum(t.values)
        self.bolt.exit_on_exception = True
        # Make sure we fail on the second batch
        work = {'status': True} # to avoid scoping problems
        def work_once(*args):
            if work['status']:
                work['status'] = False
            else:
                raise Exception('borkt')
        process_batch_mock.side_effect = work_once
        # Run the batches
        with self.assertRaises(SystemExit):
            self.bolt.run()
        self.assertEqual(process_batch_mock.call_count, 2)
        # Only some Tuples should have failed at this point. The key is that
        # all un-acked Tuples should be failed, even for batches we haven't
        # started processing yet.
        self.assertEqual(fail_mock.call_count, 2)

    @patch.object(BatchingBolt, 'read_handshake', new=lambda x: ({}, {}))
    @patch.object(BatchingBolt, 'process_batch', autospec=True)
    @patch.object(BatchingBolt, 'fail', autospec=True)
    def test_auto_fail_partial_exit_on_exception_false(self, fail_mock,
                                                       process_batch_mock):
        # Need to re-register signal handler with mocked version, because
        # mock gets created after handler was originally registered.
        self.setUp()
        # Change the group key just be the sum of values, which makes 3 separate
        # batches
        self.bolt.group_key = lambda t: sum(t.values)
        self.bolt.exit_on_exception = False
        # Make sure we fail on the second batch
        work = {'status': True, 'raised': False} # to avoid scoping problems
        def work_once(*args):
            if work['status']:
                work['status'] = False
            else:
                raise Exception('borkt')
        process_batch_mock.side_effect = work_once
        # Run the batches
        with self.assertRaises(SystemExit) as raises_fixture:
            self.bolt.run()
        assert raises_fixture.exception.code == 2
        self.assertEqual(process_batch_mock.call_count, 2)
        # Only Tuples in the current batch should have failed at this point.
        self.assertEqual(fail_mock.call_count, 1)

    @patch.object(BatchingBolt, 'read_tuple', autospec=True)
    @patch.object(BatchingBolt, 'send_message', autospec=True)
    def test_heartbeat_response(self, send_message_mock, read_tuple_mock):
        # Make sure we send sync for heartbeats
        read_tuple_mock.return_value = Tuple(id='foo', task=-1,
                                             stream='__heartbeat', values=(),
                                             component='__system')
        self.bolt._run()
        send_message_mock.assert_called_with(self.bolt, {'command': 'sync'})

    @patch.object(BatchingBolt, 'read_tuple', autospec=True)
    @patch.object(BatchingBolt, 'process_tick', autospec=True)
    def test_process_tick(self, process_tick_mock, read_tuple_mock):
        # Make sure we send sync for heartbeats
        read_tuple_mock.return_value = Tuple(id=None, task=-1,
                                             component='__system',
                                             stream='__tick', values=(50,))
        self.bolt._run()
        process_tick_mock.assert_called_with(self.bolt,
                                             read_tuple_mock.return_value)


if __name__ == '__main__':
    unittest.main()
