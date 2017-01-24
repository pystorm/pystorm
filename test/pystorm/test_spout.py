"""
Tests for Spout class
"""

from __future__ import absolute_import, print_function, unicode_literals

import logging
import unittest
from io import BytesIO

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from pystorm import ReliableSpout, Spout, Tuple


class SpoutTests(unittest.TestCase):

    def setUp(self):
        self.tup_dict = {'id': 14,
                         'comp': 'some_spout',
                         'stream': 'default',
                         'task': 'some_spout',
                         'tuple': [1, 2, 3]}
        self.tup = Tuple(self.tup_dict['id'], self.tup_dict['comp'],
                         self.tup_dict['stream'], self.tup_dict['task'],
                         self.tup_dict['tuple'],)
        self.spout = Spout(input_stream=BytesIO(),
                           output_stream=BytesIO())
        self.spout.initialize({}, {})
        self.spout.logger = logging.getLogger(__name__)

    @patch.object(Spout, 'send_message', autospec=True)
    def test_emit(self, send_message_mock):
        # A basic emit
        self.spout.emit([1, 2, 3], need_task_ids=False)
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'need_task_ids': False})

        # Emit as a direct task
        self.spout.emit([1, 2, 3], direct_task='other_spout')
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'task': 'other_spout',
                                                          'need_task_ids': False})
        # Reliable emit
        self.spout.emit([1, 2, 3], tup_id='foo', need_task_ids=False)
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'need_task_ids': False,
                                                          'id': 'foo'})

        # Reliable emit as direct task
        self.spout.emit([1, 2, 3], tup_id='foo', direct_task='other_spout')
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'task': 'other_spout',
                                                          'id': 'foo',
                                                          'need_task_ids': False})

    @patch.object(Spout, 'read_command', autospec=True,
                  return_value={'command': 'ack', 'id': 1234})
    @patch.object(Spout, 'ack', autospec=True)
    def test_ack(self, ack_mock, read_command_mock):
        # Make sure ack gets called
        self.spout._run()
        read_command_mock.assert_called_with(self.spout)
        ack_mock.assert_called_with(self.spout, 1234)

    @patch.object(Spout, 'read_command', autospec=True,
                  return_value={'command': 'fail', 'id': 1234})
    @patch.object(Spout, 'fail', autospec=True)
    def test_fail(self, fail_mock, read_command_mock):
        # Make sure fail gets called
        self.spout._run()
        read_command_mock.assert_called_with(self.spout)
        fail_mock.assert_called_with(self.spout, 1234)

    @patch.object(Spout, 'read_command', autospec=True,
                  return_value={'command': 'next', 'id': 1234})
    @patch.object(Spout, 'next_tuple', autospec=True)
    def test_next_tuple(self, next_tuple_mock, read_command_mock):
        self.spout._run()
        read_command_mock.assert_called_with(self.spout)
        self.assertEqual(next_tuple_mock.call_count, 1)

    @patch.object(Spout, 'read_command', autospec=True,
                  return_value={'command': 'activate', 'id': 1234})
    @patch.object(Spout, 'activate', autospec=True)
    def test_activate(self, activate_mock, read_command_mock):
        self.spout._run()
        read_command_mock.assert_called_with(self.spout)
        self.assertEqual(activate_mock.call_count, 1)

    @patch.object(Spout, 'read_command', autospec=True,
                  return_value={'command': 'deactivate', 'id': 1234})
    @patch.object(Spout, 'deactivate', autospec=True)
    def test_deactivate(self, deactivate_mock, read_command_mock):
        self.spout._run()
        read_command_mock.assert_called_with(self.spout)
        self.assertEqual(deactivate_mock.call_count, 1)


class ReliableSpoutTests(unittest.TestCase):

    def setUp(self):
        self.tup_dict = {'id': 14,
                         'comp': 'some_spout',
                         'stream': 'default',
                         'task': 'some_spout',
                         'tuple': [1, 2, 3]}
        self.tup = Tuple(self.tup_dict['id'], self.tup_dict['comp'],
                         self.tup_dict['stream'], self.tup_dict['task'],
                         self.tup_dict['tuple'],)
        self.spout = ReliableSpout(input_stream=BytesIO(),
                                   output_stream=BytesIO())
        self.spout.initialize({}, {})
        self.spout.logger = logging.getLogger(__name__)

    @patch.object(ReliableSpout, 'send_message', autospec=True)
    def test_emit_unreliable(self, send_message_mock):
        # An unreliable emit is not allowed with ReliableSpout
        with self.assertRaises(ValueError):
            self.spout.emit([1, 2, 3], need_task_ids=False)

    @patch.object(ReliableSpout, 'send_message', autospec=True)
    def test_emit_reliable(self, send_message_mock):
        # Reliable emit
        self.spout.emit([1, 2, 3], tup_id='foo', need_task_ids=False)
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'need_task_ids': False,
                                                          'id': 'foo'})
        self.assertEqual(self.spout.unacked_tuples.get('foo'),
                         ([1, 2, 3], None, None, False))

    @patch.object(ReliableSpout, 'send_message', autospec=True)
    def test_emit_reliable_direct(self, send_message_mock):
        # Reliable emit as direct task
        self.spout.emit([1, 2, 3], tup_id='foo', direct_task='other_spout')
        send_message_mock.assert_called_with(self.spout, {'command': 'emit',
                                                          'tuple': [1, 2, 3],
                                                          'task': 'other_spout',
                                                          'id': 'foo',
                                                          'need_task_ids': False})
        self.assertEqual(self.spout.unacked_tuples.get('foo'),
                         ([1, 2, 3], None, 'other_spout', False))

    def test_ack(self):
        self.spout.failed_tuples['foo'] = 2
        self.spout.unacked_tuples['foo'] = ([1, 2, 3], None, None, True)
        # Make sure ack cleans up failed_tuples and unacked_tuples
        self.spout.ack('foo')
        self.assertNotIn('foo', self.spout.failed_tuples)
        self.assertNotIn('foo', self.spout.unacked_tuples)

    @patch.object(ReliableSpout, 'emit', autospec=True)
    def test_fail_below_limit(self, emit_mock):
        # Make sure fail increments failed_tuples and calls emit
        self.spout.max_fails = 3
        self.spout.failed_tuples['foo'] = 2
        self.spout.unacked_tuples['foo'] = ([1, 2, 3], None, None, True)
        self.spout.fail('foo')
        self.assertEqual(self.spout.failed_tuples['foo'], 3)
        emit_mock.assert_called_with(self.spout, [1, 2, 3], direct_task=None,
                                     need_task_ids=True, stream=None,
                                     tup_id='foo')

    @patch.object(ReliableSpout, 'ack', autospec=True)
    @patch.object(ReliableSpout, 'emit', autospec=True)
    def test_fail_above_limit(self, emit_mock, ack_mock):
        # Make sure fail increments failed_tuples
        self.spout.max_fails = 3
        self.spout.failed_tuples['foo'] = 3
        emit_args = ([1, 2, 3], None, None, True)
        self.spout.unacked_tuples['foo'] = emit_args
        self.spout.fail('foo')
        self.assertEqual(emit_mock.call_count, 0)
        ack_mock.assert_called_with(self.spout, 'foo')


if __name__ == '__main__':
    unittest.main()
