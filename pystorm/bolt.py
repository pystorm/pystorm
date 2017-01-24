"""Base bolt classes."""
from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import re
import signal
import sys
import threading
import time
from collections import defaultdict, namedtuple

from six import iteritems, itervalues, reraise

from .component import Component, Tuple


# Convert names to valid Python identifiers by replacing non-word characters
# whitespace and leading digits with underscores.
_IDENTIFIER_RE = re.compile(r'\W|^(?=\d)')


log = logging.getLogger(__name__)


class Bolt(Component):
    """The base class for all pystorm bolts.

    For more information on bolts, consult Storm's
    `Concepts documentation <http://storm.apache.org/documentation/Concepts.html>`_.

    :ivar auto_anchor: A ``bool`` indicating whether or not the bolt should
                       automatically anchor emits to the incoming Tuple ID.
                       Tuple anchoring is how Storm provides reliability, you
                       can read more about
                       `Tuple anchoring in Storm's docs <https://storm.apache.org/documentation/Guaranteeing-message-processing.html#what-is-storms-reliability-api>`_.
                       Default is ``True``.

    :ivar auto_ack: A ``bool`` indicating whether or not the bolt should
                    automatically acknowledge Tuples after ``process()``
                    is called. Default is ``True``.
    :ivar auto_fail: A ``bool`` indicating whether or not the bolt should
                     automatically fail Tuples when an exception occurs when the
                     ``process()`` method is called. Default is ``True``.

    **Example**:

    .. code-block:: python

        from pystorm.bolt import Bolt

        class SentenceSplitterBolt(Bolt):

            def process(self, tup):
                sentence = tup.values[0]
                for word in sentence.split(" "):
                    self.emit([word])
    """

    auto_anchor = True
    auto_ack = True
    auto_fail = True

    # Using list; Bolt class and subclasses can have more than one current_tup.
    _current_tups = []

    def __init__(self, *args, **kwargs):
        super(Bolt, self).__init__(*args, **kwargs)
        self._source_tuple_types = defaultdict(dict)

    def _setup_component(self, storm_conf, context):
        # See Component._setup_component for docs
        super(Bolt, self)._setup_component(storm_conf, context)
        # source->stream->fields requires Storm 0.10.0 or later
        source_stream_fields = context.get('source->stream->fields', {})
        for source, stream_fields in iteritems(source_stream_fields):
            for stream, fields in iteritems(stream_fields):
                type_name = (_IDENTIFIER_RE.sub('_', source.title()) +
                             _IDENTIFIER_RE.sub('_', stream.title()) +
                             'Tuple')
                self._source_tuple_types[source][stream] = namedtuple(type_name,
                                                                      fields)

    @staticmethod
    def is_tick(tup):
        """ :returns: Whether or not the given Tuple is a tick Tuple """
        return tup.component == '__system' and tup.stream == '__tick'

    def read_tuple(self):
        """Read a tuple from the pipe to Storm."""
        cmd = self.read_command()
        source = cmd['comp']
        stream = cmd['stream']
        values = cmd['tuple']
        val_type = self._source_tuple_types[source].get(stream)
        return Tuple(cmd['id'], source, stream, cmd['task'],
                     tuple(values) if val_type is None else val_type(*values))

    def process(self, tup):
        """Process a single Tuple :class:`pystorm.component.Tuple` of
        input

        This should be overridden by subclasses.
        :class:`pystorm.component.Tuple` objects contain metadata
        about which component, stream and task it came from. The actual values
        of the Tuple can be accessed by calling ``tup.values``.

        :param tup: the Tuple to be processed.
        :type tup: :class:`pystorm.component.Tuple`
        """
        raise NotImplementedError()

    def process_tick(self, tup):
        """Process special 'tick Tuples' which allow time-based
        behaviour to be included in bolts.

        Default behaviour is to ignore time ticks.  This should be
        overridden by subclasses who wish to react to timer events
        via tick Tuples.

        Tick Tuples will be sent to all bolts in a toplogy when the
        storm configuration option 'topology.tick.tuple.freq.secs'
        is set to an integer value, the number of seconds.

        :param tup: the Tuple to be processed.
        :type tup: :class:`pystorm.component.Tuple`
        """
        pass

    def emit(self, tup, stream=None, anchors=None, direct_task=None,
             need_task_ids=False):
        """Emit a new Tuple to a stream.

        :param tup: the Tuple payload to send to Storm, should contain only
                    JSON-serializable data.
        :type tup: :class:`list` or :class:`pystorm.component.Tuple`
        :param stream: the ID of the stream to emit this Tuple to. Specify
                       ``None`` to emit to default stream.
        :type stream: str
        :param anchors: IDs the Tuples (or :class:`pystorm.component.Tuple`
                        instances) which the emitted Tuples should be anchored
                        to. If ``auto_anchor`` is set to ``True`` and
                        you have not specified ``anchors``, ``anchors`` will be
                        set to the incoming/most recent Tuple ID(s).
        :type anchors: list
        :param direct_task: the task to send the Tuple to.
        :type direct_task: int
        :param need_task_ids: indicate whether or not you'd like the task IDs
                              the Tuple was emitted (default: ``False``).
        :type need_task_ids: bool

        :returns: ``None``, unless ``need_task_ids=True``, in which case it will
                  be a ``list`` of task IDs that the Tuple was sent to if. Note
                  that when specifying direct_task, this will be equal to
                  ``[direct_task]``.
        """
        if anchors is None:
            anchors = self._current_tups if self.auto_anchor else []
        anchors = [a.id if isinstance(a, Tuple) else a for a in anchors]

        return super(Bolt, self).emit(tup, stream=stream, anchors=anchors,
                                      direct_task=direct_task,
                                      need_task_ids=need_task_ids)

    def ack(self, tup):
        """Indicate that processing of a Tuple has succeeded.

        :param tup: the Tuple to acknowledge.
        :type tup: :class:`str` or :class:`pystorm.component.Tuple`
        """
        tup_id = tup.id if isinstance(tup, Tuple) else tup
        self.send_message({'command': 'ack', 'id': tup_id})

    def fail(self, tup):
        """Indicate that processing of a Tuple has failed.

        :param tup: the Tuple to fail (its ``id`` if ``str``).
        :type tup: :class:`str` or :class:`pystorm.component.Tuple`
        """
        tup_id = tup.id if isinstance(tup, Tuple) else tup
        self.send_message({'command': 'fail', 'id': tup_id})

    def _run(self):
        """The inside of ``run``'s infinite loop.

        Separated out so it can be properly unit tested.
        """
        tup = self.read_tuple()
        self._current_tups = [tup]
        if self.is_heartbeat(tup):
            self.send_message({'command': 'sync'})
        elif self.is_tick(tup):
            self.process_tick(tup)
            if self.auto_ack:
                 self.ack(tup)
        else:
            self.process(tup)
            if self.auto_ack:
                 self.ack(tup)
        # Reset _current_tups so that we don't accidentally fail the wrong
        # Tuples if a successive call to read_tuple fails.
        # This is not done in `finally` clause because we want the current
        # Tuples to fail when there is an exception.
        self._current_tups = []

    def _handle_run_exception(self, exc):
        """Process an exception encountered while running the ``run()`` loop.

        Called right before program exits.
        """
        if len(self._current_tups) == 1:
            tup = self._current_tups[0]
            self.raise_exception(exc, tup)
            if self.auto_fail:
                self.fail(tup)


class BatchingBolt(Bolt):
    """A bolt which batches Tuples for processing.

    Batching Tuples is unexpectedly complex to do correctly. The main problem
    is that all bolts are single-threaded. The difficult comes when the
    topology is shutting down because Storm stops feeding the bolt Tuples. If
    the bolt is blocked waiting on stdin, then it can't process any waiting
    Tuples, or even ack ones that were asynchronously written to a data store.

    This bolt helps with that by grouping Tuples received between tick Tuples
    into batches.

    To use this class, you must implement ``process_batch``. ``group_key`` can
    be optionally implemented so that Tuples are grouped before
    ``process_batch`` is even called.


    :ivar auto_anchor: A ``bool`` indicating whether or not the bolt should
                       automatically anchor emits to the incoming Tuple ID.
                       Tuple anchoring is how Storm provides reliability, you
                       can read more about `Tuple anchoring in Storm's
                       docs <https://storm.apache.org/documentation/Guaranteeing-message-processing.html#what-is-storms-reliability-api>`_.
                       Default is ``True``.
    :ivar auto_ack: A ``bool`` indicating whether or not the bolt should
                    automatically acknowledge Tuples after ``process_batch()``
                    is called. Default is ``True``.
    :ivar auto_fail: A ``bool`` indicating whether or not the bolt should
                     automatically fail Tuples when an exception occurs when the
                     ``process_batch()`` method is called. Default is ``True``.
    :ivar ticks_between_batches: The number of tick Tuples to wait before
                                 processing a batch.


    **Example**:

    .. code-block:: python

        from pystorm.bolt import BatchingBolt

        class WordCounterBolt(BatchingBolt):

            ticks_between_batches = 5

            def group_key(self, tup):
                word = tup.values[0]
                return word  # collect batches of words

            def process_batch(self, key, tups):
                # emit the count of words we had per 5s batch
                self.emit([key, len(tups)])
    """

    auto_anchor = True
    auto_ack = True
    auto_fail = True
    ticks_between_batches = 1

    def __init__(self, *args, **kwargs):
        super(BatchingBolt, self).__init__(*args, **kwargs)
        self._batches = defaultdict(list)
        self._tick_counter = 0
        self._current_key = None

    def group_key(self, tup):
        """Return the group key used to group Tuples within a batch.

        By default, returns None, which put all Tuples in a single
        batch, effectively just time-based batching. Override this to create
        multiple batches based on a key.

        :param tup: the Tuple used to extract a group key
        :type tup: :class:`pystorm.component.Tuple`
        :returns: Any ``hashable`` value.
        """
        return None

    def process_batch(self, key, tups):
        """Process a batch of Tuples. Should be overridden by subclasses.

        :param key: the group key for the list of batches.
        :type key: hashable
        :param tups: a `list` of :class:`pystorm.component.Tuple` s
                     for the group.
        :type tups: list
        """
        raise NotImplementedError()

    def emit(self, tup, **kwargs):
        """Modified emit that will not return task IDs after emitting.

        See :class:`pystorm.component.Bolt` for more information.

        :returns: ``None``.
        """
        kwargs['need_task_ids'] = False
        return super(BatchingBolt, self).emit(tup, **kwargs)

    def process_tick(self, tick_tup):
        """Increment tick counter, and call ``process_batch`` for all current
        batches if tick counter exceeds ``ticks_between_batches``.

        See :class:`pystorm.component.Bolt` for more information.

        .. warning::
            This method should **not** be overriden.  If you want to tweak
            how Tuples are grouped into batches, override ``group_key``.
        """
        self._tick_counter += 1
        # ACK tick Tuple immediately, since it's just responsible for counter
        self.ack(tick_tup)
        if self._tick_counter > self.ticks_between_batches and self._batches:
            self.process_batches()
            self._tick_counter = 0

    def process_batches(self):
        """Iterate through all batches, call process_batch on them, and ack.

        Separated out for the rare instances when we want to subclass
        BatchingBolt and customize what mechanism causes batches to be
        processed.
        """
        for key, batch in iteritems(self._batches):
            self._current_tups = batch
            self._current_key = key
            self.process_batch(key, batch)
            if self.auto_ack:
                for tup in batch:
                    self.ack(tup)
            # Set current batch to [] so that we know it was acked if a
            # later batch raises an exception
            self._current_key = None
            self._batches[key] = []
        self._batches = defaultdict(list)

    def process(self, tup):
        """Group non-tick Tuples into batches by ``group_key``.

        .. warning::
            This method should **not** be overriden.  If you want to tweak
            how Tuples are grouped into batches, override ``group_key``.
        """
        # Append latest Tuple to batches
        group_key = self.group_key(tup)
        self._batches[group_key].append(tup)

    def _run(self):
        """The inside of ``run``'s infinite loop.

        Separated out so it can be properly unit tested.
        """
        self._current_tups = [self.read_tuple()]
        tup = self._current_tups[0]
        if self.is_heartbeat(tup):
            self.send_message({'command': 'sync'})
        elif self.is_tick(tup):
            self.process_tick(tup)
        else:
            self.process(tup)
        # reset so that we don't accidentally fail the wrong Tuples
        # if a successive call to read_tuple fails
        self._current_tups = []

    def _handle_run_exception(self, exc):
        """Process an exception encountered while running the ``run()`` loop.

        Called right before program exits.
        """
        self.raise_exception(exc, self._current_tups)

        if self.auto_fail:
            failed = set()
            for key, batch in iteritems(self._batches):
                # Only wipe out batches other than current for exit_on_exception
                if self.exit_on_exception or key == self._current_key:
                    for tup in batch:
                        self.fail(tup)
                        failed.add(tup.id)

            # Fail current batch or tick Tuple if we have one
            for tup in self._current_tups:
                if tup.id not in failed:
                    self.fail(tup)

            # Reset current batch info
            self._batches[self._current_key] = []
            self._current_key = None


class TicklessBatchingBolt(BatchingBolt):
    """A BatchingBolt which uses a timer thread instead of tick tuples.

    Batching tuples is unexpectedly complex to do correctly. The main problem
    is that all bolts are single-threaded. The difficult comes when the
    topology is shutting down because Storm stops feeding the bolt tuples. If
    the bolt is blocked waiting on stdin, then it can't process any waiting
    tuples, or even ack ones that were asynchronously written to a data store.

    This bolt helps with that grouping tuples based on a time interval and then
    processing them on a worker thread.

    To use this class, you must implement ``process_batch``. ``group_key`` can
    be optionally implemented so that tuples are grouped before
    ``process_batch`` is even called.


    :ivar auto_anchor: A ``bool`` indicating whether or not the bolt should
                       automatically anchor emits to the incoming tuple ID.
                       Tuple anchoring is how Storm provides reliability, you
                       can read more about `tuple anchoring in Storm's
                       docs <https://storm.incubator.apache.org/documentation/Guaranteeing-message-processing.html#what-is-storms-reliability-api>`_.
                       Default is ``True``.
    :ivar auto_ack: A ``bool`` indicating whether or not the bolt should
                    automatically acknowledge tuples after ``process_batch()``
                    is called. Default is ``True``.
    :ivar auto_fail: A ``bool`` indicating whether or not the bolt should
                     automatically fail tuples when an exception occurs when the
                     ``process_batch()`` method is called. Default is ``True``.
    :ivar secs_between_batches: The time (in seconds) between calls to
                                ``process_batch()``. Note that if there are no
                                tuples in any batch, the TicklessBatchingBolt will
                                continue to sleep.

                                .. note::
                                  Can be fractional to specify greater
                                  precision (e.g. 2.5).

    **Example**:

    .. code-block:: python

        from pystorm.bolt import TicklessBatchingBolt

        class WordCounterBolt(TicklessBatchingBolt):

            secs_between_batches = 5

            def group_key(self, tup):
                word = tup.values[0]
                return word  # collect batches of words

            def process_batch(self, key, tups):
                # emit the count of words we had per 5s batch
                self.emit([key, len(tups)])
    """

    auto_anchor = True
    auto_ack = True
    auto_fail = True
    secs_between_batches = 2

    def __init__(self, *args, **kwargs):
        super(TicklessBatchingBolt, self).__init__(*args, **kwargs)
        self.exc_info = None
        signal.signal(signal.SIGUSR1, self._handle_worker_exception)

        iname = self.__class__.__name__
        threading.current_thread().name = '{}:main-thread'.format(iname)
        self._batch_lock = threading.RLock()
        self._batcher = threading.Thread(target=self._batch_entry)
        self._batcher.name = '{}:_batcher-thread'.format(iname)
        self._batcher.daemon = True
        self._batcher.start()

    def process_tick(self, tick_tup):
        """ Just ack tick tuples and ignore them. """
        self.ack(tick_tup)

    def _batch_entry_run(self):
        """The inside of ``_batch_entry``'s infinite loop.

        Separated out so it can be properly unit tested.
        """
        time.sleep(self.secs_between_batches)
        with self._batch_lock:
            self.process_batches()

    def _batch_entry(self):
        """Entry point for the batcher thread."""
        try:
            while True:
                self._batch_entry_run()
        except:
            self.exc_info = sys.exc_info()
            os.kill(self.pid, signal.SIGUSR1)  # interrupt stdin waiting

    def _handle_worker_exception(self, signum, frame):
        """Handle an exception raised in the worker thread.

        Exceptions in the _batcher thread will send a SIGUSR1 to the main
        thread which we catch here, and then raise in the main thread.
        """
        with self._batch_lock:
            reraise(*self.exc_info)

    def _run(self):
        """The inside of ``run``'s infinite loop.

        Separate from BatchingBolt's implementation because
        we need to be able to acquire the batch lock after
        reading the tuple.

        We can't acquire the lock before reading the tuple because if
        that hangs (i.e. the topology is shutting down) the lock being
        acquired will freeze the rest of the bolt, which is precisely
        what this batcher seeks to avoid.
        """
        tup = self.read_tuple()
        with self._batch_lock:
            self._current_tups = [tup]
            if self.is_heartbeat(tup):
                self.send_message({'command': 'sync'})
            elif self.is_tick(tup):
                self.process_tick(tup)
            else:
                self.process(tup)
            # reset so that we don't accidentally fail the wrong Tuples
            # if a successive call to read_tuple fails
            self._current_tups = []
