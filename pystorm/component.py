"""Base primititve classes for working with Storm."""
from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import signal
import sys
from collections import deque, namedtuple
from logging.handlers import RotatingFileHandler
from os.path import join
from threading import RLock
from traceback import format_exc

from .exceptions import StormWentAwayError
from .serializers.msgpack_serializer import MsgpackSerializer
from .serializers.json_serializer import JSONSerializer

# Support for Storm Log levels as per STORM-414
_STORM_LOG_TRACE = 0
_STORM_LOG_DEBUG = 1
_STORM_LOG_INFO = 2
_STORM_LOG_WARN = 3
_STORM_LOG_ERROR = 4
_STORM_LOG_LEVELS = {'trace': _STORM_LOG_TRACE,
                     'debug': _STORM_LOG_DEBUG,
                     'info': _STORM_LOG_INFO,
                     'warn': _STORM_LOG_WARN,
                     'warning': _STORM_LOG_WARN,
                     'error': _STORM_LOG_ERROR,
                     'critical': _STORM_LOG_ERROR}
_PYTHON_LOG_LEVELS = {'critical': logging.CRITICAL,
                      'error': logging.ERROR,
                      'warning': logging.WARNING,
                      'warn': logging.WARNING,
                      'info': logging.INFO,
                      'debug': logging.DEBUG,
                      'trace': logging.DEBUG}
_SERIALIZERS = {"json": JSONSerializer, "msgpack": MsgpackSerializer}


log = logging.getLogger(__name__)


def remote_pdb_handler(signum, frame):
    """ Handler to drop us into a remote debugger upon receiving SIGUSR1 """
    try:
        from remote_pdb import RemotePdb
        rdb = RemotePdb(host='127.0.0.1', port=0)
        rdb.set_trace(frame=frame)
    except ImportError:
        log.warning('remote_pdb unavailable.  Please install remote_pdb to '
                    'allow remote debugging.')
    # Restore signal handler for later
    signal.signal(signum, remote_pdb_handler)


class StormHandler(logging.Handler):
    """Handler that will send messages back to Storm."""

    def __init__(self, serializer):
        """ Initialize handler

        :param serializer: The serializer of the component this handler is being
                           used for.
        """
        super(StormHandler, self).__init__()
        self.serializer = serializer

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        If exception information is present, it is formatted using
        traceback.print_exception and sent to Storm.
        """
        try:
            msg = self.format(record)
            level = _STORM_LOG_LEVELS.get(record.levelname.lower(),
                                          _STORM_LOG_INFO)
            self.serializer.send_message({'command': 'log', 'msg': str(msg),
                                          'level': level})
        except Exception:
            self.handleError(record)


class LogStream(object):
    """Object that implements enough of the Python stream API to be used as
    sys.stdout. Messages are written to the Python logger.
    """
    def __init__(self, logger):
        self.logger = logger

    def write(self, message):
        if message.strip() == "":
            return  # skip blank lines

        try:
            self.logger.info(message)
        except:
            # There's been an issue somewhere in the logging sub-system
            # so we'll put stderr and stdout back to their originals and
            # raise the exception which will cause Storm to choke
            sys.stdout = sys.__stdout__
            raise

    def flush(self):
        """No-op method to prevent crashes when someone does
        sys.stdout.flush.
        """
        pass


Tuple = namedtuple('Tuple', 'id component stream task values')
"""Storm's primitive data type passed around via streams.

:ivar id: the ID of the Tuple.
:type id: str
:ivar component: component that the Tuple was generated from.
:type component: str
:ivar stream: the stream that the Tuple was emitted into.
:type stream: str
:ivar task: the task the Tuple was generated from.
:type task: int
:ivar values: the payload of the Tuple where data is stored.
:type values: tuple (or namedtuple for Storm 0.10.0+)
"""


class Component(object):
    """Base class for spouts and bolts which contains class methods for
    logging messages back to the Storm worker process.


    :ivar input_stream: The ``file``-like object to use to retrieve commands
                        from Storm.  Defaults to ``sys.stdin``.
    :ivar output_stream: The ``file``-like object to send messages to Storm with.
                         Defaults to ``sys.stdout``.
    :ivar topology_name: The name of the topology sent by Storm in the initial
                         handshake.
    :ivar task_id: The numerical task ID for this component, as sent by Storm in
                   the initial handshake.
    :ivar component_name: The name of this component, as sent by Storm in the
                          initial handshake.
    :ivar debug: A ``bool`` indicating whether or not Storm is running in debug
                 mode.  Specified by the `topology.debug` Storm setting.
    :ivar storm_conf: A ``dict`` containing the configuration values sent by
                      Storm in the initial handshake with this component.
    :ivar context: The context of where this component is in the topology.  See
                   `the Storm Multi-Lang protocol documentation <https://storm.apache.org/documentation/Multilang-protocol.html>`__
                   for details.
    :ivar pid: An ``int`` indicating the process ID of this component as
               retrieved by ``os.getpid()``.
    :ivar logger: A logger to use with this component.

                  .. note::
                    Using ``Component.logger`` combined with the
                    :class:`pystorm.component.StormHandler` handler is
                    the recommended way for logging messages from your
                    component. If you use ``Component.log`` instead, the logging
                    messages will *always* be sent to Storm, even if they are
                    ``debug`` level messages and you are running in production.
                    Using :class:`pystorm.component.StormHandler`
                    ensures that you will instead have your logging messages
                    filtered on the Python side and only have the messages you
                    actually want logged serialized and sent to Storm.
    :ivar serializer: The ``Serializer`` that is used to serialize messages
                      between Storm and Python.
    :ivar exit_on_exception:  A ``bool`` indicating whether or not the process
                              should exit when an exception other than
                              ``StormWentAwayError`` is raised.  Defaults to
                              ``True``.
    """
    exit_on_exception = True

    def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout,
                 rdb_signal=signal.SIGUSR1, serializer="json"):
        # Ensure we don't fall back on the platform-dependent encoding and
        # always use UTF-8
        self.topology_name = None
        self.task_id = None
        self.component_name = None
        self.debug = None
        self.storm_conf = None
        self.context = None
        self.pid = os.getpid()
        self.logger = None
        # pending commands/Tuples we read while trying to read task IDs
        self._pending_commands = deque()
        # pending task IDs we read while trying to read commands/Tuples
        self._pending_task_ids = deque()
        self._reader_lock = RLock()
        self._writer_lock = RLock()
        if serializer in _SERIALIZERS:
            self.serializer = _SERIALIZERS[serializer](input_stream,
                                                       output_stream,
                                                       self._reader_lock,
                                                       self._writer_lock)
        else:
            raise ValueError("Unknown serializer: {0}", serializer)

        # Setup remote pdb handler if asked to
        if rdb_signal is not None:
            signal.signal(rdb_signal, remote_pdb_handler)

    @staticmethod
    def is_heartbeat(tup):
        """ :returns: Whether or not the given Tuple is a heartbeat """
        return tup.task == -1 and tup.stream == '__heartbeat'

    def _setup_component(self, storm_conf, context):
        """Add helpful instance variables to component after initial handshake
        with Storm.  Also configure logging.
        """
        self.topology_name = storm_conf.get('topology.name', '')
        self.task_id = context.get('taskid', '')
        self.component_name = context.get('componentid')
        # If using Storm before 0.10.0 componentid is not available
        if self.component_name is None:
            self.component_name = context.get('task->component', {})\
                                         .get(str(self.task_id), '')
        self.debug = storm_conf.get("topology.debug", False)
        self.storm_conf = storm_conf
        self.context = context

        # Set up logging
        self.logger = logging.getLogger('.'.join((__name__,
                                                  self.component_name)))
        log_path = self.storm_conf.get('pystorm.log.path')
        log_file_name = self.storm_conf.get('pystorm.log.file',
                                            'pystorm_{topology_name}'
                                            '_{component_name}'
                                            '_{task_id}'
                                            '_{pid}.log')
        root_log = logging.getLogger()
        log_level = self.storm_conf.get('pystorm.log.level', 'info')
        if log_path:
            max_bytes = self.storm_conf.get('pystorm.log.max_bytes',
                                            1000000)  # 1 MB
            backup_count = self.storm_conf.get('pystorm.log.backup_count',
                                               10)
            log_file = join(log_path,
                            (log_file_name
                             .format(topology_name=self.topology_name,
                                     component_name=self.component_name,
                                     task_id=self.task_id,
                                     pid=self.pid)))
            handler = RotatingFileHandler(log_file, maxBytes=max_bytes,
                                          backupCount=backup_count)
            log_format = self.storm_conf.get('pystorm.log.format',
                                             '%(asctime)s - %(name)s - '
                                             '%(levelname)s - %(message)s')
        else:
            self.log('pystorm StormHandler logging enabled, so all messages at '
                     'levels greater than "pystorm.log.level" ({}) will be sent'
                     ' to Storm.'.format(log_level))
            handler = StormHandler(self.serializer)
            log_format = self.storm_conf.get('pystorm.log.format',
                                             '%(asctime)s - %(name)s - '
                                             '%(message)s')
        formatter = logging.Formatter(log_format)
        log_level = _PYTHON_LOG_LEVELS.get(log_level, logging.INFO)
        if self.debug:
            # potentially override logging that was provided if
            # topology.debug was set to true
            log_level = logging.DEBUG
        handler.setLevel(log_level)
        handler.setFormatter(formatter)
        root_log.addHandler(handler)
        self.logger.setLevel(log_level)
        logging.getLogger('pystorm').setLevel(log_level)
        # Redirect stdout to ensure that print statements/functions
        # won't disrupt the multilang protocol
        if self.serializer.output_stream == sys.stdout:
            sys.stdout = LogStream(logging.getLogger('pystorm.stdout'))

    def read_message(self):
        """Read a message from Storm via serializer."""
        return self.serializer.read_message()

    def read_task_ids(self):
        if self._pending_task_ids:
            return self._pending_task_ids.popleft()
        else:
            msg = self.read_message()
            while not isinstance(msg, list):
                self._pending_commands.append(msg)
                msg = self.read_message()
            return msg

    def read_command(self):
        if self._pending_commands:
            return self._pending_commands.popleft()
        else:
            msg = self.read_message()
            while isinstance(msg, list):
                self._pending_task_ids.append(msg)
                msg = self.read_message()
            return msg

    def read_handshake(self):
        """Read and process an initial handshake message from Storm."""
        msg = self.read_message()
        pid_dir, _conf, _context = msg['pidDir'], msg['conf'], msg['context']

        # Write a blank PID file out to the pidDir
        open(join(pid_dir, str(self.pid)), 'w').close()
        self.send_message({'pid': self.pid})

        return _conf, _context

    def send_message(self, message):
        """Send a message to Storm via stdout."""
        if not isinstance(message, dict):
            logger = self.logger if self.logger else log
            logger.error("%s.%d attempted to send a non dict message to Storm: "
                         "%r", self.component_name, self.pid, message)
            return
        self.serializer.send_message(message)

    def raise_exception(self, exception, tup=None):
        """Report an exception back to Storm via logging.

        :param exception: a Python exception.
        :param tup: a :class:`Tuple` object.
        """
        if tup:
            message = ('Python {exception_name} raised while processing Tuple '
                       '{tup!r}\n{traceback}')
        else:
            message = 'Python {exception_name} raised\n{traceback}'
        message = message.format(exception_name=exception.__class__.__name__,
                                 tup=tup,
                                 traceback=format_exc())
        self.send_message({'command': 'error', 'msg': str(message)})
        self.send_message({'command': 'sync'})  # sync up right away

    def report_metric(self, name, value):
        """Report a custom metric back to Storm.

        :param name:  Name of the metric.  This can be anything.
        :param value: Value of the metric.  This is usually a number.

        Only supported in Storm 0.9.3+.

        .. note:
            In order for this to work, the metric must be registered on the
            Storm side.  See example code
            `here <https://github.com/dashengju/storm/blob/573c42a64885dac9a6a0d4c69a754500b607a8f1/storm-core/src/jvm/backtype/storm/testing/PythonShellMetricsBolt.java#L22-L23>`__.
        """
        self.send_message({'command': 'metrics', 'name': name, 'params': value})

    def log(self, message, level=None):
        """Log a message to Storm optionally providing a logging level.

        :param message: the log message to send to Storm.
        :type message: str
        :param level: the logging level that Storm should use when writing the
                      ``message``. Can be one of: trace, debug, info, warn, or
                      error (default: ``info``).
        :type level: str

        .. warning::

          This will send your message to Storm regardless of what level you
          specify.  In almost all cases, you are better of using
          ``Component.logger`` and not setting ``pystorm.log.path``, because
          that will use a :class:`pystorm.component.StormHandler` to do the
          filtering on the Python side (instead of on the Java side after taking
          the time to serialize your message and send it to Storm).
        """
        level = _STORM_LOG_LEVELS.get(level, _STORM_LOG_INFO)
        self.send_message({'command': 'log', 'msg': str(message),
                           'level': level})

    def emit(self, tup, tup_id=None, stream=None, anchors=None,
             direct_task=None, need_task_ids=False):
        """Emit a new Tuple to a stream.

        :param tup: the Tuple payload to send to Storm, should contain only
                    JSON-serializable data.
        :type tup: :class:`list` or :class:`pystorm.component.Tuple`
        :param tup_id: the ID for the Tuple. If omitted by a
                       :class:`pystorm.spout.Spout`, this emit will be
                       unreliable.
        :type tup_id: str
        :param stream: the ID of the stream to emit this Tuple to. Specify
                       ``None`` to emit to default stream.
        :type stream: str
        :param anchors: IDs the Tuples (or
                        :class:`pystorm.component.Tuple` instances)
                        which the emitted Tuples should be anchored to. This is
                        only passed by :class:`pystorm.bolt.Bolt`.
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
        if not isinstance(tup, (list, tuple)):
            raise TypeError('All Tuples must be either lists or tuples, '
                            'received {!r} instead.'.format(type(tup)))

        msg = {'command': 'emit', 'tuple': tup}
        downstream_task_ids = None

        if anchors is not None:
            msg['anchors'] = anchors
        if tup_id is not None:
            msg['id'] = tup_id
        if stream is not None:
            msg['stream'] = stream
        if direct_task is not None:
            msg['task'] = direct_task
            if need_task_ids:
                downstream_task_ids = [direct_task]

        if not need_task_ids:
            # only need to send on False, Storm's default is True
            msg['need_task_ids'] = need_task_ids

        if need_task_ids and direct_task is None:
            # Use both locks so we ensure send_message and read_task_ids are for
            # same emit
            with self._reader_lock, self._writer_lock:
                self.send_message(msg)
                downstream_task_ids = self.read_task_ids()
        # No locks necessary in simple case because serializer will acquire
        # write lock itself
        else:
            self.send_message(msg)

        return downstream_task_ids

    def _run(self):
        """The inside of ``run``'s infinite loop.

        Separated out so it can be properly unit tested.

        Must be implemented by sub-class.
        """
        raise NotImplementedError

    def initialize(self, storm_conf, context):
        """Called immediately after the initial handshake with Storm and before
        the main run loop. A good place to initialize connections to data
        sources.

        :param storm_conf: the Storm configuration for this component.  This is
                           the configuration provided to the topology, merged in
                           with cluster configuration on the worker node.
        :type storm_conf: dict
        :param context: information about the component's place within the
                        topology such as: task IDs, inputs, outputs etc.
        :type context: dict
        """
        pass

    def run(self):
        """Main run loop for all components.

        Performs initial handshake with Storm and reads Tuples handing them off
        to subclasses.  Any exceptions are caught and logged back to Storm
        prior to the Python process exiting.

        .. warning::

            Subclasses should **not** override this method.
        """
        storm_conf, context = self.read_handshake()
        self._setup_component(storm_conf, context)
        self.initialize(storm_conf, context)
        while True:
            try:
                self._run()
            except StormWentAwayError:
                log.info('Exiting because parent Storm process went away.')
                sys.exit(2)
            except Exception as e:
                log_msg = "Exception in {}.run()".format(self.__class__.__name__)
                exc_info = sys.exc_info()
                try:
                    self.logger.error(log_msg, exc_info=True)
                    self._handle_run_exception(e)
                except StormWentAwayError:
                    log.error(log_msg, exc_info=exc_info)
                    log.info('Exiting because parent Storm process went away.')
                    sys.exit(2)
                except:
                    log.error(log_msg, exc_info=exc_info)
                    log.error('While trying to handle previous exception...',
                              exc_info=sys.exc_info())
                if self.exit_on_exception:
                    sys.exit(1)

    def _handle_run_exception(self, exc):
        """Process an exception encountered while running the ``run()`` loop.

        The base implementation just uses ``Component.raise_exception(exc)`` to
        report the exception bag to Storm.

        Called right before program exits.
        """
        self.raise_exception(exc)
