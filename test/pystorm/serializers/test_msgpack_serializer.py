from __future__ import absolute_import, print_function, unicode_literals

from io import BytesIO
import warnings

try:
    from unittest import mock
except ImportError:
    import mock

try:
    import msgpack

    HAVE_MSGPACK = True
except:
    warnings.warn(
        "Cannot import msgpack. It is necessary for using MsgpackSerializer",
        category=ImportWarning,
    )
    HAVE_MSGPACK = False
import pytest

from pystorm.exceptions import StormWentAwayError
from pystorm.serializers.msgpack_serializer import MsgpackSerializer

from .serializer import SerializerTestCase


class TestMsgpackSerializer(SerializerTestCase):

    INSTANCE_CLS = MsgpackSerializer

    @pytest.mark.skipif(not HAVE_MSGPACK, reason="msgpack not installed")
    def test_read_message_dict(self):
        msg_dict = {b"hello": b"world"}
        self.instance.input_stream = BytesIO(msgpack.packb(msg_dict))
        assert self.instance.read_message() == msg_dict

    @pytest.mark.skipif(not HAVE_MSGPACK, reason="msgpack not installed")
    def test_read_message_list(self):
        msg_list = [3, 4, 5]
        self.instance.input_stream = BytesIO(msgpack.packb(msg_list))
        assert self.instance.read_message() == msg_list

    @pytest.mark.skipif(not HAVE_MSGPACK, reason="msgpack not installed")
    def test_send_message(self):
        msg_dict = {"hello": "world"}
        expected_output = msgpack.packb(msg_dict)
        self.instance.send_message(msg_dict)
        assert self.instance.output_stream.getvalue() == expected_output

    @pytest.mark.skipif(not HAVE_MSGPACK, reason="msgpack not installed")
    def test_send_message_raises_stormwentaway(self):
        bytes_io_mock = mock.MagicMock(autospec=True)

        def raiser():  # lambdas can't raise
            raise IOError()

        bytes_io_mock.flush.side_effect = raiser
        self.instance.output_stream = bytes_io_mock
        with pytest.raises(StormWentAwayError):
            self.instance.send_message({"hello": "world"})
