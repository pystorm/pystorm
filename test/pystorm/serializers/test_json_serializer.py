from __future__ import absolute_import, print_function, unicode_literals

from io import StringIO
try:
    from unittest import mock
except ImportError:
    import mock

import simplejson as json

from pystorm.serializers.json_serializer import JSONSerializer

from .serializer import SerializerTestCase


class TestJSONSerializer(SerializerTestCase):

    INSTANCE_CLS = JSONSerializer

    def test_read_message_dict(self):
        msg_dict = {'hello': "world",}
        self.instance.input_stream = StringIO(self.instance.serialize_dict(msg_dict))
        assert self.instance.read_message() == msg_dict

    def test_read_message_list(self):
        msg_list = [3, 4, 5]
        self.instance.input_stream = StringIO(self.instance.serialize_dict(msg_list))
        assert self.instance.read_message() == msg_list

    def test_send_message(self):
        msg_dict = {'hello': "world",}
        expected_output = """{"hello": "world"}\nend\n"""
        self.instance.output_stream = StringIO()
        self.instance.send_message(msg_dict)
        assert self.instance.output_stream.getvalue() == expected_output
