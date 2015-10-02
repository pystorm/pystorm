from __future__ import absolute_import, print_function, unicode_literals

from io import BytesIO
from threading import RLock

try:
    from unittest import mock
except ImportError:
    import mock

import pytest

from pystorm.serializers.serializer import Serializer


class SerializerTestCase(object):

    INSTANCE_CLS = Serializer

    @pytest.fixture(autouse=True)
    def instance_fixture(self):
        self.instance = self.INSTANCE_CLS(BytesIO(),
                                          BytesIO(),
                                          reader_lock=RLock(),
                                          writer_lock=RLock())
