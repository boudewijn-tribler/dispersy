from unittest import TestCase

from ..callback import Callback
from ..dispersy import Dispersy
from ..endpoint import StandaloneEndpoint
from ..logger import get_logger
logger = get_logger(__name__)


def call_on_dispersy_thread(func):
    def helper(*args, **kargs):
        return args[0]._dispersy.callback.call(func, args, kargs, priority=-1024)
    helper.__name__ = func.__name__
    return helper


class DispersyTestFunc(TestCase):

    """
    Setup and tear down Dispersy before and after each test method.

    setUp will ensure the following members exists before each test method is called:
    - self._dispersy
    - self._my_member
    - self._enable_strict

    tearDown will ensure these members are properly cleaned after each test method is finished.
    """

    def on_callback_exception(self, exception, is_fatal):
        logger.exception("%s (fatal: %s, strict: %s)", exception, is_fatal, self.enable_strict)

        if self.enable_strict and self._dispersy:
            # 09/12/13 Boudewijn: we must first set self._dispersy to None, then call
            # dispersy.stop() because dispersy.stop() will use callback.call to wait for all
            # existing tasks to finish, hence, when one of these tasks causes an error resulting in
            # another call to on_callback_exception, it should not call dispersy.stop again.
            dispersy = self._dispersy
            self._dispersy = None
            self._dispersy_stop_result = dispersy.stop()

        # consider every exception a fatal error when 'strict' is enabled
        return self.enable_strict

    @property
    def enable_strict(self):
        return self._enable_strict

    @enable_strict.setter
    def enable_strict(self, enable_strict):
        assert isinstance(enable_strict, bool), type(enable_strict)
        self._enable_strict = enable_strict

    def assert_dispersy_start(self):
        " Test that Dispersy properly started. "
        self.assertTrue(self._dispersy_start_result, "Dispersy did not properly start")

    def assert_dispersy_stop(self):
        " Test that Dispersy properly stopped. "
        self.assertTrue(self._dispersy_stop_result, "Dispersy did not properly stop")

    def setUp(self):
        super(DispersyTestFunc, self).setUp()
        logger.debug("setUp")

        self._enable_strict = True
        self._dispersy_start_result = False
        self._dispersy_stop_result = False

        callback = Callback("Dispersy-Unit-Test")
        callback.attach_exception_handler(self.on_callback_exception)
        endpoint = StandaloneEndpoint(12345)
        working_directory = u"."
        database_filename = u":memory:"

        self._dispersy = Dispersy(callback, endpoint, working_directory, database_filename)
        self._dispersy_start_result = self._dispersy.start()
        self.assert_dispersy_start()
        self._my_member = callback.call(self._dispersy.get_new_member, (u"low",))

    def tearDown(self):
        super(DispersyTestFunc, self).tearDown()
        logger.debug("tearDown")

        if self._dispersy:
            self._dispersy_stop_result = self._dispersy.stop()
            self._dispersy = None
        self._my_member = None

        self.assert_dispersy_stop()
