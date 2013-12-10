from time import sleep

from ..logger import get_logger
from .dispersytestclass import DispersyTestFunc, call_on_dispersy_thread
logger = get_logger(__name__)

def failure_to_success(exception_class, exception_message):
    def helper1(func):
        def helper2(*args, **kargs):
            try:
                func(*args, **kargs)
            except Exception as exception:
                if isinstance(exception, exception_class) and exception.message == exception_message:
                    # matches the pre-programmes exception, should not fail
                    return

                # not one of the pre-programmed exceptions, test should indicate failure
                raise

            # expected an exception, fail
            raise AssertionError("Expected an exception")

        helper2.__name__ = func.__name__
        return helper2
    return helper1


class TestImproperDispersyStop(DispersyTestFunc):

    def assert_dispersy_stop(self):
        " Test that Dispersy did -not- properly stop. "
        self.assertFalse(self._dispersy_stop_result, "Dispersy should not properly stop")

    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_infinite_loop_at_shutdown(self):
        " Cause infinite loop during shutdown. "
        def task(callback):
            callback.register(task, (callback,))
            sleep(1.0)
        self.assertTrue(self.enable_strict)
        task(self._dispersy.callback)

        # this assert causes Callback to call the exception handlers, resulting in Dispersy.stop().
        # Dispersy.stop() uses Callback.call(..., priority=-512, timeout=10.0) to wait until
        # the main components of Dispersy are stopped.
        #
        # However, TASK has a higher priority and will continue to run until a timeout occurs in the
        # aforementioned Callback.call(...).  Once this timeout occurs Dispersy.stop() will return
        # False, and the unit case tearDown will be called.  Note that Dispersy is still running
        # until the garbage collector figures out it is no longer used.
        self.assertTrue(False, "This must fail")

        # yield thread, causes TASK to run
        yield 1.0


class TestUnittest(DispersyTestFunc):
    """
    Tests ensuring that an exception anywhere in _dispersy.callback is propagated to the unittest framework.

    The 'strict' tests will ensure that any exception results in an early shutdown.  Early shutdown
    causes the call_on_dispersy_thread generator to receive a Shutdown command, resulting in a
    RuntimeError("Early shutdown") exception on the caller.

    Non 'strict' tests will result in the Callback ignoring KeyError and AssertionError exceptions.
    """
    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_assert(self):
        " Trivial assert. "
        self.assertTrue(False, "This must fail")
        self.fail("Should not reach this")

    @failure_to_success(KeyError, "This must fail")
    @call_on_dispersy_thread
    def test_KeyError(self):
        " Trivial KeyError. "
        raise KeyError("This must fail")

    @failure_to_success(RuntimeError, "Early shutdown")
    @call_on_dispersy_thread
    def test_assert_strict_callback(self):
        " Assert within a registered task. "
        def task():
            self.assertTrue(False, "This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.register(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(RuntimeError, "Early shutdown")
    @call_on_dispersy_thread
    def test_KeyError_strict_callback(self):
        " KeyError within a registered task with strict enabled. "
        def task():
            raise KeyError("This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.register(task)
        yield 1.0
        self.fail("Should not reach this")

    @call_on_dispersy_thread
    def test_KeyError_callback(self):
        " KeyError within a registered task. "
        def task():
            raise KeyError("This must be ignored")
        self.enable_strict = False
        self._dispersy.callback.register(task)
        yield 1.0
        self.assertTrue(True)

    @failure_to_success(RuntimeError, "Early shutdown")
    @call_on_dispersy_thread
    def test_assert_strict_callback_generator(self):
        " Assert within a registered generator task. "
        def task():
            yield 0.1
            yield 0.1
            self.assertTrue(False, "This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.register(task)
        yield 1.0
        self.fail("Should not reach this")

    @call_on_dispersy_thread
    def test_assert_callback_generator(self):
        " Assert within a registered generator task. "
        def task():
            yield 0.1
            yield 0.1
            self.assertTrue(False, "This must be ignored")
        self.enable_strict = False
        self._dispersy.callback.register(task)
        yield 1.0
        self.assertTrue(True)

    @failure_to_success(RuntimeError, "Early shutdown")
    @call_on_dispersy_thread
    def test_KeyError_strict_callback_generator(self):
        " KeyError within a registered generator task. "
        def task():
            yield 0.1
            yield 0.1
            raise KeyError("This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.register(task)
        yield 1.0
        self.fail("Should not reach this")

    @call_on_dispersy_thread
    def test_KeyError_callback_generator(self):
        " KeyError within a registered generator task. "
        def task():
            yield 0.1
            yield 0.1
            raise KeyError("This must be ignored")
        self.enable_strict = False
        self._dispersy.callback.register(task)
        yield 1.0
        self.assertTrue(True)

    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_assert_strict_callback_call(self):
        " Assert within a 'call' task. "
        def task():
            self.assertTrue(False, "This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_assert_callback_call(self):
        " Assert within a 'call' task. "
        def task():
            self.assertTrue(False, "This must fail")
        self.enable_strict = False
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(KeyError, "This must fail")
    @call_on_dispersy_thread
    def test_KeyError_strict_callback_call(self):
        " KeyError within a 'call' task. "
        def task():
            raise KeyError("This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(KeyError, "This must fail")
    @call_on_dispersy_thread
    def test_KeyError_callback_call(self):
        " KeyError within a 'call' task. "
        def task():
            raise KeyError("This must fail")
        self.enable_strict = False
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_assert_strict_callback_call_generator(self):
        " Assert within a 'call' generator task. "
        def task():
            yield 0.1
            yield 0.1
            self.assertTrue(False, "This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(AssertionError, "This must fail")
    @call_on_dispersy_thread
    def test_assert_callback_call_generator(self):
        " Assert within a 'call' generator task. "
        def task():
            yield 0.1
            yield 0.1
            self.assertTrue(False, "This must fail")
        self.enable_strict = False
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(KeyError, "This must fail")
    @call_on_dispersy_thread
    def test_KeyError_strict_callback_call_generator(self):
        " KeyError within a 'call' generator task. "
        def task():
            yield 0.1
            yield 0.1
            raise KeyError("This must fail")
        self.assertTrue(self.enable_strict)
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")

    @failure_to_success(KeyError, "This must fail")
    @call_on_dispersy_thread
    def test_KeyError_callback_call_generator(self):
        " KeyError within a 'call' generator task. "
        def task():
            yield 0.1
            yield 0.1
            raise KeyError("This must fail")
        self.enable_strict = False
        self._dispersy.callback.call(task)
        yield 1.0
        self.fail("Should not reach this")
