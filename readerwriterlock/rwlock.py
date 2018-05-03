#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read Write Lock."""

import threading
import time

from typing import Optional
from types import TracebackType
import typing


class RWLockRead(object):
    """A Read/Write lock giving preference to Reader."""

    def __init__(self, lock_factory: typing.Callable = threading.Lock) -> None:
        """Init."""
        self.read_count = 0
        self.resource = lock_factory()
        self.lock_read_count = lock_factory()

    class _aReader(object):
        def __init__(self, RWLock: "RWLockRead") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
            deadline = None if timeout is None else (time.time() + timeout)
            if not self._rw_lock.lock_read_count.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                return False
            self._rw_lock.read_count += 1
            if 1 == self._rw_lock.read_count:
                if not self._rw_lock.resource.acquire(
                    blocking=True,
                    timeout=-1 if deadline is None else max(0, deadline - time.time())
                ):
                    self._rw_lock.read_count -= 1
                    self._rw_lock.lock_read_count.release()
                    return False
            self._rw_lock.lock_read_count.release()
            self._locked = True
            return True

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.lock_read_count.acquire()
            self._rw_lock.read_count -= 1
            if 0 == self._rw_lock.read_count:
                self._rw_lock.resource.release()
            self._rw_lock.lock_read_count.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    class _aWriter(object):
        def __init__(self, RWLock: "RWLockRead") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            self._locked = self._rw_lock.resource.acquire(blocking, timeout)
            return self._locked

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.resource.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    def gen_rlock(self) -> "RWLockRead._aReader":
        """Generate a reader lock."""
        return RWLockRead._aReader(self)

    def gen_wlock(self) -> "RWLockRead._aWriter":
        """Generate a writer lock."""
        return RWLockRead._aWriter(self)


class RWLockWrite(object):
    """A Read/Write lock giving preference to Writer."""

    def __init__(self, lock_factory: typing.Callable = threading.Lock) -> None:
        """Init."""
        self.read_count = 0
        self.write_count = 0
        self.lock_read_count = lock_factory()
        self.lock_write_count = lock_factory()
        self.lock_read_entry = lock_factory()
        self.lock_read_try = lock_factory()
        self.resource = lock_factory()

    class _aReader(object):
        def __init__(self, RWLock: "RWLockWrite") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
            deadline = None if timeout is None else (time.time() + timeout)
            if not self._rw_lock.lock_read_entry.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                return False
            if not self._rw_lock.lock_read_try.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                self._rw_lock.lock_read_entry.release()
                return False
            if not self._rw_lock.lock_read_count.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                self._rw_lock.lock_read_try.release()
                self._rw_lock.lock_read_entry.release()
                return False
            self._rw_lock.read_count += 1
            if 1 == self._rw_lock.read_count:
                if not self._rw_lock.resource.acquire(
                    blocking=True,
                    timeout=-1 if deadline is None else max(0, deadline - time.time())
                ):
                    self._rw_lock.lock_read_try.release()
                    self._rw_lock.lock_read_entry.release()
                    self._rw_lock.read_count -= 1
                    self._rw_lock.lock_read_count.release()
                    return False
            self._rw_lock.lock_read_count.release()
            self._rw_lock.lock_read_try.release()
            self._rw_lock.lock_read_entry.release()
            self._locked = True
            return True

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.lock_read_count.acquire()
            self._rw_lock.read_count -= 1
            if 0 == self._rw_lock.read_count:
                self._rw_lock.resource.release()
            self._rw_lock.lock_read_count.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    class _aWriter(object):
        def __init__(self, RWLock: "RWLockWrite") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
            deadline = None if timeout is None else (time.time() + timeout)
            if not self._rw_lock.lock_write_count.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                return False
            self._rw_lock.write_count += 1
            if 1 == self._rw_lock.write_count:
                if not self._rw_lock.lock_read_try.acquire(
                    blocking=True,
                    timeout=-1 if deadline is None else max(0, deadline - time.time())
                ):
                    self._rw_lock.write_count -= 1
                    self._rw_lock.lock_write_count.release()
                    return False
            self._rw_lock.lock_write_count.release()
            if not self._rw_lock.resource.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                self._rw_lock.lock_write_count.acquire()
                self._rw_lock.write_count -= 1
                if 0 == self._rw_lock.write_count:
                    self._rw_lock.lock_read_try.release()
                self._rw_lock.lock_write_count.release()
                return False
            self._locked = True
            return True

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.resource.release()
            self._rw_lock.lock_write_count.acquire()
            self._rw_lock.write_count -= 1
            if 0 == self._rw_lock.write_count:
                self._rw_lock.lock_read_try.release()
            self._rw_lock.lock_write_count.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    def gen_rlock(self) -> "RWLockWrite._aReader":
        """Generate a reader lock."""
        return RWLockWrite._aReader(self)

    def gen_wlock(self) -> "RWLockWrite._aWriter":
        """Generate a writer lock."""
        return RWLockWrite._aWriter(self)


class RWLockFair(object):
    """A Read/Write lock giving fairness to both Reader and Writer."""

    def __init__(self, lock_factory: typing.Callable = threading.Lock) -> None:
        """Init."""
        self.read_count = 0
        self.lock_read_count = lock_factory()
        self.lock_read = lock_factory()
        self.lock_write = lock_factory()

    class _aReader(object):
        def __init__(self, RWLock: "RWLockFair") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
            deadline = None if timeout is None else (time.time() + timeout)
            if not self._rw_lock.lock_read.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                return False
            if not self._rw_lock.lock_read_count.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                self._rw_lock.lock_read.release()
                return False
            self._rw_lock.read_count += 1
            if 1 == self._rw_lock.read_count:
                if not self._rw_lock.lock_write.acquire(
                    blocking=True,
                    timeout=-1 if deadline is None else max(0, deadline - time.time())
                ):
                    self._rw_lock.read_count -= 1
                    self._rw_lock.lock_read_count.release()
                    self._rw_lock.lock_read.release()
                    return False
            self._rw_lock.lock_read_count.release()
            self._rw_lock.lock_read.release()
            self._locked = True
            return True

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.lock_read_count.acquire()
            self._rw_lock.read_count -= 1
            if 0 == self._rw_lock.read_count:
                self._rw_lock.lock_write.release()
            self._rw_lock.lock_read_count.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    class _aWriter(object):
        def __init__(self, RWLock: "RWLockFair") -> None:
            self._rw_lock = RWLock
            self._locked = False

        def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
            """Acquire a lock."""
            timeout = None if (blocking and timeout < 0) else (timeout if blocking else 0)
            deadline = None if timeout is None else (time.time() + timeout)
            if not self._rw_lock.lock_read.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                return False
            if not self._rw_lock.lock_write.acquire(
                blocking=True,
                timeout=-1 if deadline is None else max(0, deadline - time.time())
            ):
                self._rw_lock.lock_read.release()
                return False
            self._locked = True
            return True

        def release(self) -> None:
            """Release the lock."""
            if not self._locked: raise RuntimeError("cannot release un-acquired lock")
            self._locked = False
            self._rw_lock.lock_write.release()
            self._rw_lock.lock_read.release()

        def locked(self) -> bool:
            """Answer to 'is file locked?'."""
            return self._locked

        def __enter__(self) -> None:
            self.acquire()

        def __exit__(self, exc_type, exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
            self.release()
            return False

    def gen_rlock(self) -> "RWLockFair._aReader":
        """Generate a reader lock."""
        return RWLockFair._aReader(self)

    def gen_wlock(self) -> "RWLockFair._aWriter":
        """Generate a writer lock."""
        return RWLockFair._aWriter(self)
