from __future__ import annotations

from typing import Any, Callable, Dict, Tuple, Union

from sisyphus.hash import sis_hash_helper
from sisyphus.tools import try_get


class DelayedBase:
    def __init__(self, a, b=None):
        self.a = a
        self.b = b

    @staticmethod
    def _is_set_helper(var):
        if isinstance(var, DelayedBase):
            if not var.is_set():
                return False
        return True

    def is_set(self):
        return self._is_set_helper(self.a) and self._is_set_helper(self.b)

    def __add__(self, other):
        return DelayedAdd(self, other)

    def __radd__(self, other):
        return DelayedAdd(other, self)

    def __sub__(self, other):
        return DelayedSub(self, other)

    def __rsub__(self, other):
        return DelayedSub(other, self)

    def __mul__(self, other):
        return DelayedMul(self, other)

    def __rmul__(self, other):
        return DelayedMul(other, self)

    def __truediv__(self, other):
        return DelayedTrueDiv(self, other)

    def __rtruediv__(self, other):
        return DelayedTrueDiv(other, self)

    def __floordiv__(self, other):
        return DelayedFloorDiv(self, other)

    def __rfloordiv__(self, other):
        return DelayedFloorDiv(other, self)

    def __mod__(self, other):
        return DelayedMod(self, other)

    def __pow__(self, other, modulo=None):
        if modulo is None:
            return DelayedPow(self, other)
        else:
            return DelayedMod(DelayedPow(self, other), modulo)

    def __rpow__(self, other):
        return DelayedPow(other, self)

    def __round__(self, n: Union[None, int, DelayedBase] = None):
        return DelayedRound(self, n)

    def __getitem__(self, key):
        return DelayedGetItem(self, key)

    def __call__(self, *args, **kwargs):
        return DelayedCall(self, args, kwargs)

    def format(self, *args, **kwargs):
        return DelayedFormat(self, *args, **kwargs)

    def rformat(self, fstring, *args, **kwargs):
        """Reverse format call e.g.:
        a.rformat(b, *args, **kwargs) is mapped to b.format(a, *args, **kwargs)"""
        return DelayedFormat(fstring, self, *args, **kwargs)

    def replace(self, *args, **kwargs):
        return DelayedReplace(self, *args, **kwargs)

    def function(self, func, *args, **kwargs):
        return DelayedFunction(self, func, *args, **kwargs)

    def fallback(self, fallback):
        """If this variable is not set yet the fallback value will be returned"""
        return DelayedFallback(self, fallback)

    def get(self):
        assert False, "This method needs to be declared in a child class"

    def __repr__(self):
        return str(self.get())

    def __str__(self):
        return str(self.get())


class DelayedAdd(DelayedBase):
    def get(self):
        return try_get(self.a) + try_get(self.b)


class DelayedSub(DelayedBase):
    def get(self):
        return try_get(self.a) - try_get(self.b)


class DelayedMul(DelayedBase):
    def get(self):
        return try_get(self.a) * try_get(self.b)


class DelayedTrueDiv(DelayedBase):
    def get(self):
        return try_get(self.a) / try_get(self.b)


class DelayedFloorDiv(DelayedBase):
    def get(self):
        return try_get(self.a) // try_get(self.b)


class DelayedMod(DelayedBase):
    def get(self):
        return try_get(self.a) % try_get(self.b)


class DelayedPow(DelayedBase):
    def get(self):
        return try_get(self.a) ** try_get(self.b)


class DelayedRound(DelayedBase):
    def get(self):
        return round(try_get(self.a), try_get(self.b))


class DelayedGetItem(DelayedBase):
    def get(self):
        return try_get(self.a)[try_get(self.b)]


class DelayedCall(DelayedBase):
    def __init__(
        self,
        func: Union[DelayedBase, Callable],
        args: Tuple[Union[DelayedBase, Any], ...],
        kwargs: Dict[str, Union[DelayedBase, Any]],
    ):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def get(self):
        func = try_get(self.func)
        args = [try_get(arg) for arg in self.args]
        kwargs = {key: try_get(value) for key, value in self.kwargs.items()}
        return func(*args, **kwargs)


class Delayed(DelayedBase):
    def __init__(self, a):
        self.a = a
        self.b = None

    def get(self):
        return try_get(self.a)

    def _sis_hash(self):
        return sis_hash_helper(self.a)


class DelayedFunctionBase(DelayedBase):
    """Base class to delays a function call until the get method is called"""

    def __init__(self, string, *args, **kwargs):
        self.string = string
        self.args = args
        self.kwargs = kwargs

    def is_set(self):
        return all(self._is_set_helper(var) for var in [self.string] + list(self.args) + list(self.kwargs.values()))


class DelayedFunction(DelayedFunctionBase):
    def __init__(self, string, func, *args, **kwargs):
        self.func = func
        assert func.__name__ != "<lambda>", "Hashing of lambda functions is not supported"
        super().__init__(string, *args, **kwargs)

    def get(self):
        return self.func(try_get(self.string), *self.args, **self.kwargs)


class DelayedFormat(DelayedFunctionBase):
    def get(self):
        return try_get(self.string).format(*(try_get(i) for i in self.args), **self.kwargs)


class DelayedReplace(DelayedFunctionBase):
    def get(self):
        return try_get(self.string).replace(*self.args, **self.kwargs)


class DelayedFallback(DelayedBase):
    """Return second value if first value is not available yet"""

    def get(self):
        if self.a.is_set():
            return self.a.get()
        else:
            return try_get(self.b)


class DelayedSlice(DelayedBase):
    def __init__(self, iterable, index_start=0, index_end=-1, step=1):
        """
        :param Iterable[Any] iterable:
        :param int|DelayedBase index_start:
        :param int|DelayedBase index_end:
        :param int|DelayedBase step:
        """
        self.iterable = iterable
        self.index_start = index_start
        self.index_end = index_end
        self.step = step

    def get(self):
        return try_get(self.iterable)[try_get(self.index_start) : try_get(self.index_end) : try_get(self.step)]


class DelayedJoin(DelayedBase):
    def __init__(self, iterable, separator):
        """

        :param Iterable[DelayedBase|str] iterable:
        :param str separator:
        """
        self.iterable = iterable
        self.separator = separator

    def get(self):
        return self.separator.join([try_get(obj) for obj in self.iterable])
