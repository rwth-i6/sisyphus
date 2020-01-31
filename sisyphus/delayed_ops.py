from sisyphus.hash import sis_hash_helper
from sisyphus.tools import try_get


class DelayedBase:
    def __init__(self, a, b):
        self.a = a
        self.b = b

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

    def __mod__(self, other):
        return DelayedMod(self, other)

    def __getitem__(self, key):
        return DelayedGetItem(self, key)

    def format(self, *args, **kwargs):
        return DelayedFormat(self, *args, **kwargs)

    def replace(self, *args, **kwargs):
        return DelayedReplace(self, *args, **kwargs)

    def function(self, func, *args, **kwargs):
        return DelayedFunction(self, func, *args, **kwargs)

    def get(self):
        assert False, 'This method needs to be declared in a child class'

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


class DelayedMod(DelayedBase):
    def get(self):
        return try_get(self.a) % try_get(self.b)


class DelayedGetItem(DelayedBase):
    def get(self):
        return try_get(self.a)[self.b]


class Delayed(DelayedBase):
    def __init__(self, a):
        self.a = a

    def get(self):
        return try_get(self.a)

    def _sis_hash(self):
        return sis_hash_helper(self.a)


class DelayedFunctionBase(DelayedBase):
    """ Base class to delays a function call until the get method is called """

    def __init__(self, string, *args, **kwargs):
        self.string = string
        self.args = args
        self.kwargs = kwargs


class DelayedFunction(DelayedFunctionBase):
    def __init__(self, string, func, *args, **kwargs):
        self.func = func
        assert func.__name__ != '<lambda>', "Hashing of lambda functions is not supported"
        super().__init__(string, *args, **kwargs)

    def get(self):
        return self.func(try_get(self.string), *self.args, **self.kwargs)


class DelayedFormat(DelayedFunctionBase):
    def get(self):
        return try_get(self.string).format(*self.args, **self.kwargs)


class DelayedReplace(DelayedFunctionBase):
    def get(self):
        return try_get(self.string).replace(*self.args, **self.kwargs)
