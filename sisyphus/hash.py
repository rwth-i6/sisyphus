import hashlib
from inspect import isclass, isfunction


def md5(obj):
    """
    :param obj:
    :rtype: str
    """
    return hashlib.md5(str(obj).encode()).hexdigest()


def int_hash(obj):
    """
    :param object obj:
    :rtype: int
    """
    h = hashlib.sha256(sis_hash_helper(obj)).digest()
    return int.from_bytes(h, byteorder='big', signed=False)


def short_hash(obj,
               length=12,
               chars='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'):
    """
    :param object obj:
    :param int length:
    :param str|T chars:
    :rtype: str|T
    """
    h = hashlib.sha256(sis_hash_helper(obj)).digest()
    h = int.from_bytes(h, byteorder='big', signed=False)
    ls = []
    for i in range(length):
        ls.append(chars[int(h % len(chars))])
        h = h // len(chars)
    return ''.join(ls)


def get_object_state(obj):
    """
    Export current object status

    Comment: Maybe obj.__reduce__() is a better idea? is it stable for hashing?
    """

    if hasattr(obj, '__getnewargs_ex__'):
        args = obj.__getnewargs_ex__()
    elif hasattr(obj, '__getnewargs__'):
        args = obj.__getnewargs__()
    else:
        args = None

    if hasattr(obj, '__sis_state__'):
        state = obj.__sis_state__()
    elif hasattr(obj, '__getstate__'):
        state = obj.__getstate__()
    elif hasattr(obj, '__dict__'):
        state = obj.__dict__
    elif hasattr(obj, '__slots__'):
        state = {k: getattr(obj, k) for k in obj.__slots__ if hasattr(obj, k)}
    else:
        assert args is not None, "Failed to get object state of: %s" % repr(obj)
        state = None

    if args is None:
        return state
    else:
        return args, state


def is_namedtuple_instance(x):
    """
    Checks if x is a namedtuple (or looks very much like it)
    -> see https://stackoverflow.com/a/2166841 for details
    :param object x: object to check
    :rtype: bool
    """
    t = type(x)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, '_fields', None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


def sis_hash_helper(obj):
    """
    Takes most object and tries to convert the current state into bytes.

    :param object obj:
    :rtype: bytes
    """

    # Store type to ensure it's unique
    byte_list = [type(obj).__qualname__.encode()]

    # Using type and not isinstance to avoid derived types
    if isinstance(obj, bytes):
        byte_list.append(obj)
    elif obj is None:
        pass
    elif type(obj) in (int, float, bool, str, complex):
        byte_list.append(repr(obj).encode())
    elif type(obj) in (list, tuple) or is_namedtuple_instance(obj):
        byte_list += map(sis_hash_helper, obj)
    elif type(obj) in (set, frozenset):
        byte_list += sorted(map(sis_hash_helper, obj))
    elif isinstance(obj, dict):
        # sort items to ensure they are always in the same order
        byte_list += sorted(map(sis_hash_helper, obj.items()))
    elif isfunction(obj):
        # Handle functions
        # Not a nice way to check if the given function is a lambda function, but the best I found
        # assert not isinstance(lambda m: m, LambdaType) is true for all functions
        assert obj.__name__ != '<lambda>', "Hashing of lambda functions is not supported"
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__)))
    elif isclass(obj):
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__)))
    elif hasattr(obj, '_sis_hash'):
        # sis job or path object
        return obj._sis_hash()
    else:
        byte_list.append(sis_hash_helper(get_object_state(obj)))

    byte_str = b'(' + b', '.join(byte_list) + b')'
    if len(byte_str) > 4096:
        # hash long outputs to avoid arbitrary long return values. 4096 is just
        # picked because it looked good and not optimized,
        # it's most likely not that important.
        return hashlib.sha256(byte_str).digest()
    else:
        return byte_str
