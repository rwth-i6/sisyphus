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


def sis_hash_helper(obj, _visited=None, _add_to_visited=True):
    """
    Takes most object and tries to convert the current state into bytes.

    :param object obj:
    :param _visited: (internal use)
    :param _add_to_visited: (internal use)
    :rtype: bytes
    """
    if _visited is None:
        # keep ref to obj alive to avoid having the same id for different objs
        _visited = {}  # id -> (bytes, obj)
    if id(obj) in _visited:
        return _visited[id(obj)][0]

    # Store type to ensure it's unique
    byte_list = [type(obj).__qualname__.encode()]

    # Using type and not isinstance to avoid derived types
    if isinstance(obj, bytes):
        byte_list.append(obj)
    elif obj is None:
        pass
    elif type(obj) in (int, float, bool, str, complex):
        byte_list.append(repr(obj).encode())
    elif type(obj) in (list, tuple):
        byte_list += [sis_hash_helper(x, _visited=_visited) for x in obj]
    elif type(obj) in (set, frozenset):
        byte_list += sorted(sis_hash_helper(x, _visited=_visited) for x in obj)
    elif isinstance(obj, dict):
        # sort items to ensure they are always in the same order
        byte_list += sorted(
            sis_hash_helper(
                x, _visited=_visited, _add_to_visited=False)  # tuple is temp object, don't store
            for x in obj.items())
    elif isfunction(obj):
        # Handle functions
        # Not a nice way to check if the given function is a lambda function, but the best I found
        # assert not isinstance(lambda m: m, LambdaType) is true for all functions
        assert obj.__name__ != '<lambda>', "Hashing of lambda functions is not supported"
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__), _visited=_visited))
    elif isclass(obj):
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__), _visited=_visited))
    elif hasattr(obj, '_sis_hash'):
        # sis job or path object
        return obj._sis_hash()
    else:
        byte_list.append(sis_hash_helper(get_object_state(obj), _visited=_visited))

    byte_str = b'(' + b', '.join(byte_list) + b')'
    if len(byte_str) > 4096:
        # hash long outputs to avoid arbitrary long return values. 4096 is just
        # picked because it looked good and not optimized,
        # it's most likely not that important.
        byte_str = hashlib.sha256(byte_str).digest()
    if _add_to_visited:
        _visited[id(obj)] = (byte_str, obj)
    return byte_str
