from typing import Tuple
import enum
import hashlib
import pathlib
from inspect import isclass, isfunction, ismemberdescriptor


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
    return int.from_bytes(h, byteorder="big", signed=False)


def short_hash(obj, length=12, chars="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"):
    """
    :param object obj:
    :param int length:
    :param str|T chars:
    :rtype: str|T
    """
    h = hashlib.sha256(sis_hash_helper(obj)).digest()
    h = int.from_bytes(h, byteorder="big", signed=False)
    ls = []
    for i in range(length):
        ls.append(chars[int(h % len(chars))])
        h = h // len(chars)
    return "".join(ls)


_BasicTypes: Tuple[type, ...] = (int, float, bool, str, complex)
_BasicSeqTypes: Tuple[type, ...] = (list, tuple)
_BasicSetTypes: Tuple[type, ...] = (set, frozenset)
_BasicDictTypes: Tuple[type, ...] = (dict,)
_BasicTypesCombined: Tuple[type, ...] = _BasicTypes + _BasicSeqTypes + _BasicSetTypes + _BasicDictTypes


def get_object_state(obj):
    """
    Export current object status

    Comment: Maybe obj.__reduce__() is a better idea? is it stable for hashing?
    """

    # Note: sis_hash_helper does not call get_object_state in these cases.
    # However, other code might (e.g. extract_paths),
    # so we keep consistent to the behavior of sis_hash_helper.
    if obj is None:
        return None
    if isinstance(obj, _BasicTypesCombined):
        for type_ in _BasicTypesCombined:
            if isinstance(obj, type_):
                if type(obj) is type_:
                    return obj
                else:
                    # This is a derived type. E.g. consider a namedtuple or np.float.
                    # We want to return the basic type, to break any potential recursion.
                    return type(obj)
        assert False, f"should not get here, obj {obj!r} type {type(obj)!r}"
    if isfunction(obj) or isclass(obj):
        return obj.__module__, obj.__qualname__

    if isinstance(obj, pathlib.PurePath):
        # pathlib paths have a somewhat technical internal state
        # ('_drv', '_root', '_parts', '_str', '_hash', '_pparts', '_cached_cparts'),
        # so we don't want to rely on this, but instead just use the string representation as state.
        # https://github.com/rwth-i6/sisyphus/pull/208#issuecomment-2405560718
        return str(obj)

    if hasattr(obj, "__getnewargs_ex__"):
        args = obj.__getnewargs_ex__()
    elif hasattr(obj, "__getnewargs__"):
        args = obj.__getnewargs__()
    else:
        args = None

    if hasattr(obj, "__sis_state__"):
        state = obj.__sis_state__()
    # Note: Since Python 3.11, there is a default object.__getstate__.
    # However, this default object.__getstate__ is not correct for some native types, e.g. _functools.partial.
    # https://github.com/rwth-i6/sisyphus/issues/207
    # https://github.com/python/cpython/issues/125094
    # Thus, only use __getstate__ if it is not the default object.__getstate__.
    elif hasattr(obj, "__getstate__") and obj.__class__.__getstate__ is not getattr(object, "__getstate__", None):
        state = obj.__getstate__()
    else:
        state = _getmembers(obj)
        if not state and not hasattr(obj, "__dict__") and not hasattr(obj, "__slots__"):
            # Keep compatibility with old behavior.
            assert args is not None, "Failed to get object state of: %s" % repr(obj)
            state = None

    if isinstance(obj, enum.Enum):
        assert isinstance(state, dict)
        # In Python >=3.11, keep hash same as in Python <=3.10, https://github.com/rwth-i6/sisyphus/issues/188
        state.pop("_sort_order_", None)

    if args is None:
        return state
    else:
        return args, state


def sis_hash_helper(obj):
    """
    Takes most object and tries to convert the current state into bytes.

    :param object obj:
    :rtype: bytes
    """

    # Store type to ensure it's unique
    byte_list = [_obj_type_qualname(obj)]

    # Using type and not isinstance to avoid derived types
    if isinstance(obj, bytes):
        byte_list.append(obj)
    elif obj is None:
        pass
    # Note: Using type(obj) in _Types instead of isinstance(obj, _Types)
    # because of historical reasons (and we cannot change this now).
    # For derived types (e.g. namedtuple, np.float), it is then handled by get_object_state.
    # That's why the handling of get_object_state for those types is important.
    elif type(obj) in _BasicTypes:
        byte_list.append(repr(obj).encode())
    elif type(obj) in _BasicSeqTypes:
        byte_list += map(sis_hash_helper, obj)
    elif type(obj) in _BasicSetTypes:
        byte_list += sorted(map(sis_hash_helper, obj))
    elif isinstance(obj, _BasicDictTypes):
        # sort items to ensure they are always in the same order
        byte_list += sorted(map(sis_hash_helper, obj.items()))
    elif isfunction(obj):
        # Handle functions
        # Not a nice way to check if the given function is a lambda function, but the best I found
        # assert not isinstance(lambda m: m, LambdaType) is true for all functions
        assert obj.__name__ != "<lambda>", "Hashing of lambda functions is not supported"
        assert obj.__module__ != "__main__", "Hashing of functions defined in __main__ is not supported"
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__)))
    elif isclass(obj):
        assert obj.__module__ != "__main__", "Hashing of classes defined in __main__ is not supported"
        byte_list.append(sis_hash_helper((obj.__module__, obj.__qualname__)))
    elif hasattr(obj, "_sis_hash"):
        # sis job or path object
        return obj._sis_hash()
    else:
        byte_list.append(sis_hash_helper(get_object_state(obj)))

    byte_str = b"(" + b", ".join(byte_list) + b")"
    if len(byte_str) > 4096:
        # hash long outputs to avoid arbitrary long return values. 4096 is just
        # picked because it looked good and not optimized,
        # it's most likely not that important.
        return hashlib.sha256(byte_str).digest()
    else:
        return byte_str


def _obj_type_qualname(obj) -> bytes:
    if type(obj) is enum.EnumMeta:  # EnumMeta is old alias for EnumType
        # In Python >=3.11, keep hash same as in Python <=3.10, https://github.com/rwth-i6/sisyphus/issues/188
        return b"EnumMeta"
    return type(obj).__qualname__.encode()


def _getmembers(obj):
    res = {}
    if hasattr(obj, "__dict__"):
        res.update(obj.__dict__)
    if hasattr(obj, "__slots__"):
        for key in obj.__slots__:
            try:
                res[key] = getattr(obj, key)
            except AttributeError:
                pass
    # Note, there are cases where `__dict__` or `__slots__` don't contain all attributes,
    # e.g. for some native types, e.g. _functools.partial.
    # (https://github.com/rwth-i6/sisyphus/issues/207)
    # `dir()` usually still lists those attributes.
    # However, to keep the behavior as before, we only want to return the object attributes here,
    # not the class attributes.
    cls_dict = {}
    for cls in reversed(type(obj).__mro__):
        if getattr(cls, "__dict__", None):
            cls_dict.update(cls.__dict__)
    for key in dir(obj):
        if key.startswith("__"):
            continue
        if key in res:
            continue
        # Get class attribute first, to maybe skip descriptors.
        if key in cls_dict:
            cls_value = cls_dict[key]
            if hasattr(cls_value, "__get__"):  # descriptor
                # descriptor are e.g. properties, bound methods, etc. We don't want to have those.
                # But member descriptors are usually for slots (even for native types without __slots__),
                # so that is why we keep them.
                if not ismemberdescriptor(cls_value):
                    continue
        try:
            value = getattr(obj, key)
        except AttributeError:
            # dir might not be reliable. just skip this
            continue
        if key in cls_dict and cls_dict[key] is value:
            continue  # this is a class attribute
        res[key] = value
    return res
