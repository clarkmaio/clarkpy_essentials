

from functools import wraps
from copy import deepcopy


def force_kwargs(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args:
            raise RuntimeError("Arguments must be passed as keyword arguments.")
        return func(*args, **kwargs)
    return wrapper

def deepcopy_args(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        deepcopied_args = tuple(deepcopy(arg) for arg in args)
        deepcopied_kwargs = {k: deepcopy(v) for k, v in kwargs.items()}
        return func(*deepcopied_args, **deepcopied_kwargs)
    return wrapper