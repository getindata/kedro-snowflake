import inspect


def identity(x):
    return x


def get_arg_type(fn, arg_position: int = 0):
    """
    Returns the type of the argument at position `arg_position` of `fn`.
    """
    sig = inspect.signature(fn)
    arg_type = sig.parameters[list(sig.parameters.keys())[arg_position]].annotation
    return arg_type
