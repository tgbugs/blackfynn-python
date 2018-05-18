import sys


# try to import optional packages
try:
    import pandas
except ImportError:
    pandas = None

try:
    import numpy
except ImportError:
    numpy = None


class ExtensionsException(Exception):
    pass

def raise_exception():
    # TODO: figure out name for extensions
    raise ExtensionsException("""This command require optional dependencies. To install, run:

pip install blackfynn[XXX]

""")


def check_extension(exception=True):
    """
        Make sure that appropriate packages are installed
        This function will raise an exception if required modules is not found.
        Guard a function invocation by calling this function
        at the start of the function
    """
    okay = True
    if "pandas" not in sys.modules:
        okay = False
    if "numpy" not in sys.modules:
        okay = False

    if not okay:
        raise_exception()

    return okay
