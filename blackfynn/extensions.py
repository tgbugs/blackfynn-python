import sys


# try to import optional packages
try:
    import pandas
except ImportError:
    pd = None

try:
    import numpy
except ImportError:
    np = None


class ExtensionsException(Exception):
    pass

def raise_exception():
    # TODO: figure out name for extensions
    raise AgentException("""This command require optional dependencies. To install, run:

pip install blackfynn[XXX]

""")

def check_extension(exception=True):
    """Makes sure that appropriate packages are installed"""
    okay = True
    if "pandas" not in sys.modules:
        okay = False
    if "numpy" not in sys.modules:
        okay = False

    if exception:
        raise_exception()

    return okay
