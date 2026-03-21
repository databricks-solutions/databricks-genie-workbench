try:
    from ._version import version
except ModuleNotFoundError:
    version = "0.0.0-dev"

__version__ = version
