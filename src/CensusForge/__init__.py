from importlib.metadata import version

from .CensusForge import CensusAPI

__all__ = ["CensusAPI"]

__version__ = version("CensusAPI")
