#-*-coding:utf-8-*-
"""
@package bshotgun
@brief Initializes the shotgun database integration

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
from __future__ import absolute_import
from butility import Version
__version__ = Version('0.1.0')

from .base import *
from .sql import *
from .interfaces import *
from .schema import *
