#-*-coding:utf-8-*-
"""
@package bshotgun.tests.test_base
@brief tests for bshotgun.base

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
__all__ = []

import shotgun_api3
from .base import ShotgunTestCase

# test import *
from bshotgun import *
from shotgun_api3 import Fault


class TestShotgun(ShotgunTestCase):
    __slots__ = ()
    

    def test_base(self):
        """verify shotgun connection with a mock - for now there is no real datbase access to not slow down anything"""
        sg = ProxyShotgunConnection()
        self.failUnlessRaises(Fault, svc.find_one, 'Foo', [('id', 'is', 1)])
        
    

