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
from bapp.tests import with_application


class TestShotgun(ShotgunTestCase):
    __slots__ = ()
    

    @with_application(from_file=__file__)
    def test_base(self):
        """verify shotgun connection with a mock - for now there is no real datbase access to not slow down anything"""
        sg = ProxyShotgunConnection()
        # we are not configured
        self.failUnlessRaises(ValueError, sg.find_one, 'Foo', [('id', 'is', 1)])
        
    

