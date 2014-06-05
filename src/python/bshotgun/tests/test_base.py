#-*-coding:utf-8-*-
"""
@package bshotgun.tests.test_base
@brief tests for bshotgun.base

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
__all__ = []

from copy import deepcopy
from .base import (ShotgunTestCase,
                   ShotgunConnectionMock)

# test import *
from bshotgun import *
from bapp.tests import with_application


class TestShotgun(ShotgunTestCase):
    __slots__ = ()
    

    @with_application(from_file=__file__)
    def test_base(self):
        """verify shotgun connection with a mock - for now there is no real datbase access to not slow down anything"""
        sg = ProxyShotgunConnection()
        # we are not configured
        self.failUnlessRaises(ValueError, sg.find_one, 'Foo', [('id', 'is', 1)])

    def test_mock(self):
        """tests for test-tools"""
        sg = ShotgunConnectionMock()

        p =       {      'type': 'Project',
                         'id': 1,
                         'tank_name': None,
                         'name': 'project_name' }

        ls = {'code' : 'foo',
                         'mac_path' : 'this',
                         'linux_path' : 'this',
                         'windows_path' : 'this',
                         'id' : 1,
                         'type' : 'LocalStorage'}
        ls2 = deepcopy(ls)
        ls2['id'] = 5
        sg.set_entities([p, ls, ls2])

        assert sg.find_one('Project', [['id', 'is', 1]])
        assert len(sg.find('LocalStorage', list())) == 2

        assert isinstance(sg.server_info, dict)
        assert isinstance(sg.schema_read(), dict)

        sg.set_entity_schema('Project', dict(name = 42))
        assert sg.schema_field_read('Project', 'name') == {'name' : 42}


        sg.update('Project', 1, {'newval' : 1, 'tank_name' : 'hi'})
        assert p['newval'] == 1 and p['tank_name'] == 'hi'

        del p['type']
        newp = sg.create('Project', p)
        assert newp is not p and 'id' in newp and 'type' in newp
        assert newp['id'] != p['id']

    

