#-*-coding:utf-8-*-
"""
@package bshotgun.tests.test_sql
@brief tests for bshotgun.sql

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
__all__ = []

import sys
from time import time

import shotgun_api3

from .base import (ShotgunTestCase,
                   ReadOnlyTestSQLProxyShotgunConnection,
                   TestShotgunTypeFactory)

# test import *
from bshotgun import *


class TestShotgunSQL(ShotgunTestCase):
    __slots__ = ()
        
    def test_sql(self):
        """Check some SQL functionality with our SQL test database"""
        sg = ReadOnlyTestSQLProxyShotgunConnection()
        assert sg.has_database()
        
        # Write is disabled
        self.failUnlessRaises(AssertionError, sg.batch)
        
        # read disabled unless overridden
        self.failUnlessRaises(AssertionError, sg.schema_read)
        
        fac = TestShotgunTypeFactory()
        
        # Simple query
        sg_id = 612
        Asset = fac.type_by_name('Asset')
        data = sg.find_one(Asset.__name__, [('id', 'is', sg_id)], Asset.sg_schema.keys())
        assert isinstance(data, dict), "didn't get a value of correct type"
        
        entity = Asset(sg_id, data)
        assert entity.code.value() == 'Mortar_A'
        
        assert sg.find_one(Asset.__name__, [('id', 'is', 10)], ['id']) is None, 'invalid ids just yield None'
        
        
        total_record_count = 0
        fetch_count = 0
        tst = time()
        for type_name in fac.type_names():
            for filter in ([], [('id', 'is', sg_id)]):
                for limit in (0, 10):
                    Entity = fac.type_by_name(type_name)
                    # we can get empty data
                    st = time()
                    data = sg.find(Entity.__name__, filter, Entity.sg_schema.keys(), limit=limit)
                    total_record_count += len(data)
                    fetch_count += 1
                    if data:
                        sys.stdout.write("Fetched %i records in %fs" % (len(data), time() - st))
                    # end performance printing
                    
                    if limit:
                        assert len(data) <= limit
                    if limit or filter:
                        continue
                    
                    # just retrieve the value , to see it really works.
                    ######################
                    # remove for stress testing - this works, and is suffciently tested in orm/test_base.py
                    continue
                    
                    for edata in data:
                        node = Entity(edata['id'], edata)
                        for prop_name in Entity.sg_schema.keys():
                            getattr(node, prop_name).value()
                        # end for each property name
                    # end for each entity data block
                # end for each limit
            # end for each filter
        # end for each type
        sys.stdout.write("Received a total of %i records in %i fetches in %fs\n" % (total_record_count, fetch_count, time() - tst))
        
        
    
