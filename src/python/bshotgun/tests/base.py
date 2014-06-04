#-*-coding:utf-8-*-
"""
@package bshotgun.tests.base
@brief Some basic testing utilities

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
__all__ = ['ShotgunTestDatabase', 'ReadOnlyTestSQLProxyShotgunConnection', 'ShotgunTestCase', 
           'TestShotgunTypeFactory', 'ShotgunConnectionMock']

import json
import marshal
import time
import sys
import os
import zlib
from copy import deepcopy


from butility.tests import TestCase
from butility import Path
from bshotgun import (ProxyShotgunConnection,
                      SQLProxyShotgunConnection,
                      ProxyMeta)
from bshotgun.orm import ShotgunTypeFactory
from mock import Mock


DEFAULT_DB_SAMPLE='scrambled-ds1'


def dataset_tree(sample_name):
    """@return Path containing all data of a particular sample"""
    return ShotgunTestCase.sample_root(sample_name)


class ShotgunTestCase(TestCase):
    """Base for all bshotgun test cases"""
    __slots__ = ()

    fixture_root = Path(__file__).dirname()

    # -------------------------
    ## @name Interface
    # @{
    
    @classmethod
    def sample_root(cls, sample_name):
        """@return a path at which all test-sample data is located"""
        return cls.fixture_path('samples/%s' % sample_name)

    ## -- End Interface -- @}

# end class ShotgunTestCase


class TestShotgunTypeFactory(ShotgunTypeFactory):
    """A factory that supports a sample name, leading to the path from which to read samples.
    It belongs to the ShotgunTestDatabase, as it will read the schema part.
    In Theory, both could be combined, but they seem quite orthogonal to me
    """
    __slots__ = ('_sample_name')

    SCHEMA_SUBDIR = 'schema.jsonz'

    def __init__(self, *args, **kwargs):
        self._sample_name = kwargs.pop('sample_name', DEFAULT_DB_SAMPLE)
        super(TestShotgunTypeFactory, self).__init__(*args, **kwargs)

    def _schema_path(self, type_name):
        return self.schema_tree(self._sample_name) / ('%s%s' % (type_name, self.SCHEMA_FILE_EXTENSION))

    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def schema_tree(cls, sample_name):
        """@return path for schema for given sample, use with update_schema()"""
        return dataset_tree(sample_name) / cls.SCHEMA_SUBDIR
    
    ## -- End Interface -- @}

# end class TestShotgunTypeFactory


class ShotgunTestDatabase(object):
    """A test database that can be used for testing read-only, but that can be writte in memory.
    Every time a value is queried, it will be deserialized from disk.
    
    It provides automatic creation of an intermediate 'fast' cache  which is faster to read
    """
    __slots__ = (
                    '_use_records_cache',
                    '_sample_name',
                )
    
    DATASET_SUBDIR = 'data.jsonz'
    EVAR_NO_CACHE = 'BSHOTGUN_TESTS_NO_DATASET_CACHE'
    
    def __init__(self, use_records_cache = True, sample_name=DEFAULT_DB_SAMPLE):
        """Initialize this instance
        @param use_records_cache if True, when 'records' are queried the first time, a fast cache file
        of the records will be created. It loads 60 times faster than json
        @param sample_name name of the sample in our fixtures database"""
        self._use_records_cache = use_records_cache
        if self.EVAR_NO_CACHE in os.environ:
            self._use_records_cache = False
        # end turn off cache to possibly save time
        self._sample_name = sample_name

    def _record_storage_path(self, type_name):
        """@return path to shotgun storage data"""
        return self.data_tree(self._sample_name) / ('%s.json.z' % type_name)
        
    def _record_fast_storage_path(self, type_name):
        """@return path to file for storing records in a fast cache format"""
        return self._record_storage_path(type_name) + '.marshal_tmp'
        
    @classmethod
    def _serialize_records(cls, path, records):
        """Write all records to the given path"""
        open(path, 'w').write(zlib.compress(json.dumps(records, indent=4, separators=(',', ': '), default=str), 9))
        
    @classmethod
    def _deserialize_records(cls, path):
        """@return deserialized shotgun records"""
        return json.loads(zlib.decompress(open(path).read()))
        
        
    # -------------------------
    ## @name Initialization
    # @{
    
    def rebuild_database(self, type_names, fetcher):
        """Retrieve all data for all known entity types from some source shotgun.
        @param type_names an iterable of types to fetch from fetcher
        @param fetcher f(type_name) => [entity_data,...], whereas entity_data is a dict of values
        matching the entity-schema
        @note we don't write the schema - this should be done 
        @note may overwrite existing data"""
        for dir in (self.data_tree(self._sample_name), TestShotgunTypeFactory.schema_tree(self._sample_name)):
            try:
                dir.makedirs()
            except OSError:
                pass
            # end ignore it may exist
        # end for each dir to create

        for type_name in type_names:
            path = self._record_storage_path(type_name)
            sys.stderr.write("Dumping '%s' data to %s ...\n" % (type_name, path))
            st = time.time()
            records = fetcher(type_name)
            self._serialize_records(path, records)
            sys.stderr.write("Obtained %i '%s' records in %fs\n" % (len(records), type_name, time.time() - st))
        # end for each type to read
    
    ## -- End Initialization -- @}
        

    # -------------------------
    ## @name Interface
    # @{

    @classmethod
    def data_tree(cls, sample_name):
        """@return directory to our data set"""
        return dataset_tree(sample_name) / cls.DATASET_SUBDIR

    def exists(self):
        """@return True if the dataset seems to exist"""
        return dataset_tree(self._sample_name).isdir()
    
    def has_records(self, type_name):
        """@return True if records exist for the given shotgun data type
        @param type_name the Shotgun entity type, like 'Asset'"""
        return self._record_storage_path(type_name).isfile()
    
    def records(self, type_name):
        """@return a list of dictionaries in a format you would receive when queried using the API.
        It will be owned by you, as it is freshly deserialized
        @param type_name the Shotgun type name, like 'Asset'
        @note check for has_records beforehand"""
        if not self.has_records(type_name):
            return list()
        # end 
        ppath = self._record_fast_storage_path(type_name)
        st = time.time()
        cache_type = 'json'
        if self._use_records_cache and ppath.isfile():
            records = marshal.load(open(ppath))
            cache_type = 'fast'
        else:
            records = self._deserialize_records(self._record_storage_path(type_name))
            if self._use_records_cache and not ppath.isfile():
                rst = time.time()
                marshal.dump(records, open(ppath, 'w'))
                sys.stderr.write("Wrote %i '%s' records in %ss into fast cache\n" % (len(records), type_name, time.time() - rst))
            # end update pickle cache
        # end load pickle cache
        args = (type_name, cache_type, len(records), time.time() - st)
        sys.stdout.write("Loaded '%s' dataset(%s) with %i records in %fs\n" % args)
        return records
        
    def serialize_records(self, type_name, records):
        """Create or udpate a serialized version of records, which match the give shotgun type_name
        @return self"""
        st = time.time()
        self._serialize_records(self._record_storage_path(type_name))
        sys.stderr.write("Serialized %i '%s' records in %fs\n" % (len(records), type_name, time.time() - st))
        return self
                
    
    ## -- End Interface -- @}
    
# end class ShotgunTestDatabase


class ReadOnlyProxyMeta(ProxyMeta):
    """Non-Read-only methods will cause an exception, when called. This prevents write calls from 
    leaving the test-bed"""
    __slots__ = ()

    @classmethod
    def _create_method(cls, method_name, is_readonly, proxy_attr):
        """@return a new method named method_name that does not alter it's instance
        @note all additional arguments are mainly for your information
        @param cls this metaclass instance
        @param method_name name of method that is to be created
        @param is_readonly if True, the method must not change the underlying object
        @param proxy_attr the name of the attribute on instance that keeps the proxy instance."""
        if is_readonly:
            # don't override methods in our direct base
            if method_name in SQLProxyShotgunConnection.__dict__:
                return SQLProxyShotgunConnection.__dict__[method_name]
            return super(ReadOnlyProxyMeta, cls)._create_method(method_name, is_readonly, proxy_attr)
        else:
            def write_not_permitted(*args, **kwargs):
                raise AssertionError("%s: Cannot change database in test-mode" % method_name)
            return write_not_permitted
        # end handle read-onlyness
    ## -- End Subclass Interface -- @}

# end class ReadOnlyProxyMeta


class ReadOnlyTestSQLProxyShotgunConnection(SQLProxyShotgunConnection):
    """A front-end to the normal SQLProxyShotgunConnection which simply skips writes 
    and helps to auto-generate it's underlying SQL database"""
    __slots__ = ('_sample_name')
    __metaclass__ = ReadOnlyProxyMeta
    
    
    def __init__(self, db_url = None, sample_name=DEFAULT_DB_SAMPLE):
        """Initialize ourselves, making sure we read from our own database, if needed."""
        # If we don't yet have a test-database, rebuild it automatically
        # Must happen before super class is initialized, as it will create a base file right away
        self._sample_name = sample_name
        if not self.has_database():
            self.rebuild_database()
        # end handle rebuild on demand
        
        
        if db_url is None:
            db_url = self._sqlite_rodb_url()
        super(ReadOnlyTestSQLProxyShotgunConnection, self).__init__(db_url)
        
    def _set_cache_(self, name):
        if name == '_proxy':
            assert False, "Cannot use shotgun proxy, please check your test to assure it doesn't trigger it"
        else:
            super(ReadOnlyTestSQLProxyShotgunConnection, self)._set_cache_(name)
        #end handle engine instantiation
        
    
    def _sqlite_rodb_url(self):
        """@return an sqlalchemy compatible engine URL to our local READ-ONLY database
        It will be created on the fly and must not be checked in"""
        return 'sqlite:///%s' % self._sqlite_rodb_path() 
        
    def _sqlite_rodb_path(self):
        """@return a path to the designated sqlite database"""
        return ShotgunTestCase.fixture_path('sqlite.%s.db_tmp' % self._sample_name)
    
    # -------------------------
    ## @name Test Database Initialization
    # @{
    
    def has_database(self):
        """@return True if our database is initialized. If not, rebuild_database() should be called"""
        return self._sqlite_rodb_path().isfile()
        
    def rebuild_database(self):
        """Build our read-only test database from scratch, using data from the ShotgunTestDatabase
        @return a new instance of our type"""
        sqlite_path = self._sqlite_rodb_path()
        if sqlite_path.isfile():
            sqlite_path.remove()
        #end clear previous database
        
        fac = TestShotgunTypeFactory(sample_name=self._sample_name)
        fetcher = ShotgunTestDatabase().records
        
        return type(self)(SQLProxyShotgunConnection.init_database(self._sqlite_rodb_url(), fac, fetcher)._meta)
    
    ## -- End Test Database Initialization -- @}

# end class ReadOnlyTestSQLProxyShotgunConnection


class ShotgunConnectionMock(Mock):
    """Mocks a shotgun connection to some extend, yielding information of an in-memory shotgun database
    @note This mock currently is only good for well-behaving clients who supply valid information. We will not 
    reproduce exceptions as created by the actual implementation
    @note this implementation is based on the sgtk implementation at 
    https://github.com/shotgunsoftware/tk-core/blob/master/tests/python/tank_test/tank_test_base.py#L343"""
    _slots_ = ('_db', '_schema', '_server_info')

    def __init__ (self, *args, **kwargs):
        super(ShotgunConnectionMock, self).__init__(*args, **kwargs)
        self.clear()

    def _get_child_mock(self, **kwargs):
        raise AssertionError("Should explicitly mock '%s'" % kwargs['name'])
        
    # -------------------------
    ## @name Shotgun Connection Interface
    # @{

    base_url = 'http://nointernet.intern'

    def find_one(self, entity_type, filters, fields=None, *args, **kws):
        """Version of find_one which only returns values when filtered by id
        @return all entity information, not filtered by field"""
        for (type, id), entity in self._db.items():
            if type != entity_type:
                continue
            # now check filters
            
            # convert complex into new style
            if isinstance(filters, dict):
                new_filters = list()
                if filters['logical_operator'] != 'and':
                    raise Exception('unsupported sg mock find_one filter %s' % filters)
                for c in filters['conditions']:
                    if c['relation'] != 'is':
                        raise Exception('Unsupported sg mock find one filter %s' % filters)
                    field = c['path']
                    value = c['values'][0]
                    if len(c['values']) > 1:
                        raise Exception('Unsupported sg mock find one filter %s' % filters)
                    new_filters.append( [field, 'is', value])
                # end for each condition
                filters = new_filters
            # end convert dicts to lists
            

            found = True
            for f in filters:
                # assume operator is equals: e.g
                # filter = ["field", "is", "value"]
                field = f[0]
                if f[1] != "is":
                    raise Exception('Unsupported sg mock find one filter %s' % filters)
                value = f[2]

                # now search through entity to see if we got it
                if field in entity and entity[field] == value:
                    # it is a match! Keep going...
                    pass
                else:
                    # no match!
                    found = False
                    break
                # end 
            # end for each filter
                
            # did we find it?
            if found:
                return deepcopy(entity)
            # end bail out early
        # end for each piece of entity information
        
        # no match
        return None            

    def find(self, entity_type, filters, *args, **kws):
        """@return all matching entries for specified type. 
        Filters are only partially implemented, which could return too much"""
        results = [self._db[key] for key in self._db if key[0] == entity_type]
        
        # support [['id', 'in', 23, 34]]
        if isinstance(filters, list) and len(filters) == 1:
            # we have a [[something]] structure
            inner_filter = filters[0]
            if isinstance(inner_filter, list) and \
               len(inner_filter) > 2 and \
               inner_filter[0] == 'id' and \
               inner_filter[1] == 'in':
                all_items_of_type = [self._db[key] for key in self._db if key[0] == entity_type]
                ids_to_find = inner_filter[2:]
                matches = []
                for i in all_items_of_type:
                    if i['id'] in ids_to_find:
                        matches.append(i)
                
                # assign to final matches structure
                results = matches
        # end handle filter type
            
        # support dict style with 'is' relation
        if isinstance(filters, dict):
            for sg_filter in filters.get('conditions', list()):
                if sg_filter['relation'] != 'is':
                    continue
                    
                if sg_filter['values'] == [None] and sg_filter['path'] == 'id':
                    # always return empty for this
                    results = list()
                
                if isinstance(sg_filter['values'][0], (int, str)):
                    # only handling simple string and number relations
                    field_name = sg_filter['path']
                    # filter only if value exists in mocked data (if field not there don't skip)
                    results = [result for result in results 
                                if result.get(field_name, sg_filter['values'][0]) in sg_filter['values']]
                # end
            # end for each filter condition
        # end handle dict filter

        return deepcopy(results)

    def schema_field_read(self, entity_type, field_name):
        """@return the schema info dictionary for the given field of the entity type"""
        schema = self._schema[entity_type]
        if field_name is None:
            return deepcopy(schema)
        # end handle schema
        return {field_name : deepcopy(schema[field_name])}

    def schema_read(self):
        """@return our schema so far"""
        return deepcopy(self._schema)

    def upload_thumbnail(*args, **kwargs):
        """noop"""
        # do nothing

    @property
    def server_info(self):
        """@return server version - see set_server_info()"""
        return self._server_info
    
    ## -- End Shotgun Connection Interface -- @}

    # -------------------------
    ## @name Interface
    # Interface to help building a database
    # @{

    def db(self):
        """@return {('Type', id) : dict(field_data)} a data structure similar to the given one"""
        return self._db

    def set_entity_schema(self, entity_type, schema_data):
        """Set the entire schema of a given entity type.
        @param entity_type 
        @param schema_data matches what would be retrieved by entity_schema(entity_type, None), 
        or a subset of it
        @return self"""
        assert isinstance(schema_data, dict)
        self._schema[entity_type] = schema_data
        return self

    def set_entities(self, entities):
        """Fill db with given entities
        @param entities a single dict with entitiy fields and values, or a list of such dicts.
        Each needs type and id set
        @return self"""
        if not isinstance(entities, (list, tuple)):
            entities = [entities]
        # end

        for e in entities:
            self._db[(e['type'], e['id'])] = e
        # end for each entity to set

        return self

    def set_server_info(self, version_tuple):
        """Set the server info to the given major,minor,patch tuple
        @return self"""
        self._server_info['version'] =  version_tuple
        return self
        
    def clear(self):
        """Clear all data in the database
        @return self"""
        self._db = dict()
        self._schema = dict()
        self._server_info = dict(version=(4,3,9))
        return self
        
    ## -- End Interface -- @}

