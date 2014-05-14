#-*-coding:utf-8-*-
"""
@package bshotgun.tests.base
@brief Some basic testing utilities

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://github.com/Byron/bshotgun/blob/master/LICENSE.md)
"""
__all__ = ['ShotgunTestDatabase', 'ReadOnlyTestSQLProxyShotgunConnection', 'ShotgunTestCase', 'TestShotgunTypeFactory']

import json
import marshal
import time
import sys
import zlib


from butility.tests import TestCaseBase
from butility import Path
from bshotgun import (ProxyShotgunConnection,
                      SQLProxyShotgunConnection,
                      ProxyMeta)
from bshotgun.orm import ShotgunTypeFactory


DEFAULT_DB_SAMPLE='scrambled-ds1'


def dataset_tree(sample_name):
    """@return Path containing all data of a particular sample"""
    return ShotgunTestCase.sample_root(sample_name)


class ShotgunTestCase(TestCaseBase):
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
    
    def __init__(self, use_records_cache = True, sample_name=DEFAULT_DB_SAMPLE):
        """Initialize this instance
        @param use_records_cache if True, when 'records' are queried the first time, a fast cache file
        of the records will be created. It loads 60 times faster than json
        @param sample_name name of the sample in our fixtures database"""
        self._use_records_cache = use_records_cache
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

