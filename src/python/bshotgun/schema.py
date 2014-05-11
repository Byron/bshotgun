#-*-coding:utf-8-*-
"""
@package bshotgun.schema
@brief defines schemas we use when querying configuration

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['sql_shotgun_schema', 'shotgun_schema', 'type_factory_schema', 'combined_shotgun_schema']

from butility import Path
from bkvstore import (KeyValueStoreSchema,
                      KeyValueStoreSchemaValidator)


shotgun_schema = KeyValueStoreSchema('shotgun', {'host' : str,
                                                 'api_script' : str,
                                                 'api_token' : str,
                                                 'http_proxy' : str})

type_factory_schema = KeyValueStoreSchema(shotgun_schema.key(), {'schema_cache_tree' : Path})

sql_shotgun_schema = KeyValueStoreSchemaValidator.merge_schemas(
                        (shotgun_schema,
                            KeyValueStoreSchema(shotgun_schema.key(), {'sql_cache_url' : str })))


# this one should contain all the keys
combined_shotgun_schema = KeyValueStoreSchemaValidator.merge_schemas((sql_shotgun_schema, type_factory_schema))
