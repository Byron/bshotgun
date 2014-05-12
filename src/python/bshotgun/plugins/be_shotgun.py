#-*-coding:utf-8-*-
"""
@package bshotgun.plugins.be_shotgun
@brief A be command to interact with shotgun

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['ShotgunBeSubCommand']

import sys

import bapp
from butility import (Version,
                      Path)
from be import BeSubCommand
from bshotgun import ProxyShotgunConnection
from bshotgun.orm import ShotgunTypeFactory
from bcmd import CommandlineOverridesMixin

from bshotgun import combined_shotgun_schema

# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

class CommandShotgunTypeFactory(ShotgunTypeFactory):
    """Writes everything into a particular tree"""
    __slots__ = ('_tree',
                 '_type_whitelist')

    def __init__(self, *args, **kwargs):
        self._tree = kwargs.pop('write_to', None)
        self._type_whitelist = set(kwargs.pop('allowed_types', tuple()))
        super(CommandShotgunTypeFactory, self).__init__(*args, **kwargs)

    def _schema_path(self, type_name):
        if not self._tree:
            return super(CommandShotgunTypeFactory, self)._schema_path(type_name)
        return self._tree / ('%s%s' % (type_name, self.SCHEMA_FILE_EXTENSION))

    def type_names(self):
        types = super(CommandShotgunTypeFactory, self).type_names()
        if self._type_whitelist:
            types = set(types) & self._type_whitelist
        return types
    
# end class CommandShotgunTypeFactory

## -- End Utility Types -- @}



class ShotgunBeSubCommand(CommandlineOverridesMixin, BeSubCommand, bapp.plugin_type()):
    """Allows to interact with any configured shotgun database"""
    __slots__ = ()

    name = 'shotgun'
    version = Version('0.1.0')
    description = "data wrangling with shotgun"


    # -------------------------
    ## @name Configuration
    # @{

    OP_SCHEMA_CACHE = 'update-schema-cache'
    OP_SQL_CACHE = 'initialize-sql-cache'
    
    ## -- End Configuration -- @}


    def setup_argparser(self, parser):
        """Setup your flags using argparse"""
        super(ShotgunBeSubCommand, self).setup_argparser(parser)
        self.setup_overrides_argparser(parser)
        factory = parser.add_subparsers(title="operations",
                                        description="Choose between various operations",
                                        dest='operation',
                                        help="Choose between various operations")

        #####################################
        # SUBCOMMAND: update-schema-cache ##
        ####################################
        description = "update schema cache from shotgun"
        help = """The ORM uses a cache to know about the data-base schema.
It needs to be (re)created and cached locally to prevent lengthy startup times.
This should be repeated each time the schema changes."""
        subparser = factory.add_parser(self.OP_SCHEMA_CACHE, description=description, help=help)

        help = "A writable location into which to drop the schema cache. Existing files will be overwritten \
without warning."
        subparser.add_argument('tree',
                               type=Path, 
                               help=help)
        
        ######################################
        # SUBCOMMAND: initialize-sql-cache ##
        ####################################
        description = "creates a new SQL cache from an existing shotgun database"
        help = """Pull all data from a shotgun database and write it into an SQL database to 
accelerate read access. Please read the online docs for more information."""
        subparser = factory.add_parser(self.OP_SQL_CACHE, description=description, help=help)

        help = "An sqlalchemy compatible URL to put the data into, which didn't exist.\
e.g. sqlite:///relative-path.sqlite or mysql://host/db"
        subparser.add_argument('sqlalchemy-url',
                               type=str, 
                               help=help)

        help = "A single entity type to include in the cache. If unset, all will be pulled.\
Mainly used for debugging"
        subparser.add_argument('--type',
                               nargs=1,
                               type=str,
                               dest='allowed_types',
                               help=help)
        return self

    def execute(self, args, remaining_args):
        try:
            self.apply_overrides(combined_shotgun_schema, args)
            if args.operation == self.OP_SCHEMA_CACHE:
                conn = ProxyShotgunConnection()
                CommandShotgunTypeFactory(write_to=args.tree).update_schema(conn)
            elif args.operation == self.OP_SQL_CACHE:
                from bshotgun.sql import SQLProxyShotgunConnection
                conn = ProxyShotgunConnection()
                tf = CommandShotgunTypeFactory(allowed_types=args.allowed_types)
                fetcher = lambda tn: conn.find(tn, list(), tf.schema_by_name(tn).keys())
                SQLProxyShotgunConnection.init_database(getattr(args, 'sqlalchemy-url'), tf, fetcher)
            else:
                raise AssertionError("Didn't implement subcommand")
            return self.SUCCESS
        except ValueError as err:
            sys.stdout.write(str(err) + '\n')
            sys.stdout.write("Be sure to set the kvstore values listed with -c\n")
            raise
            return self.ERROR
        # end convert exceptions
