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
                      Path,
                      datetime_to_date_time_string)
from be import BeSubCommand
from bshotgun import (ProxyShotgunConnection,
                      SQLProxyShotgunConnection)
from bshotgun.orm import ShotgunTypeFactory
from bcmd import CommandlineOverridesMixin

from bshotgun import combined_shotgun_schema

from .utility import (is_sqlalchemy_url,
                      TypeStreamer)


# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

class CommandShotgunTypeFactory(ShotgunTypeFactory):
    """Writes everything into a particular tree"""
    __slots__ = ('_tree',
                 '_type_blacklist')

    def __init__(self, *args, **kwargs):
        self._tree = kwargs.pop('write_to', None)
        self._type_blacklist = set(kwargs.pop('ignored_types', tuple()))
        super(CommandShotgunTypeFactory, self).__init__(*args, **kwargs)

    def _schema_path(self, type_name):
        if not self._tree:
            return super(CommandShotgunTypeFactory, self)._schema_path(type_name)
        return self._tree / ('%s%s' % (type_name, self.SCHEMA_FILE_EXTENSION))

    def type_names(self):
        types = super(CommandShotgunTypeFactory, self).type_names()
        if self._type_blacklist:
            types = set(types) - self._type_blacklist
        return types

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
    OP_SHOW = 'show'
    
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

        help = "A single entity type to ignore in the cache. If unset, all will be pulled.\
Mainly used for debugging, and entities with invalid things in them."
        subparser.add_argument('--ignore-type',
                               nargs=1,
                               default=list(),
                               dest='ignored_type',
                               help=help)

        ######################
        # SUBCOMMAND: show ##
        ####################
        description = "Write the entire dataset to stdout"
        help = """Provided with a data source, it will display all data as json."""
        subparser = factory.add_parser(self.OP_SHOW, description=description, help=help)

        help = "Either an sqlalchemy URL to show sql contents, or anything else to show what's in shotgun"
        subparser.add_argument('location',
                                metavar='sqlalchemy_url',
                                nargs='?',
                                help=help)

        return self

    def execute(self, args, remaining_args):
        try:
            self.apply_overrides(combined_shotgun_schema, args)
            if args.operation == self.OP_SCHEMA_CACHE:
                conn = ProxyShotgunConnection()
                CommandShotgunTypeFactory(write_to=args.tree).update_schema(conn)
            elif args.operation == self.OP_SQL_CACHE:
                conn = ProxyShotgunConnection()
                tf = CommandShotgunTypeFactory(ignored_types=args.ignored_type)
                fetcher = lambda tn: conn.find(tn, list(), tf.schema_by_name(tn).keys())
                SQLProxyShotgunConnection.init_database(getattr(args, 'sqlalchemy-url'), tf, fetcher)
            elif args.operation == self.OP_SHOW:
                if args.location and is_sqlalchemy_url(args.location):
                    # SQL
                    db = SQLProxyShotgunConnection(db_url=args.location)
                    fetcher = lambda tn: db.find(tn, [], ['id'])
                    type_names = db.type_names()
                else:
                    # SHOTGUN
                    conn = ProxyShotgunConnection()
                    fac = ShotgunTypeFactory()
                    fetcher = lambda tn: conn.find(tn, list(), fac.schema_by_name(tn).keys())
                    type_names = fac.type_names()
                # end handle supported types
                TypeStreamer(fetcher, type_names).stream(sys.stdout.write)
            else:
                raise NotImplemented(self.operation)
            return self.SUCCESS
        except ValueError as err:
            sys.stdout.write(str(err) + '\n')
            sys.stdout.write("Be sure to set the kvstore values listed with -c\n")
            return self.ERROR
        # end convert exceptions
