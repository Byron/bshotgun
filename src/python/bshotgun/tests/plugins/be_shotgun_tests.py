#-*-coding:utf-8-*-
"""
@package bshotgun.tests.plugins.be_shotgun_tests

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import sys
from urlparse import urlparse
from hashlib import md5

import bapp
from butility import (Version,
                      DictObject)
from be import BeSubCommand
from bcmd import (InputError,
                  CommandlineOverridesMixin)

from bshotgun import (ProxyShotgunConnection,
                      SQLProxyShotgunConnection,
                      combined_shotgun_schema)
from bshotgun.orm import ShotgunTypeFactory
from bshotgun.tests import (ShotgunTestDatabase,
                            TestShotgunTypeFactory)


# -------------------------
## @name Utility Types
# @{

class WriterShotgunTypeFactory(ShotgunTypeFactory):
    """Writes schema to a specific path"""
    __slots__ = ('_tree')

    def __init__(self, tree):
        ShotgunTypeFactory.__init__(self)
        self._tree = tree

    def settings_value(self):
        if not self._tree.isdir():
            self._tree.makedirs()
        # end dirs on demand
        return DictObject({'schema_cache_tree' : self._tree})    

# end class WriterShotgunTypeFactory


## -- End Utility Types -- @}


class ShotgunTestsBeSubCommand(CommandlineOverridesMixin, BeSubCommand, bapp.plugin_type()):
    __slots__ = ()

    name = 'shotgun-tests'
    version = Version('0.1.0')
    description = "Builds samples for use in unit-tests from real databases"

    # -------------------------
    ## @name Constants
    # @{

    FORMAT_JSONZ = 'jsonz'

    OP_BUILD = 'build-dataset'
    OP_SHOW = 'show'
    
    ## -- End Constants -- @}

    # -------------------------
    ## @name Utilities
    # @{

    def validate_args(self, args):
        """Inserts 'fetcher' function and 'type_names' list into the args namespace
        @throws InputError if something is wrong with our argument logic
{

    // Word wrapping - follow PEP 8 recommendations
    "rulers": [ 112, 120 ],
    "word_wrap": true,
    "wrap_width": 120,

    // Whitespace - no tabs, trimming, end files with \n
    "tab_size": 4,
    "translate_tabs_to_spaces": true,
    "ensure_newline_at_eof_on_save": true,
    
}        @return possibly changed args"""
        dsname = 'UNSET'
        if args.operation != self.OP_BUILD:
            args.scrambling_disabled = True
        else:
            dsname = getattr(args, 'db-name')
            sgtdb = ShotgunTestDatabase(sample_name=dsname)
            if sgtdb.exists():
                raise InputError("Dataset named '%s' did already exist - please choose a different one" % dsname)
            # end
        # end never scramble outside of build mode

        # SETUP SCRAMBLER
        if args.scrambling_disabled:
            scrambler = lambda r: r            
        else:
            def scrambler(records):
                for rec in records:
                    for k,v in rec.iteritems():
                        if isinstance(v, basestring):
                            if isinstance(v, unicode):
                                v = v.encode('utf-8')
                            rec[k] = unicode(md5(v).hexdigest()) + u'😄'
                        # end hash value
                    # end for each value
                # end for each record
                return records
            # end scrambles records in place
        # end scrambler

        # FIGURE OUT SOURCE AND SETUP TYPES
        # get rid of list
        args.source = args.source and args.source[0] or list()
        if not args.source:
            # SHOTGUN
            #########
            conn = ProxyShotgunConnection()
            fac = WriterShotgunTypeFactory(TestShotgunTypeFactory.schema_tree(dsname))

            # update schema from DB
            if args.operation == self.OP_BUILD:
                fac.update_schema(conn)
            # end don't wrote in query mode

            args.fetcher = lambda tn: scrambler(conn.find(tn, list(), fac.schema_by_name(tn).keys()))
            args.type_names = fac.type_names()
        else:
            url = urlparse(args.source)
            if url.scheme:
                # SQLALCHEMY
                ############
                db = SQLProxyShotgunConnection(db_url=args.source)

                args.fetcher = lambda tn: scrambler(db.find(tn, [], ['id']))
                args.type_names = db.type_names()
            else:
                # JSONZ
                #######
                fac = TestShotgunTypeFactory(sample_name=args.source)
                db = ShotgunTestDatabase(sample_name=args.source)

                args.fetcher = lambda tn: scrambler(db.records(tn))
                args.type_names = fac.type_names()
            # end handle value
        # end handle source

        return args
    
    ## -- End Utilities -- @}


    def setup_argparser(self, parser):
        """Setup your flags using argparse"""
        super(ShotgunTestsBeSubCommand, self).setup_argparser(parser)
        self.setup_overrides_argparser(parser)
        factory = parser.add_subparsers(title="operations",
                                        description="Choose between various operations",
                                        dest='operation', # args.operation == chosen subcommand
                                        help="Choose between various operations")
        
        ##################################
        # SUBCOMMAND: 'build-dataset ##
        ################################
        description = "Create a dataset for use in unit-tests"
        help = """Unittests must have access to real-world data, for quick loading and sandboxing.
        This operation reads data from various sources, and outputs it to a format that is meant to be storage efficient.
        From there, respective test-cases will build their own high-speed dataset the first time they hit it.

        Provided a source of the data, you can output it to the '%s' format used by tests.
        By default, data will be scrambled, which will effectively hash all strings.

        IMPORTANT: We don't write the schema - you will have to do this youself using the 'update-schema-cache' 
        subcommand of the 'shotgun' be command, unless you retrieve everything from the database
        """ % self.FORMAT_JSONZ

        subparser = factory.add_parser(self.OP_BUILD, description=description, help=help)

        default = 'shotgun'
        help = "The source of the data for the sample db, default is '%s'.\
        %s: pull from shotgun directly (requires internet and api_token;\
        If it looks like an sqlalchemy URL to a previously created SQL database, this one will be used;\
        If it is just a name, it's interpreted as sample of an existing jsonz dataset;" % (default, default)
        subparser.add_argument('-s', '--source',
                               metavar='NAME-OR-SQLURL',
                               nargs=1, # normalize with source in 'show'
                               dest='source',
                               help=help)

        help = "If set, the output will NOT be scrambled. This is not the default for privacy reasons"
        subparser.add_argument('--no-scrambling',
                               action='store_true',
                               default=False,
                               dest='scrambling_disabled',
                               help=help)

        help = "The name of the sample to create, like 'db2' - it really doesn't matter. \
        Destination sample must not exist."
        subparser.add_argument('db-name',
                                help=help)

        ######################
        # SUBCOMMAND: show ##
        ####################
        description = "Similar to the shotgun command's version, but the unit-test datasets"
        help = description
        subparser = factory.add_parser(self.OP_SHOW, description=description, help=help)

        help = "Either an sqlalchemy URL to show sql contents or a name of a dataset for testing"
        subparser.add_argument('source',
                                metavar='NAME-OR-SQLURL',
                                nargs=1,
                                help=help)
        
        return self

    def execute(self, args, remaining_args):
        self.apply_overrides(combined_shotgun_schema, args)
        args = self.validate_args(args)
        if args.operation == self.OP_BUILD:
            ShotgunTestDatabase(sample_name=getattr(args, 'db-name')).rebuild_database(args.type_names, args.fetcher)
        elif args.operation == self.OP_SHOW:
            from bshotgun.plugins.be_shotgun import TypeStreamer
            TypeStreamer(args.fetcher, args.type_names).stream(sys.stdout.write)
        else:
            raise NotImplemented(self.operation)
        return self.SUCCESS

