#-*-coding:utf-8-*-
"""
@package bshotgun.tests.plugins.be_shotgun_tests

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import sys
from hashlib import md5
from datetime import (date,
                      datetime)

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
from bshotgun.orm.types import ShotgunDate
from bshotgun.tests import (ShotgunTestDatabase,
                            TestShotgunTypeFactory)

from bshotgun.plugins.utility import (is_sqlalchemy_url,
                                      TypeStreamer)


# -------------------------
## @name Utilities
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


def scramble_nested_strings(records, whitelist, transformer=lambda s: s):
    """Scrambles all strings in any nested structure, unless the value in question is in the 
    whitelist. transformer will be called to generate a value which is checked for whitelist membership.
    As special case, we don't scramble values whose key is 'type'. We also don't scramble dates"""
    whitelist = set(whitelist)

    def scramble_string(v):
        """scramble a string"""
        if transformer(v) in whitelist:
            return v
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        # don't scramble dates
        try:
            d = ShotgunDate(v)
            if isinstance(d, (date, datetime)):
                return v
            # if it's a number, don't do it. Yes, sg stores numbers as strings (sometimes)
            d = int(v)
            # we don't bother fixing this one ... have to deal with it anyway when this comes from sg
            return v
        except ValueError:
            # it's not a date.
            pass
        # end
        return unicode(md5(v).hexdigest()) + u'😄'

    def scramble_value(v):
        """scramble a value of any type recursively"""
        if isinstance(v, basestring):
            return scramble_string(v)
        elif isinstance(v, dict):
            return scramble_dict(v)
        elif isinstance(v, (tuple, list)):
            for vid, item in enumerate(v):
                v[vid] = scramble_value(item)
            return v
        return v
        # end hash value

    def scramble_dict(rec):
        """scrambles a dict, recursively traversing the tree"""
        for k,v in rec.iteritems():
            if k.lower().endswith('type'):
                continue
            rec[k] = scramble_value(v)
        # end for each value
        return rec
    # end scramble

    return scramble_value(records)
# end scrambles records in place


## -- End Utilities -- @}


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
            scrambler = lambda r, tn: r
        else:
            scrambler = scramble_nested_strings
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

            args.fetcher = lambda tn: scrambler(conn.find(tn, list(), fac.schema_by_name(tn).keys()), fac.type_names())
            args.type_names = fac.type_names()
        else:
            if is_sqlalchemy_url(args.source):
                # SQLALCHEMY
                ############
                db = SQLProxyShotgunConnection(db_url=args.source)

                # type-names are lower case for sql tables, so we have to transform the value for comparison
                args.fetcher = lambda tn: scrambler(db.find(tn, [], ['id']), db.type_names(), transformer = lambda v: v.lower())
                args.type_names = db.type_names()
            else:
                # JSONZ
                #######
                fac = TestShotgunTypeFactory(sample_name=args.source)
                db = ShotgunTestDatabase(sample_name=args.source)

                args.fetcher = lambda tn: scrambler(db.records(tn), fac.type_names())
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

        help = "If set, show will only show type names"
        subparser.add_argument('--list-types-only',
                               dest='list_types_only',
                               action='store_true',
                               help=help)

        help = "The types to show. If unset, all types will be shown"
        subparser.add_argument('-t', '--types',
                                dest='only_these_types',
                                nargs='+',
                                help=help)
        
        return self

    def execute(self, args, remaining_args):
        self.apply_overrides(combined_shotgun_schema, args)
        args = self.validate_args(args)
        if args.operation == self.OP_BUILD:
            ShotgunTestDatabase(sample_name=getattr(args, 'db-name')).rebuild_database(args.type_names, args.fetcher)
        elif args.operation == self.OP_SHOW:
            if args.list_types_only:
                for tn in args.type_names:
                    sys.stdout.write(tn + '\n')
                # end print types
                return self.SUCCESS
            # end list only
            type_names = args.type_names
            if args.only_these_types:
                type_names = sorted(set(args.type_names) & set(args.only_these_types))
            # end filter type_names
            if not type_names:
                raise InputError("Didn't find any type-names for display - is the schema set or available ?")
            TypeStreamer(args.fetcher, type_names).stream(sys.stdout.write)
        else:
            raise NotImplemented(self.operation)
        return self.SUCCESS

