#-*-coding:utf-8-*-
"""
@package bshotgun.tests.plugins.be_shotgun_tests

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

from urlparse import urlparse
from hashlib import md5

import bapp
from butility import Version
from be import BeSubCommand
from bcmd import (InputError,
                  CommandlineOverridesMixin)

from bshotgun import (ProxyShotgunConnection,
                      SQLProxyShotgunConnection,
                      combined_shotgun_schema)
from bshotgun.orm import ShotgunTypeFactory
from bshotgun.tests import (ShotgunTestDatabase,
                            TestShotgunTypeFactory)


class ShotgunTestsBeSubCommand(CommandlineOverridesMixin, BeSubCommand, bapp.plugin_type()):
    __slots__ = ()

    name = 'shotgun-tests'
    version = Version('0.1.0')
    description = "Builds samples for use in unit-tests from real databases"

    # -------------------------
    ## @name Constants
    # @{

    FORMAT_JSONZ = 'jsonz'
    
    ## -- End Constants -- @}

    # -------------------------
    ## @name Utilities
    # @{

    def validate_args(self, args):
        """Inserts 'fetcher' function and 'type_names' instance into the args namespace
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
        dsname = getattr(args, 'db-name')
        sgtdb = ShotgunTestDatabase(sample_name=dsname)
        if sgtdb.exists():
            raise InputError("Dataset named '%s' did already exist - please choose a different one" % dsname)
        # end

        # SETUP SCRAMBLER
        if args.scrambling_disabled:
            scrambler = lambda r: r            
        else:
            def scrambler(records):
                for rec in records:
                    for k,v in rec.iteritems():
                        if isinstance(v, basestring):
                            rec[k] = unicode(md5(v.encode('utf-16')).hexdigest()) + u'😄'
                        # end hash value
                    # end for each value
                # end for each record
                return records
            # end scrambles records in place
        # end scrambler

        # FIGURE OUT SOURCE AND SETUP TYPES
        if args.source is None:
            # SHOTGUN
            #########
            conn = ProxyShotgunConnection()
            fac = ShotgunTypeFactory()

            args.fetcher = lambda tn: scrambler(conn.find(tn, list(), fac.schema_by_name(tn).keys()))
            args.type_names = fac.type_names()
        else:
            url = urlparse(args.source)
            if url.scheme:
                # SQLALCHEMY
                ############
                db = SQLProxyShotgunConnection(db_url=args.source)

                args.fetcher = lambda tn: scrambler(db.find(tn, [], db.fields_by_typename(tn)))
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
        """ % self.FORMAT_JSONZ

        subparser = factory.add_parser('build-dataset', description=description, help=help)

        default = 'shotgun'
        help = "The source of the data for the sample db, default is '%s'.\
        %s: pull from shotgun directly (requires internet and api_token;\
        If it looks like an sqlalchemy URL to a previously created SQL database, this one will be used;\
        If it is just a name, it's interpreted as sample of an existing jsonz dataset;" % (default, default)
        subparser.add_argument('-s', '--source',
                               metavar='NAME-OR-SQLURL',
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
        
        return self

    def execute(self, args, remaining_args):
        self.apply_overrides(combined_shotgun_schema, args)
        args = self.validate_args(args)
        ShotgunTestDatabase(sample_name=getattr(args, 'db-name')).rebuild_database(args.type_names, args.fetcher)
        return self.SUCCESS

