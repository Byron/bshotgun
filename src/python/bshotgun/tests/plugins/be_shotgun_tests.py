#-*-coding:utf-8-*-
"""
@package bshotgun.tests.plugins.be_shotgun_tests

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = []

import bapp
from butility import Version
from be import BeSubCommand


class ShotgunTestsBeSubCommand(BeSubCommand, bapp.plugin_type()):
    __slots__ = ()

    name = 'shotgun-tests'
    version = Version('0.1.0')
    description = "Builds samples for use in unit-tests from real databases"

    def setup_argparser(self, parser):
        """Setup your flags using argparse"""
        # parser.add_argument('-v', '--verbose',
        #                     action='store_true', 
        #                     default=False, 
        #                     dest='verbosity',
        #                     help='enable verbose mode')
        return self

    def execute(self, args, remaining_args):
        raise NotImplementedError('tbd')
        return self.SUCCESS

