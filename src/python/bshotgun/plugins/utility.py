#-*-coding:utf-8-*-
"""
@package bshotgun.plugins.utility
@brief a bunch of utilities for bshotgun plugins

@author Sebastian Thiel
@copyright [GNU Lesser General Public License](https://www.gnu.org/licenses/lgpl.html)
"""
__all__ = ['is_sqlalchemy_url', 'TypeStreamer']

import json
from datetime import datetime
from urlparse import urlparse


# ==============================================================================
## @name Utility Types
# ------------------------------------------------------------------------------
## @{

def is_sqlalchemy_url(url):
    """@return True if this is an sqlalchemy URL
    @note for now, any URL will do"""
    return bool(urlparse(url).scheme)


class TypeStreamer(object):
    """Streams shotgun data using a simple fetcher interface, with fixing for sg datetime objects"""
    __slots__ = ('_fetcher', '_type_names')

    def __init__(self, fetcher, type_names):
        """@param fetcher f(type_name) => iter([dict_like, ...])
        @param type_names list of names for the fetcher"""
        self._fetcher = fetcher
        self._type_names = type_names

    # -------------------------
    ## @name Interface
    # @{

    def stream(self, writer):
        """Call writer with the serialized object information"""
        for tn in self._type_names:
            for rec in self._fetcher(tn):
                writer(json.dumps(rec, check_circular=False, ensure_ascii=True,
                                       allow_nan=True, indent=2, default=str))
            # end for record
        # end for each tn
    ## -- End Interface -- @}
    

# end class TypeStreamer
    
# end class CommandShotgunTypeFactory

## -- End Utility Types -- @}

