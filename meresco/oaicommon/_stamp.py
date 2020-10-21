## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2018, 2020 Seecr (Seek You Too B.V.) http://seecr.nl
#
# This file is part of "Meresco Oai Common"
#
# "Meresco Oai Common" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco Oai Common" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco Oai Common"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from time import time, strftime, gmtime, strptime
from calendar import timegm
from sys import maxsize

def stamp2zulutime(stamp, preciseDatestamp=False):
    if stamp is None:
        return ''
    stamp = int(stamp)
    microseconds = ".%06d" % (stamp % DATESTAMP_FACTOR) if preciseDatestamp else ""
    return "%s%sZ" % (strftime('%Y-%m-%dT%H:%M:%S', gmtime(stamp / DATESTAMP_FACTOR)), microseconds)

def timestamp():
    return int(time() * DATESTAMP_FACTOR)

def timeToNumber(time):
    try:
        return int(timegm(strptime(time, '%Y-%m-%dT%H:%M:%SZ')) * DATESTAMP_FACTOR)
    except (ValueError, OverflowError):
        return maxsize * DATESTAMP_FACTOR

DATESTAMP_FACTOR = 1000000

__all__ = ['stamp2zulutime', 'timestamp', 'timeToNumber']
