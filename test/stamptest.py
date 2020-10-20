## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2018 Seecr (Seek You Too B.V.) http://seecr.nl
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

from unittest import TestCase
from meresco.oaicommon import stamp2zulutime, timeToNumber

class StampTest(TestCase):

    def testStamp2Zulutime(self):
        self.assertEqual("2012-10-04T09:21:04Z", stamp2zulutime("1349342464030008"))
        self.assertEqual("", stamp2zulutime(None))
        self.assertRaises(Exception, stamp2zulutime, "not-a-stamp")
        self.assertEqual("2012-10-04T09:21:04.030008Z", stamp2zulutime("1349342464030008", preciseDatestamp=True))

    def testTimeToNumber(self):
        self.assertEqual(1349342464000000, timeToNumber("2012-10-04T09:21:04Z"))
        self.assertEqual(9223372036854775807000000, timeToNumber("deze"))

