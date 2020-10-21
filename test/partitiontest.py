## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015, 2018, 2020 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase

from meresco.oaicommon import Partition

class PartitionTest(SeecrTestCase):
    def testDisallowed(self):
        self.assertRaises(ValueError, lambda: Partition.create('1,4/3'))
        self.assertRaises(ValueError, lambda: Partition.create('1/30'))

    def testHash(self):
        self.assertEqual(485, Partition.hashId("identifier"))
        self.assertEqual(1024, Partition.NR_OF_PARTS)

    def testRanges(self):
        self.assertEqual([(0,512)], list(Partition.create('1/2').ranges()))
        self.assertEqual([(512,1024)], list(Partition.create('2/2').ranges()))
        self.assertEqual([(0,205)], list(Partition.create('1/5').ranges()))
        self.assertEqual([(820,1025)], list(Partition.create('5/5').ranges()))
        self.assertEqual([(927,1030)], list(Partition.create('10/10').ranges()))
        self.assertEqual([(0,205), (820,1025)], list(Partition.create('1,5/5').ranges()))
        self.assertEqual([(0, 410), (820,1025)], list(Partition.create('1,2,5/5').ranges()))

    def testStr(self):
        self.assertEqual("1/2", "%s" % Partition.create('1/2'))
        self.assertEqual("2/2", str(Partition.create('2/2')))
        self.assertEqual("2/10", str(Partition.create('2/10')))
        self.assertEqual("1/10", str(Partition.create('1/10')))
        self.assertEqual("1,3,4,5/7", str(Partition.create('1,3,4,5/7')))

    def testEquals(self):
        self.assertEqual(Partition.create('1/2'), Partition.create('1/2'))
        self.assertEqual(hash(Partition.create('1/2')), hash(Partition([1],2)))

    def testFromStringNone(self):
        self.assertEqual(None, Partition.create(None))
        self.assertEqual(None, Partition.create(''))
        self.assertEqual(Partition.create('1/2'), Partition.create(Partition.create('1/2')))
