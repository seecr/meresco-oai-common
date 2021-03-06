## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010-2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2011-2012, 2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from seecr.test import SeecrTestCase, CallTrace

from lxml.etree import parse
from io import StringIO

from meresco.oaicommon import UpdateAdapterFromOaiDownloadProcessor
from weightless.core import compose

def addMock(identifier, partname, lxmlNode):
    return
    yield

def deleteMock(identifier):
    return
    yield

class UpdateAdapterTest(SeecrTestCase):
    def testDelete(self):
        adapter = UpdateAdapterFromOaiDownloadProcessor()
        observer = CallTrace('observer', methods={'delete': deleteMock})
        adapter.addObserver(observer)

        list(compose(adapter.add(identifier='oai:test:identifier', lxmlNode=parse(StringIO(OAI_DELETED_RECORD)), datestamp="2010-10-19T09:57:32Z")))

        self.assertEqual(['delete'], [m.name for m in observer.calledMethods])
        self.assertEqual({'identifier':'oai:test:identifier'}, observer.calledMethods[0].kwargs)

    def testAdd(self):
        adapter = UpdateAdapterFromOaiDownloadProcessor()
        observer = CallTrace('observer', methods={'add': addMock})
        adapter.addObserver(observer)

        recordNode = parse(StringIO(OAI_RECORD))
        list(compose(adapter.add(identifier='oai:test:identifier', lxmlNode=recordNode, datestamp="2010-10-19T09:57:32Z")))

        self.assertEqual(['add'], [m.name for m in observer.calledMethods])
        kwargs = observer.calledMethods[0].kwargs
        self.assertEqual({'identifier': 'oai:test:identifier',
            'partname': 'record',
            'lxmlNode': recordNode}, kwargs)


OAI_DELETED_RECORD = """<oai:record xmlns:oai="http://www.openarchives.org/OAI/2.0/">
    <oai:header status="deleted">
            <oai:identifier>oai:test:identifier</oai:identifier>
            <oai:datestamp>2010-10-19T09:57:32Z</oai:datestamp>
    </oai:header>
</oai:record>"""

OAI_RECORD = """<oai:record xmlns:oai="http://www.openarchives.org/OAI/2.0/">
    <oai:header>
            <oai:identifier>oai:test:identifier</oai:identifier>
            <oai:datestamp>2010-10-19T09:57:32Z</oai:datestamp>
    </oai:header>
    <oai:metadata><somedata xmlns="http://example.org">DATA</somedata></oai:metadata>
</oai:record>"""

