## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011-2016, 2018 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012, 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from io import StringIO
from datetime import datetime
from lxml.etree import parse
from os.path import join, isfile
from simplejson import load
from urllib.parse import urlencode

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stdout_replaced
from weightless.core import compose, consume, be, local
from weightless.io import Suspend

from meresco.core import Observable
from meresco.components import lxmltostring, Schedule

from meresco.oaicommon import OaiDownloadProcessor


class OaiDownloadProcessorTest(SeecrTestCase):
    def testRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testRequestWithAdditionalUserAgent(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, userAgentAddition="From a certain server")
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x (From a certain server)\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testPartitionRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, partition="1/2")
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-partition=1%%2F2&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testUpdateRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.setPath('/otherOai')
        oaiDownloadProcessor.setMetadataPrefix('otherPrefix')
        oaiDownloadProcessor.setSet('aSet')
        oaiDownloadProcessor.setFrom('2014')
        self.assertEqual("""GET /otherOai?verb=ListRecords&from=2014&metadataPrefix=otherPrefix&set=aSet&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testUpdateRequestAfterSetResumptionToken(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="aSet", workingDirectory=self.tempdir, xWait=False)
        oaiDownloadProcessor.setPath('/otherOai')
        oaiDownloadProcessor.setFrom('2014')
        oaiDownloadProcessor.setResumptionToken('ReSumptionToken')
        self.assertEqual("""GET /otherOai?verb=ListRecords&resumptionToken=ReSumptionToken HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testScheduleNextRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path='/p', metadataPrefix='p', workingDirectory=self.tempdir)
        oaiDownloadProcessor._time = lambda: 17
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertTrue(oaiDownloadProcessor._earliestNextRequestTime > 17)

        oaiDownloadProcessor.scheduleNextRequest()
        self.assertEqual(0, oaiDownloadProcessor._earliestNextRequestTime)
        self.assertEqual(True, oaiDownloadProcessor._timeForNextRequest())
        self.assertNotEqual(None, oaiDownloadProcessor.buildRequest())

        oaiDownloadProcessor.scheduleNextRequest(Schedule(period=0))
        self.assertEqual(17, oaiDownloadProcessor._earliestNextRequestTime)
        self.assertEqual(True, oaiDownloadProcessor._timeForNextRequest())
        self.assertNotEqual(None, oaiDownloadProcessor.buildRequest())

        oaiDownloadProcessor.scheduleNextRequest(Schedule(period=120))
        self.assertEqual(137, oaiDownloadProcessor._earliestNextRequestTime)
        self.assertEqual(False, oaiDownloadProcessor._timeForNextRequest())
        self.assertEqual(None, oaiDownloadProcessor.buildRequest())

    def testSignalHarvestingDone(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path='/p', metadataPrefix='p', workingDirectory=self.tempdir, incrementalHarvestSchedule=None)
        oaiDownloadProcessor.addObserver(observer)

        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], observer.calledMethodNames())

    def testRequestWithAdditionalHeaders(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        request = oaiDownloadProcessor.buildRequest(additionalHeaders={'Host': 'example.org'})
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nHost: example.org\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, request)

    def testListIdentifiersRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, verb='ListIdentifiers')
        self.assertEqual("""GET /oai?verb=ListIdentifiers&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testSetInRequest(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="setName", workingDirectory=self.tempdir, xWait=True)
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&set=setName&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="set-_.!~*'()", workingDirectory=self.tempdir, xWait=True)
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&set=set-_.%%21%%7E%%2A%%27%%28%%29&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", set="setName", workingDirectory=self.tempdir, xWait=True)
        self.assertEqual("""GET /oai?verb=ListRecords&resumptionToken=u%%7Cc1286437597991025%%7Cmprefix%%7Cs%%7Cf&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())

    def testHandle(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False)
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], [m.name for m in observer.calledMethods])
        addMethod = observer.calledMethods[1]
        self.assertEqual(0, len(addMethod.args))
        self.assertEqualsWS(ONE_RECORD, lxmltostring(addMethod.kwargs['lxmlNode']))
        self.assertEqual('2011-08-22T07:34:00Z', addMethod.kwargs['datestamp'])
        self.assertEqual('oai:identifier:1', addMethod.kwargs['identifier'])

    def testOaiListRequestOnCallstack(self):
        listRequests = []
        def addMethod(**kwargs):
            listRequests.append(local('__callstack_var_oaiListRequest__'))
            return
            yield
        observer = CallTrace(methods={'add': addMethod})
        top = be((Observable(),
            (OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False),
                (observer,)
            )
        ))
        consume(top.all.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], [m.name for m in observer.calledMethods])
        self.assertEqual([{'set': None, 'metadataPrefix': 'oai_dc'}], listRequests)

        listRequests = []
        observer.calledMethods.reset()
        top = be((Observable(),
            (OaiDownloadProcessor(path="/oai", metadataPrefix="other", set='aSet', workingDirectory=self.tempdir, xWait=True),
                (observer,)
            )
        ))
        consume(top.all.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
        self.assertEqual([{'set': 'aSet', 'metadataPrefix': 'other'}], listRequests)

    def testListIdentifiersHandle(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, verb='ListIdentifiers')
        oaiDownloadProcessor.addObserver(observer)
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTIDENTIFIERS_RESPONSE)))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], [m.name for m in observer.calledMethods])
        addMethod = observer.calledMethods[1]
        self.assertEqual(0, len(addMethod.args))
        self.assertEqualsWS(ONE_HEADER, lxmltostring(addMethod.kwargs['lxmlNode']))
        self.assertEqual('2011-08-22T07:34:00Z', addMethod.kwargs['datestamp'])
        self.assertEqual('oai:identifier:1', addMethod.kwargs['identifier'])

    def testHandleWithTwoRecords(self):
        observer = CallTrace(methods={'add': lambda **kwargs: (x for x in [])})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        oaiDownloadProcessor.addObserver(observer)
        secondRecord = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:2</identifier><datestamp>2011-08-22T07:41:00Z</datestamp></header><metadata>ignored</metadata></record>'
        list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % (secondRecord + RESUMPTION_TOKEN))))))
        self.assertEqual(['startOaiBatch', 'add', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
        addMethod0, addMethod1 = observer.calledMethods[1:3]
        self.assertEqual(0, len(addMethod0.args))
        self.assertEqualsWS(ONE_RECORD, lxmltostring(addMethod0.kwargs['lxmlNode']))
        self.assertEqual('2011-08-22T07:34:00Z', addMethod0.kwargs['datestamp'])
        self.assertEqual('oai:identifier:1', addMethod0.kwargs['identifier'])
        self.assertEqualsWS(secondRecord, lxmltostring(addMethod1.kwargs['lxmlNode']))
        self.assertEqual('2011-08-22T07:41:00Z', addMethod1.kwargs['datestamp'])
        self.assertEqual('oai:identifier:2', addMethod1.kwargs['identifier'])

    def testRaiseErrorOnBadResponse(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        badRecord = '<record>No Header</record>'
        try:
            list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % badRecord)))))
            self.fail()
        except IndexError:
            pass

    def testListRecordsRequestError(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        self.assertEqual('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())
        consume(oaiDownloadProcessor.handle(parse(StringIO(ERROR_RESPONSE))))
        self.assertEqual(0, len(observer.calledMethods))
        self.assertEqual("someError: Some error occurred.\n", oaiDownloadProcessor._err.getvalue())
        self.assertEqual('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('metadataPrefix', 'oai_dc'), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())

    def testUseResumptionToken(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEqual('x?y&z', oaiDownloadProcessor._resumptionToken)
        self.assertEqual('GET /oai?verb=ListRecords&resumptionToken=x%%3Fy%%26z&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n' % oaiDownloadProcessor._identifier, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEqual('x?y&z', oaiDownloadProcessor._resumptionToken)

    def testReadResumptionTokenFromStateWithNewline(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEqual(resumptionToken, oaiDownloadProcessor._resumptionToken)

    def testReadResumptionTokenWhenNoState(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)

    def testReadInvalidState(self):
        open(join(self.tempdir, 'harvester.state'), 'w').write("invalid")
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)

    def testKeepResumptionTokenOnFailingAddCall(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        observer.exceptions={'add': Exception("Could be anything")}
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        self.assertEqual('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())
        self.assertRaises(Exception, lambda: list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))))
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch'], [m.name for m in observer.calledMethods])
        errorOutput = oaiDownloadProcessor._err.getvalue()
        self.assertTrue(errorOutput.startswith('Traceback'), errorOutput)
        self.assertTrue('Exception: Could be anything\nWhile processing:\n<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:1' in errorOutput, errorOutput)
        self.assertEqual('GET /oai?%s HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n' % (urlencode([('verb', 'ListRecords'), ('resumptionToken', resumptionToken), ('x-wait', 'True')]), oaiDownloadProcessor._identifier), oaiDownloadProcessor.buildRequest())

    def testHandleYieldsAtLeastOnceAfterEachRecord(self):
        def add(**kwargs):
            return
            yield
        observer = CallTrace(methods={'add': add})
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False)
        oaiDownloadProcessor.addObserver(observer)
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEqual(1, len(yields))

        secondRecord = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:2</identifier><datestamp>2011-08-22T07:41:00Z</datestamp></header><metadata>ignored</metadata></record>'
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % secondRecord)))))
        self.assertEqual(2, len(yields))

    def testYieldSuspendFromAdd(self):
        observer = CallTrace()
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False)
        oaiDownloadProcessor.addObserver(observer)
        suspend = Suspend()
        observer.returnValues['add'] = (x for x in [suspend])
        yields = list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % '')))))
        self.assertEqual([suspend, None], yields)

    def testHarvesterState(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        state = oaiDownloadProcessor.getState()
        self.assertEqual(None, state.resumptionToken)
        self.assertEqual(None, state.from_)
        self.assertEqual(None, state.errorState)
        self.assertEqual(None, state.name)
        self.assertEqual("/oai", state.path)
        self.assertEqual("oai_dc", state.metadataPrefix)
        self.assertEqual(None, state.set)
        self.assertEqual(0, state.nextRequestTime)
        oaiDownloadProcessor.setSet('s')
        oaiDownloadProcessor.setPath('/p')
        oaiDownloadProcessor.setMetadataPrefix('pref')
        oaiDownloadProcessor.observable_setName('aName')
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        state = oaiDownloadProcessor.getState()
        self.assertEqual("x?y&z", state.resumptionToken)
        self.assertEqual('2002-06-01T19:20:30Z', state.from_)
        self.assertEqual(None, state.errorState)
        self.assertEqual('aName', state.name)
        self.assertEqual("/p", state.path)
        self.assertEqual("pref", state.metadataPrefix)
        self.assertEqual('s', state.set)
        self.assertEqual(0, state.nextRequestTime)

        # Change state of oaiDownloadProcessor -> changes stateView.
        oaiDownloadProcessor.setSet('x')
        self.assertEqual('x', state.set)

        oaiDownloadProcessor2 = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        state2 = oaiDownloadProcessor2.getState()
        self.assertEqual(None, state2.name)
        self.assertEqual("oai_dc", state2.metadataPrefix)
        self.assertEqual("x?y&z", state2.resumptionToken)
        self.assertEqual('2002-06-01T19:20:30Z', state2.from_)
        self.assertEqual(None, state2.errorState)
        self.assertEqual(0, state.nextRequestTime)

    def testHarvesterStateWithError(self):
        resumptionToken = "u|c1286437597991025|mprefix|s|f"
        open(join(self.tempdir, 'harvester.state'), 'w').write("Resumptiontoken: %s\n" % resumptionToken)
        observer = CallTrace()
        observer.exceptions={'add': Exception("Could be anything")}
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO(), name="Name")
        oaiDownloadProcessor.addObserver(observer)
        self.assertRaises(Exception, lambda: list(compose(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))))
        state = oaiDownloadProcessor.getState()
        self.assertEqual(resumptionToken, state.resumptionToken)
        self.assertEqual(None, state.from_)
        self.assertEqual("ERROR while processing 'oai:identifier:1': Could be anything", state.errorState)
        self.assertEqual("Name", state.name)

        oaiDownloadProcessor2 = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True, err=StringIO())
        state2 = oaiDownloadProcessor2.getState()
        self.assertEqual(resumptionToken, state2.resumptionToken)
        self.assertEqual("ERROR while processing 'oai:identifier:1': Could be anything", state2.errorState)

    def testPersistentIdentifier(self):
        identifierFilepath = join(self.tempdir, 'harvester.identifier')
        self.assertFalse(isfile(identifierFilepath))
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        currentIdentifier = oaiDownloadProcessor._identifier
        self.assertTrue(isfile(identifierFilepath))
        self.assertEqual(currentIdentifier, open(identifierFilepath).read())
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % currentIdentifier, oaiDownloadProcessor.buildRequest())

        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=True)
        self.assertEqual("""GET /oai?verb=ListRecords&metadataPrefix=oai_dc&x-wait=True HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: %s\r\nUser-Agent: Meresco-Oai-DownloadProcessor/5.x\r\n\r\n""" % currentIdentifier, oaiDownloadProcessor.buildRequest())

    @stdout_replaced
    def testShutdownPersistsStateOnAutocommit(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, autoCommit=False)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        state = oaiDownloadProcessor.getState()
        self.assertFalse(isfile(join(self.tempdir, 'harvester.state')))

        oaiDownloadProcessor.handleShutdown()
        self.assertEqual({"errorState": None, 'from': '2002-06-01T19:20:30Z', "resumptionToken": state.resumptionToken}, load(open(join(self.tempdir, 'harvester.state'))))

    def testResponseDateAsFrom(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % RESUMPTION_TOKEN))))
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

    def testBuildRequestNoneWhenNoResumptionToken(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        self.assertEqual(None, oaiDownloadProcessor.buildRequest())

    def testRestartAfterFinish(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), restartAfterFinish=True)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        request = oaiDownloadProcessor.buildRequest()
        self.assertTrue(request.startswith('GET /oai?verb=ListRecords&metadataPrefix=oai_dc HTTP/1.0\r\nX-Meresco-Oai-Client-Identifier: '), request)

    def testIncrementalHarvestWithFromAfterSomePeriod(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=Schedule(period=10))
        oaiDownloadProcessor._time = lambda: 1.0
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)

        self.assertEqual(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 6.0
        self.assertEqual(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 10.0
        self.assertEqual(None, oaiDownloadProcessor.buildRequest())
        oaiDownloadProcessor._time = lambda: 11.1
        request = oaiDownloadProcessor.buildRequest()
        self.assertTrue(request.startswith('GET /oai?verb=ListRecords&from=2002-06-01T19%3A20%3A30Z&metadataPrefix=oai_dc'), request)

    def testIncrementalHarvestWithFromWithDefaultScheduleMidnight(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor._time = oaiDownloadProcessor._incrementalHarvestSchedule._time = lambda: 0o1 * 60 * 60
        oaiDownloadProcessor._incrementalHarvestSchedule._utcnow = lambda: datetime.strptime("01:00", "%H:%M")
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        self.assertEqual(24 * 60 * 60.0, oaiDownloadProcessor._earliestNextRequestTime)

    def testIncrementalHarvestScheduleNone(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=None)
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        self.assertEqual(None, oaiDownloadProcessor._earliestNextRequestTime)

    def testSetIncrementalHarvestSchedule(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=None)
        oaiDownloadProcessor._time = lambda: 10
        oaiDownloadProcessor.setIncrementalHarvestSchedule(schedule=Schedule(period=3))
        self.assertEqual(0, oaiDownloadProcessor._earliestNextRequestTime)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(13, oaiDownloadProcessor._earliestNextRequestTime)

    def testIncrementalHarvestScheduleNoneOverruledWithSetIncrementalHarvestSchedule(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), incrementalHarvestSchedule=None)
        oaiDownloadProcessor._time = lambda: 10
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(None, oaiDownloadProcessor._resumptionToken)
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        self.assertEqual(None, oaiDownloadProcessor._earliestNextRequestTime)

        oaiDownloadProcessor.setIncrementalHarvestSchedule(schedule=Schedule(period=3))
        self.assertEqual(None, oaiDownloadProcessor.buildRequest())
        self.assertEqual(None, oaiDownloadProcessor._earliestNextRequestTime)
        oaiDownloadProcessor.scheduleNextRequest()
        self.assertNotEqual(None, oaiDownloadProcessor.buildRequest())
        self.assertEqual(0, oaiDownloadProcessor._earliestNextRequestTime)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual(13, oaiDownloadProcessor._earliestNextRequestTime)

    def testSetIncrementalHarvestScheduleNotAllowedInCaseOfRestartAfterFinish(self):
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", workingDirectory=self.tempdir, xWait=False, err=StringIO(), restartAfterFinish=True)
        self.assertRaises(ValueError, lambda: oaiDownloadProcessor.setIncrementalHarvestSchedule(schedule=Schedule(period=3)))

    def testIncrementalHarvestScheduleSetToNone(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", incrementalHarvestSchedule=Schedule(period=0), workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        self.assertNotEqual(None, oaiDownloadProcessor._earliestNextRequestTime)
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], observer.calledMethodNames())

        observer.calledMethods.reset()
        oaiDownloadProcessor.setFrom(from_=None)
        oaiDownloadProcessor.setIncrementalHarvestSchedule(schedule=None)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE))))
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        self.assertEqual(None, oaiDownloadProcessor._earliestNextRequestTime)
        self.assertEqual(['startOaiBatch', 'add', 'stopOaiBatch', 'signalHarvestingDone'], observer.calledMethodNames())

    def testIncrementalHarvestReScheduleIfNoRecordsMatch(self):
        observer = CallTrace(emptyGeneratorMethods=['add'])
        oaiDownloadProcessor = OaiDownloadProcessor(path="/oai", metadataPrefix="oai_dc", incrementalHarvestSchedule=Schedule(period=0), workingDirectory=self.tempdir, xWait=False, err=StringIO())
        oaiDownloadProcessor.addObserver(observer)
        consume(oaiDownloadProcessor.handle(parse(StringIO(LISTRECORDS_RESPONSE % ''))))
        self.assertEqual('2002-06-01T19:20:30Z', oaiDownloadProcessor._from)
        consume(oaiDownloadProcessor.handle(parse(StringIO(NO_RECORDS_MATCH_RESPONSE))))
        self.assertEqual(None, oaiDownloadProcessor._errorState)
        self.assertEqual('2012-06-01T19:20:30Z', oaiDownloadProcessor._from)

ONE_RECORD = '<record xmlns="http://www.openarchives.org/OAI/2.0/"><header><identifier>oai:identifier:1</identifier><datestamp>2011-08-22T07:34:00Z</datestamp></header><metadata>ignored</metadata></record>'

LISTRECORDS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListRecords>
    %s
  </ListRecords>
</OAI-PMH>
""" % (ONE_RECORD + "%s")

ERROR_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <error code="someError">Some error occurred.</error>
</OAI-PMH>
"""

NO_RECORDS_MATCH_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <error code="noRecordsMatch">The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.</error>
</OAI-PMH>
"""

RESUMPTION_TOKEN = """<resumptionToken expirationDate="2002-06-01T23:20:00Z"
      completeListSize="6"
      cursor="0">x?y&amp;z</resumptionToken>"""

ONE_HEADER = '<header xmlns="http://www.openarchives.org/OAI/2.0/"><identifier>oai:identifier:1</identifier><datestamp>2011-08-22T07:34:00Z</datestamp></header>'

LISTIDENTIFIERS_RESPONSE = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2002-06-01T19:20:30Z</responseDate>
  <request verb="ListRecords" from="1998-01-15"
           metadataPrefix="dc">http://an.oa.org/OAI-script</request>
  <ListIdentifiers>
    <!-- ListIdentifiers -->
    %s
  </ListIdentifiers>
</OAI-PMH>
""" % ONE_HEADER

