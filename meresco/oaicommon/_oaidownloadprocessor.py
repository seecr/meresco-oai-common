## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011-2012, 2014-2016, 2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2012, 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from sys import stderr
from os import makedirs, rename
from os.path import join, isfile, isdir
from traceback import format_exc
from time import time
from urllib.parse import urlencode
from uuid import uuid4
from simplejson import dump, loads

from lxml.etree import ElementTree

from meresco.core import Observable
from meresco.xml import xpath, xpathFirst
from meresco.components import lxmltostring, Schedule
from .__version__ import VERSION
from meresco.xml.namespaces import curieToTag


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}
_UNSPECIFIED = 'UNSPECIFIED'


class OaiDownloadProcessor(Observable):
    def __init__(self, path, metadataPrefix, workingDirectory, set=None, xWait=True, partition=None, err=None, verb=None, autoCommit=True, incrementalHarvestSchedule=_UNSPECIFIED, restartAfterFinish=False, userAgentAddition=None, name=None):
        Observable.__init__(self, name=name)
        self._userAgent = _USER_AGENT + ('' if userAgentAddition is None else ' (%s)' % userAgentAddition)
        self._path = path
        self._metadataPrefix = metadataPrefix
        isdir(workingDirectory) or makedirs(workingDirectory)
        self._stateFilePath = join(workingDirectory, "harvester.state")
        self._set = set
        self._xWait = xWait
        self._partition = partition
        self._err = err or stderr
        self._verb = verb or 'ListRecords'
        self._autoCommit = autoCommit
        if restartAfterFinish and incrementalHarvestSchedule and incrementalHarvestSchedule != _UNSPECIFIED:
            raise ValueError("In case restartAfterFinish==True, incrementalHarvestSchedule must not be set")
        self._restartAfterFinish = restartAfterFinish
        if incrementalHarvestSchedule == _UNSPECIFIED and not restartAfterFinish:
            incrementalHarvestSchedule = Schedule(timeOfDay='00:00')
        self._incrementalHarvestSchedule = incrementalHarvestSchedule
        self._resumptionToken = None
        self._from = None
        self._errorState = None
        self._earliestNextRequestTime = 0
        self._readState()
        self._identifierFilePath = join(workingDirectory, "harvester.identifier")
        if isfile(self._identifierFilePath):
            self._identifier = _open_read(self._identifierFilePath).strip()
        else:
            self._identifier = str(uuid4())
            with open(self._identifierFilePath, 'w') as f: f.write(self._identifier)

    def setPath(self, path):
        self._path = path

    def setMetadataPrefix(self, metadataPrefix):
        self._metadataPrefix = metadataPrefix

    def setSet(self, set):
        self._set = set

    def setFrom(self, from_):
        self._from = from_

    def setResumptionToken(self, resumptionToken):
        self._resumptionToken = resumptionToken

    def setIncrementalHarvestSchedule(self, schedule):
        if self._restartAfterFinish and not schedule is None:
            raise ValueError("In case restartAfterFinish==True, incrementalHarvestSchedule must not be set")
        self._incrementalHarvestSchedule = schedule

    def scheduleNextRequest(self, schedule=_UNSPECIFIED):
        if schedule is None:
            self._earliestNextRequestTime = None
        elif schedule is _UNSPECIFIED:
            self._earliestNextRequestTime = 0
        else:
            self._earliestNextRequestTime = self._time() + schedule.secondsFromNow()

    def buildRequest(self, additionalHeaders=None):
        if not self._timeForNextRequest():
            return None
        arguments = [('verb', self._verb)]
        if self._resumptionToken:
            arguments.append(('resumptionToken', self._resumptionToken))
        else:
            if self._from:
                arguments.append(('from', self._from))
            arguments.append(('metadataPrefix', self._metadataPrefix))
            if self._set:
                arguments.append(('set', self._set))
            if self._partition:
                arguments.append(('x-partition', self._partition))
        if self._xWait:
            arguments.append(('x-wait', 'True'))
        request = "GET %s?%s HTTP/1.0\r\n%s\r\n"
        headers = "X-Meresco-Oai-Client-Identifier: %s\r\n" % self._identifier
        userAgent = self._userAgent
        if additionalHeaders:
            headers += ''.join("{0}: {1}\r\n".format(k, v) for k, v in list(additionalHeaders.items()))
            userAgent = additionalHeaders.pop('User-Agent', self._userAgent)
        headers += "User-Agent: %s\r\n" % userAgent
        return request % (self._path, urlencode(arguments), headers)

    def handle(self, lxmlNode):
        __callstack_var_oaiListRequest__ = {
            'metadataPrefix': self._metadataPrefix,
            'set': self._set,
        }
        harvestingDone = False
        noRecordsMatch = False

        errors = xpath(lxmlNode, "/oai:OAI-PMH/oai:error")
        if len(errors) == 1 and errors[0].get("code") == "noRecordsMatch":
            noRecordsMatch = True
        if len(errors) > 0 and not noRecordsMatch:
            for error in errors:
                self._errorState = "%s: %s" % (error.get("code"), error.text)
                self._logError(self._errorState)
            self._resumptionToken = None
            self._maybeCommit()
            return
        try:
            if not noRecordsMatch:
                self.do.startOaiBatch()
                try:
                    yield self._processRecords(lxmlNode)
                finally:
                    self.do.stopOaiBatch()
            self._from = xpathFirst(lxmlNode, '/oai:OAI-PMH/oai:responseDate/text()')
            if self._resumptionToken is None:
                harvestingDone = True
                if self._restartAfterFinish:
                    self._from = None
                else:
                    self.scheduleNextRequest(self._incrementalHarvestSchedule)
            self._errorState = None
        finally:
            self._maybeCommit()

        if harvestingDone:
            self.do.signalHarvestingDone(state=self.getState())

    def _processRecords(self, lxmlNode):
        verbNode = xpathFirst(lxmlNode, "/oai:OAI-PMH/oai:%s" % self._verb)
        for item in verbNode.iterchildren(tag=VERB_TAGNAME[self._verb]):
            header = None
            for h in item.iterchildren():
                if h.tag == HEADER_TAG:
                    header = h
                    break
            else:
                if item.tag != HEADER_TAG:
                    raise IndexError("Invalid oai header")
                header = item
            for child in header.iterchildren():
                if child.tag == IDENTIFIER_TAG:
                    identifier = child.text
                elif child.tag == DATESTAMP_TAG:
                    datestamp = child.text
            try:
                yield self._add(identifier=identifier, lxmlNode=ElementTree(item), datestamp=datestamp)
            except Exception as e:
                self._logError(format_exc())
                self._logError("While processing:")
                self._logError(lxmltostring(item))
                self._errorState = "ERROR while processing '%s': %s" % (identifier, str(e))
                raise
            yield # some room for others
        self._resumptionToken = xpathFirst(verbNode, "oai:resumptionToken/text()")

    def commit(self):
        tmpFilePath = self._stateFilePath + '.tmp'
        with open(tmpFilePath, 'w') as f:
            dump({
                'from': self._from,
                'resumptionToken': self._resumptionToken,
                'errorState': self._errorState,
            }, f)
        rename(tmpFilePath, self._stateFilePath)

    def getState(self):
        return HarvestStateView(self)

    def handleShutdown(self):
        print('handle shutdown: saving OaiDownloadProcessor %s' % self._stateFilePath)
        from sys import stdout; stdout.flush()
        self.commit()

    def _add(self, identifier, lxmlNode, datestamp):
        yield self.all.add(identifier=identifier, lxmlNode=lxmlNode, datestamp=datestamp)

    def _maybeCommit(self):
        if self._autoCommit:
            self.commit()

    def _readState(self):
        self._resumptionToken = None
        self._errorState = None
        if isfile(self._stateFilePath):
            state = _open_read(self._stateFilePath)
            if not state.startswith('{'):
                if RESUMPTIONTOKEN_STATE in state:
                    self._resumptionToken = state.split(RESUMPTIONTOKEN_STATE)[-1].strip()
                self._maybeCommit()
                return
            d = loads(state)
            self._from = d.get('from')
            self._resumptionToken = d['resumptionToken']
            self._errorState = d['errorState']

    def _logError(self, message):
        self._err.write(message)
        if not message.endswith('\n'):
            self._err.write('\n')
        self._err.flush()

    def _timeForNextRequest(self):
        if self._earliestNextRequestTime is None:
            return False
        return self._time() >= self._earliestNextRequestTime

    def _time(self):
        return time()

def _open_read(path):
    with open(path) as f:
        return f.read()


class HarvestStateView(object):
    def __init__(self, oaiDownloadProcessor):
        self._processor = oaiDownloadProcessor

    @property
    def errorState(self):
        return self._processor._errorState

    @property
    def resumptionToken(self):
        return self._processor._resumptionToken

    @property
    def from_(self):
        return self._processor._from

    @property
    def name(self):
        return self._processor.observable_name()

    @property
    def path(self):
        return self._processor._path

    @property
    def metadataPrefix(self):
        return self._processor._metadataPrefix

    @property
    def set(self):
        return self._processor._set

    @property
    def nextRequestTime(self):
        return self._processor._earliestNextRequestTime

RESUMPTIONTOKEN_STATE = "Resumptiontoken: "

VERB_TAGNAME = {
    'ListRecords': curieToTag('oai:record'),
    'ListIdentifiers': curieToTag('oai:header')
}
_USER_AGENT = "Meresco-Oai-DownloadProcessor/%s" % VERSION

HEADER_TAG = curieToTag('oai:header')
IDENTIFIER_TAG = curieToTag('oai:identifier')
DATESTAMP_TAG = curieToTag('oai:datestamp')

__all__ = ['OaiDownloadProcessor']
