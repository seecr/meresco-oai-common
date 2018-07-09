## begin license ##
#
# "Meresco Oai Common" are utils to support "Meresco Oai".
#
# Copyright (C) 2013-2015, 2018 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from os.path import join, isdir
from os import makedirs
from shutil import rmtree
from itertools import islice

from escaping import escapeFilename

from meresco.components import lxmltostring
from .iterateoaipmh import iterateOaiPmh

def dumpOai(port, path, oaiDumpDir, metadataPrefix, set_=None, host=None, limit=None, append=False):
    host = host or '127.0.0.1'
    baseurl = 'http://%s:%s%s' % (host, port, path)
    if not append:
        isdir(oaiDumpDir) and rmtree(oaiDumpDir)
        makedirs(oaiDumpDir)
    with open(join(oaiDumpDir, 'oai.ids'), 'a') as ids:
        for oaiItem in islice(iterateOaiPmh(baseurl=baseurl, metadataPrefix=metadataPrefix, set=set_), limit):
            filename = '%s.%s' % (oaiItem.identifier, metadataPrefix)
            ids.write('%s %s |%s|\n' % ('DEL' if oaiItem.deleted else 'ADD', filename, '|'.join(sorted(oaiItem.setSpecs))))
            if not oaiItem.deleted:
                open(join(oaiDumpDir, escapeFilename(filename)), 'w').write(lxmltostring(oaiItem.metadata, pretty_print=True))
    print "Oai dump created in %s" % oaiDumpDir
