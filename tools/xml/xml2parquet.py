#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import gzip
import collections
import xml.sax
try:
    import lxml.etree
    import lxml.sax
    haveLxml = True
except ImportError:
    haveLxml = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    haveParquet = True
except ImportError:
    haveParquet = False

if 'SUMO_HOME' in os.environ:
    sys.path.append(os.path.join(os.environ['SUMO_HOME'], 'tools'))
import sumolib  # noqa


class ParquetWriter(sumolib.xml.NestingHandler):

    def __init__(self, attrFinder, options):
        sumolib.xml.NestingHandler.__init__(self)
        self.attrFinder = attrFinder
        self.options = options
        self.currentValues = collections.defaultdict(lambda: "")
        self.haveUnsavedValues = False
        self.dataBuffers = {}
        self.rootDepth = 1 if options.split else 0
        for root in sorted(attrFinder.depthTags):
            self.dataBuffers[root] = []
            if options.output:
                if not options.output.isdigit() and not options.output.endswith(".parquet"):
                    options.output += ".parquet"
            else:
                if isinstance(options.source, str):
                    options.output = os.path.splitext(options.source)[0] + ".parquet"
                else:
                    options.output = options.source.name + ".parquet"

# the following two are needed for the lxml saxify to work
    def startElementNS(self, name, qname, attrs):
        self.startElement(qname, attrs)

    def endElementNS(self, name, qname):
        self.endElement(qname)

    def startElement(self, name, attrs):
        sumolib.xml.NestingHandler.startElement(self, name, attrs)
        if self.depth() >= self.rootDepth:
            root = self.tagstack[self.rootDepth]
            if name in self.attrFinder.depthTags[root][self.depth()]:
                for a, v in attrs.items():
                    if isinstance(a, tuple):
                        a = a[1]
                    if a in self.attrFinder.tagAttrs[name]:
                        if self.attrFinder.xsdStruc:
                            enum = self.attrFinder.xsdStruc.getEnumeration(
                                self.attrFinder.tagAttrs[name][a].type)
                            if enum:
                                v = enum.index(v)
                        a2 = self.attrFinder.renamedAttrs.get((name, a), a)
                        self.currentValues[a2] = v
                        self.haveUnsavedValues = True

    def endElement(self, name):
        if self.depth() >= self.rootDepth:
            root = self.tagstack[self.rootDepth]
            if name in self.attrFinder.depthTags[root][self.depth()]:
                if self.haveUnsavedValues:
                    row = {a: self.currentValues[a] for a in self.attrFinder.attrs[root]}
                    self.dataBuffers[root].append(row)
                    self.haveUnsavedValues = False
                for a in self.attrFinder.tagAttrs[name]:
                    a2 = self.attrFinder.renamedAttrs.get((name, a), a)
                    del self.currentValues[a2]
        if self.depth() == 0:
            self._write_parquet_files()
        sumolib.xml.NestingHandler.endElement(self, name)

    def _write_parquet_files(self):
        for root, data in self.dataBuffers.items():
            if data:
                if len(self.attrFinder.depthTags) == 1:
                    outfilename = self.options.output
                else:
                    outfilename = self.options.output + "%s.parquet" % root
                
                # Convert to pandas DataFrame which handles type inference better
                df = pd.DataFrame(data)
                # Replace empty strings and SUMO's "NULL" with None/NaN
                df.replace("", None, inplace=True)
                df.replace("NULL", None, inplace=True)
                # Let pandas infer numeric types
                df = df.infer_objects()
                # Try to convert object columns to numeric where possible
                for col in df.columns:
                    if df[col].dtype == 'object':
                        try:
                            df[col] = pd.to_numeric(df[col])
                        except (ValueError, TypeError):
                            # Keep as string if conversion fails
                            pass
                
                # Convert DataFrame to PyArrow Table and write
                table = pa.Table.from_pandas(df)
                pq.write_table(table, outfilename)


def get_options(arglist=None):
    optParser = sumolib.options.ArgumentParser(description="Convert a XML file to a Parquet file")
    # input
    optParser.add_argument("source", category="input", type=optParser.file,
                           help="the input data (stream given by digits or file")
    # output
    optParser.add_argument("-o", "--output", category="output", type=optParser.file,
                           help="base name for output")
    # processing
    optParser.add_argument("-x", "--xsd", category="processing",
                           help="xsd schema to use")
    optParser.add_argument("-a", "--validation", action="store_true", default=False,
                           help="enable schema validation")
    optParser.add_argument("--keep-attributes", dest="keepAttrs",
                           help="Only keep the given attributes")
    optParser.add_argument("-p", "--split", action="store_true", default=False,
                           help="split in different files for the first hierarchy level")
    options = optParser.parse_args(arglist)
    if options.validation and not haveLxml:
        print("lxml not available, skipping validation", file=sys.stderr)
        options.validation = False
    if options.source.isdigit():
        if not options.xsd:
            print("a schema is mandatory for stream parsing", file=sys.stderr)
            sys.exit()
        options.source = sumolib.miscutils.getSocketStream(int(options.source))
    elif options.source.endswith(".gz"):
        options.source = gzip.open(options.source)
    if options.output and options.output.isdigit() and options.split:
        print("it is not possible to use splitting together with stream output", file=sys.stderr)
        sys.exit()
    if options.keepAttrs:
        options.keepAttrs = set(options.keepAttrs.split(','))
    return options


def main(args=None):
    options = get_options(args)
    # Check parquet availability
    if not haveParquet:
        print("pyarrow not available, please install it with: pip install pyarrow", file=sys.stderr)
        sys.exit(1)
    # get attributes
    attrFinder = sumolib.xml.AttrFinder(options.xsd, options.source, options.split, options.keepAttrs)
    # write parquet
    handler = ParquetWriter(attrFinder, options)
    if options.validation:
        schema = lxml.etree.XMLSchema(file=options.xsd)
        parser = lxml.etree.XMLParser(schema=schema, resolve_entities=False, no_network=True)
        tree = lxml.etree.parse(options.source, parser)
        lxml.sax.saxify(tree, handler)
    else:
        if not options.xsd and hasattr(options.source, "name") and options.source.name.endswith(".gz"):
            # we need to reopen the file because the AttrFinder already read and closed it
            options.source = gzip.open(options.source.name)
        xml.sax.parse(options.source, handler)


if __name__ == "__main__":
    main()
