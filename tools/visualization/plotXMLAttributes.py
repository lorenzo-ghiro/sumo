#!/usr/bin/env python
# Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.org/sumo
# Copyright (C) 2007-2021 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the Eclipse
# Public License 2.0 are satisfied: GNU General Public License, version 2
# or later which is available at
# https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later

# @file    plotXMLAttributes.py
# @author  Jakob Erdmann
# @date    2021-11-05

"""
This script plots arbitrary xml attributes from xml files
Individual trajectories can be clicked in interactive mode to print the data Id on the console

selects two attributs for x and y axis and a third (id-attribute) for grouping
of data points into lines

"""
from __future__ import absolute_import
from __future__ import print_function
import os
import re
import sys
sys.path.append(os.path.join(os.path.dirname(sys.argv[0]), '..'))
from collections import defaultdict
from optparse import OptionParser
import matplotlib
if 'matplotlib.backends' not in sys.modules:
    if 'TEXTTEST_SANDBOX' in os.environ or (os.name == 'posix' and 'DISPLAY' not in os.environ):
        matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa
import math  # noqa

try:
    import xml.etree.cElementTree as ET
except ImportError as e:
    print("recovering from ImportError '%s'" % e)
    import xml.etree.ElementTree as ET

from sumolib.xml import parse_fast, parse_fast_nested, _open  # noqa
from sumolib.miscutils import uMin, uMax, parseTime  # noqa

def getOptions(args=None):
    optParser = OptionParser()
    optParser.add_option("-x", "--xattr",  help="attribute for x-axis")
    optParser.add_option("-y", "--yattr",  help="attribute for y-axis")
    optParser.add_option("-i", "--idattr",  help="attribute for grouping data points into lines")
    optParser.add_option("--xelem",  help="element for x-axis")
    optParser.add_option("--yelem",  help="element for y-axis")
    optParser.add_option("--idelem",  help="element for grouping data points into lines")
    optParser.add_option("-s", "--show", action="store_true", default=False, help="show plot directly")
    optParser.add_option("-o", "--output", help="outputfile for saving plots", default="plot.png")
    optParser.add_option("--csv-output", dest="csv_output", help="write plot as csv", metavar="FILE")
    optParser.add_option("--filter-ids", dest="filterIDs", help="only plot data points from the given list of ids")
    optParser.add_option("-p", "--pick-distance", dest="pickDist", type="float", default=1,
                         help="pick lines within the given distance in interactive plot mode")
    optParser.add_option("--label", help="plot label (default input file name")
    optParser.add_option("--invert-yaxis", dest="invertYAxis", action="store_true",
                         default=False, help="Invert the Y-Axis")
    optParser.add_option("--legend", action="store_true", default=False, help="Add legend")
    optParser.add_option("-v", "--verbose", action="store_true", default=False, help="tell me what you are doing")

    options, args = optParser.parse_args(args=args)
    if len(args) < 1:
        sys.exit("mandatory argument XML_FILE missing")
    options.files = args

    options.attrOptions = ['idattr', 'xattr', 'yattr']

    if options.filterIDs is not None:
        options.filterIDs = set(options.filterIDs.split(','))

    for a in options.attrOptions:
        if getattr(options, a) is None:
            sys.exit("mandatory argument --%s is missing" % a)

    return options


def write_csv(data, fname):
    with open(fname, 'w') as f:
        for veh, vals in sorted(data.items()):
            f.write('"%s"\n' % veh)
            for x in zip(*vals):
                f.write(" ".join(map(str, x)) + "\n")
            f.write('\n')


def short_names(filenames):
    if len(filenames) == 1:
        return filenames
    reversedNames = [''.join(reversed(f)) for f in filenames]
    prefixLen = len(os.path.commonprefix(filenames))
    suffixLen = len(os.path.commonprefix(reversedNames))
    return [f[prefixLen:-suffixLen] for f in filenames]


def onpick(event):
    mevent = event.mouseevent
    print("veh=%s x=%d y=%d" % (event.label, mevent.xdata, mevent.ydata))

def getDataStream(options):
    # determine elements and nesting for the given attributes
    # by reading from the first file

    attrOptions = options.attrOptions
    attr2elem = {} 
    elem2level = {}

    level = 0
    for event, elem in ET.iterparse(_open(options.files[0]), ("start", "end")):
        if event == "start":
            level += 1
            for a in attrOptions:
                attr = getattr(options, a)
                if attr in elem.keys():
                    elem2level[elem.tag] = level
                    if attr in attr2elem:
                        oldTag = attr2elem[attr]
                        if oldTag != elem.tag:
                            if elem2level[oldTag] < level:
                                attr2elem[attr] = elem.tag
                            print("Warning: found %s '%s' in element %s (level %s) and element %s (level %s). Using %s" % (
                                a, attr, oldTag, elem2level[oldTag], elem.tag, level, attr2elem[attr]))
                    else:
                        attr2elem[attr] = elem.tag
            if len(attr2elem) == 3:
                # all attributes have been seen
                break
        elif event == "end":
            level -= 1
        
    if len(attr2elem) != 3:
        for a in attrOptions:
            attr = getattr(options, a)
            if not attr in attr2elem:
                sys.exit("%s '%s' not found in %s" % (a, attr, options.files[0]))


    allElems = list(set(attr2elem.values()))
    attrs = [getattr(options, a) for a in attrOptions]

    # we don't know the order of the elements and we cannot get it from our xml parser

    if len(allElems) == 2:
        def datastream(xmlfile):
            elems = sorted(allElems, key=lambda e : elem2level[e])
            mE0 = "<%s " % elems[0]
            mE1 = "<%s " % elems[1]
            attrs0 = [a for a in attrs if attr2elem[a] == elems[0]]
            attrs1 = [a for a in attrs if attr2elem[a] == elems[1]]
            mAs0 = [(a, re.compile('%s="([^"]*)"' % a)) for a in attrs0]
            mAs1 = [(a, re.compile('%s="([^"]*)"' % a)) for a in attrs1]

            values = {} # attr -> value
            for line in _open(xmlfile):
                if mE0 in line:
                    for a, r in mAs0:
                        values[a] = r.search(line).groups()[0]
                if mE1 in line:
                    for a, r in mAs1:
                        values[a] = r.search(line).groups()[0]
                    yield [values[a] for a in attrs]
        return datastream

    elif len(allElems) == 1:
        def datastream(xmlfile):
            mE = "<%s " % allElems[0]
            mAs = [re.compile('%s="([^"]*)"' % a) for a in attrs]
            for line in _open(xmlfile):
                if mE in line:
                    matches = [r.search(line) for r in mAs]
                    if all(matches):
                        yield [m.groups()[0] for m in matches]
        return datastream

    else:
        sys.exit("Found attributes at elements %s but at most 2 elements are supported" % elems)

def main(options):

    dataStream = getDataStream(options)

    fig = plt.figure(figsize=(14, 9), dpi=100)
    fig.canvas.mpl_connect('pick_event', onpick)

    shortFileNames = short_names(options.files)
    plt.xlabel(options.xattr)
    plt.ylabel(options.yattr)
    plt.title(','.join(shortFileNames) if options.label is None else options.label)
    xdata = 0
    ydata = 1

    data = defaultdict(lambda: tuple(([] for i in range(2))))
    for fileIndex, datafile in enumerate(options.files):
        totalIDs = 0
        filteredIDs = 0
        for dataID, x, y in dataStream(datafile):
            totalIDs += 1
            if options.filterIDs and dataID not in options.filterIDs:
                continue
            if len(options.files) > 1:
                suffix = shortFileNames[fileIndex]
                if len(suffix) > 0:
                    dataID += "#" + suffix
            x = parseTime(x)
            y = parseTime(y)
            data[dataID][xdata].append(x)
            data[dataID][ydata].append(y)
            filteredIDs += 1
        if totalIDs == 0 or filteredIDs == 0 or options.verbose:
            print("Found %s datapoints in %s and kept %s" % (
                totalIDs, datafile, filteredIDs))

    if filteredIDs == 0:
        sys.exit()

    def line_picker(line, mouseevent):
        if mouseevent.xdata is None:
            return False, dict()
        # minxy = None
        # mindist = 10000
        for x, y in zip(line.get_xdata(), line.get_ydata()):
            dist = math.sqrt((x - mouseevent.xdata) ** 2 + (y - mouseevent.ydata) ** 2)
            if dist < options.pickDist:
                return True, dict(label=line.get_label())
            # else:
            #    if dist < mindist:
            #        print("   ", x,y, dist, (x - mouseevent.xdata) ** 2, (y - mouseevent.ydata) ** 2)
            #        mindist = dist
            #        minxy = (x, y)
        # print(mouseevent.xdata, mouseevent.ydata, minxy, dist,
        #        line.get_label())
        return False, dict()

    minY = uMax
    maxY = uMin
    minX = uMax
    maxX = uMin

    for dataID, d in data.items():

        minY = min(minY, min(d[ydata]))
        maxY = max(maxY, max(d[ydata]))
        minX = min(minX, min(d[xdata]))
        maxX = max(maxX, max(d[xdata]))

        plt.plot(d[xdata], d[ydata], picker=line_picker, label=dataID)
    if options.invertYAxis:
        plt.axis([minX, maxX, maxY, minY])

    if options.legend > 0:
        plt.legend()

    plt.savefig(options.output)
    if options.csv_output is not None:
        write_csv(data, options.csv_output)
    if options.show:
        plt.show()


if __name__ == "__main__":
    main(getOptions())
