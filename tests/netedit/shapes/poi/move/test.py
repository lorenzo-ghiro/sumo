#!/usr/bin/env python
# Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
# Copyright (C) 2009-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# This Source Code may also be made available under the following Secondary
# Licenses when the conditions for such availability set forth in the Eclipse
# Public License 2.0 are satisfied: GNU General Public License, version 2
# or later which is available at
# https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later

# @file    test.py
# @author  Pablo Alvarez Lopez
# @date    2016-11-25

# import common functions for netedit tests
import os
import sys

testRoot = os.path.join(os.environ.get('SUMO_HOME', '.'), 'tests')
neteditTestRoot = os.path.join(
    os.environ.get('TEXTTEST_HOME', testRoot), 'netedit')
sys.path.append(neteditTestRoot)
import neteditTestFunctions as netedit  # noqa

# Open netedit
neteditProcess, referencePosition = netedit.setupAndStart(neteditTestRoot)

# go to shape mode
netedit.shapeMode()

# go to shape mode
netedit.changeElement("poi")

# create poi
netedit.leftClick(referencePosition, 336, 136)

# change color to white (To see icon)
netedit.changeDefaultValue(netedit.attrs.poi.create.color, "white")

# Change parameter width with a valid value (To see icon)
netedit.changeDefaultValue(netedit.attrs.poi.create.width, "10")

# Change parameter height with a valid value (To see icon)
netedit.changeDefaultValue(netedit.attrs.poi.create.height, "10")

# change imgfile (valid)
netedit.changeDefaultValue(netedit.attrs.poi.create.imgFile, "berlin_icon.ico")

# create poi
netedit.leftClick(referencePosition, 336, 345)

# go to move mode
netedit.moveMode()

# move first POI to left down
netedit.moveElement(referencePosition, 336, 136, 634, 188)

# move second POI to left up
netedit.moveElement(referencePosition, 336, 345, 634, 277)

# Check undo redo
netedit.checkUndoRedo(referencePosition)

# save Netedit config
netedit.saveNeteditConfig(referencePosition)

# quit netedit
netedit.quit(neteditProcess)