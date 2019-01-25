#!/usr/bin/env python
# Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.org/sumo
# Copyright (C) 2009-2019 German Aerospace Center (DLR) and others.
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v20.html
# SPDX-License-Identifier: EPL-2.0

# @file    test.py
# @author  Pablo Alvarez Lopez
# @date    2016-11-25
# @version $Id$

# import common functions for netedit tests
import os
import sys

testRoot = os.path.join(os.environ.get('SUMO_HOME', '.'), 'tests')
neteditTestRoot = os.path.join(
    os.environ.get('TEXTTEST_HOME', testRoot), 'netedit')
sys.path.append(neteditTestRoot)
import neteditTestFunctions as netedit  # noqa

# Open netedit
neteditProcess, referencePosition = netedit.setupAndStart(neteditTestRoot, [])

# apply zoom
netedit.setZoom("25", "20", "25")

# go to additional mode
netedit.additionalMode()

# select BusStop
netedit.changeAdditional("busStop")

# create BusStop with default parameters
netedit.leftClick(referencePosition, 375, 250)

# select Access
netedit.changeAdditional("access")

# Create Access
netedit.selectAdditionalChild(7, 0)
netedit.leftClick(referencePosition, 200, 250)

# go to inspect mode
netedit.inspectMode()

# inspect Access
netedit.leftClick(referencePosition, 208, 260)

# Change lenght (invalid)
netedit.modifyAttribute(6, "dummyLenght")

# Change lenght (invalid, negative)
netedit.modifyAttribute(6, "-30")

# Change lenght (valid, positive)
netedit.modifyAttribute(6, "5")

# Check undo redo
netedit.undo(referencePosition, 3)
netedit.redo(referencePosition, 3)

# save additionals
netedit.saveAdditionals()

# save network
netedit.saveNetwork()

# quit netedit
netedit.quit(neteditProcess)
