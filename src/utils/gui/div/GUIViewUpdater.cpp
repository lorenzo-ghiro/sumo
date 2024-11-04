/****************************************************************************/
// Eclipse SUMO, Simulation of Urban MObility; see https://eclipse.dev/sumo
// Copyright (C) 2001-2024 German Aerospace Center (DLR) and others.
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// https://www.eclipse.org/legal/epl-2.0/
// This Source Code may also be made available under the following Secondary
// Licenses when the conditions for such availability set forth in the Eclipse
// Public License 2.0 are satisfied: GNU General Public License, version 2
// or later which is available at
// https://www.gnu.org/licenses/old-licenses/gpl-2.0-standalone.html
// SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
/****************************************************************************/
/// @file    GUIViewUpdater.cpp
/// @author  Pablo Alvarez Lopez
/// @date    Nov 2024
///
// class used for enable/disable updating view
/****************************************************************************/
#include <config.h>

#include <utils/common/MsgHandler.h>

#include "GUIViewUpdater.h"

// ===========================================================================
// method definitions
// ===========================================================================

GUIViewUpdater::GUIViewUpdater() {}


bool
GUIViewUpdater::allowUpdate() const {
    return myAllowUpdate == 0;
}


void
GUIViewUpdater::enableUpdate() {
    myAllowUpdate--;
    if (myAllowUpdate < 0) {
        WRITE_ERROR("myAllowUpdate cannot be less than 0");
    }
}


void
GUIViewUpdater::disableUpdate() {
    myAllowUpdate++;
}

/****************************************************************************/