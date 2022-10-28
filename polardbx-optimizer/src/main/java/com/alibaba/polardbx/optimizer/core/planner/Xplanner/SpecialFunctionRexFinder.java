/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

/**
 * @version 1.0
 */
public class SpecialFunctionRexFinder extends RexVisitorImpl<Void> {

    private boolean hasGeoFunction = false;
    private boolean hasTrimFunction = false;

    public SpecialFunctionRexFinder() {
        super(true);
    }

    public boolean isHasGeoFunction() {
        return hasGeoFunction;
    }

    public boolean isHasTrimFunction() {
        return hasTrimFunction;
    }

    @Override
    public Void visitCall(RexCall call) {
        // Naive name check.
        if (SqlKind.OTHER_FUNCTION == call.getKind() &&
            call.getOperator().getName().contains("Geo")) {
            hasGeoFunction = true;
        } else if (SqlKind.TRIM == call.getKind() ||
            SqlKind.LTRIM == call.getKind() ||
            SqlKind.RTRIM == call.getKind()) {
            hasTrimFunction = true;
        }
        // Go deeper.
        return super.visitCall(call);
    }
}
