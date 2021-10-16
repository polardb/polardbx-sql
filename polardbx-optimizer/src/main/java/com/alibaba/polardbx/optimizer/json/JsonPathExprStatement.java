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

package com.alibaba.polardbx.optimizer.json;

import java.util.List;

/**
 * @author arnkore 2017-07-12 16:39
 */
public class JsonPathExprStatement {

    private List<AbstractPathLeg> pathLegs;

    public JsonPathExprStatement(List<AbstractPathLeg> pathLegs) {
        this.pathLegs = pathLegs;
    }

    @Override
    public String toString() {
        StringBuilder appendable = new StringBuilder();
        appendable.append("$");

        for (int i = 0; i < pathLegs.size(); i++) {
            if (i == 0 && pathLegs.get(i) instanceof ArrayLocation
                && ((ArrayLocation) pathLegs.get(i)).isAsterisk()) {
                // skip first asterisk
                continue;
            }
            appendable.append(pathLegs.get(i).toString());
        }

        return appendable.toString();
    }

    public List<AbstractPathLeg> getPathLegs() {
        return pathLegs;
    }

    public void setPathLegs(List<AbstractPathLeg> pathLegs) {
        this.pathLegs = pathLegs;
    }
}
