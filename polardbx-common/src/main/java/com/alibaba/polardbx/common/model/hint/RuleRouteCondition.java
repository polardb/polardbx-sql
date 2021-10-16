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

package com.alibaba.polardbx.common.model.hint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.utils.TreeMaps;

public class RuleRouteCondition extends ExtraCmdRouteCondition implements RouteCondition, ComparativeMapChoicer {

    private Map<String, Comparative> parameters = TreeMaps.caseInsensitiveMap();

    public void put(String key, Comparable<?> parameter) {
        if (key == null) {
            throw new IllegalArgumentException("key为null");
        }
        if (parameter instanceof Comparative) {
            parameters.put(key, (Comparative) parameter);
        } else {

            parameters.put(key, getComparative(Comparative.Equivalent, parameter));
        }

    }

    public Comparative getComparative(int i, Comparable<?> c) {
        return new Comparative(i, c);
    }

    @Override
    public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
        Map<String, Comparative> retMap = new HashMap<String, Comparative>(parameters.size());
        for (String str : partnationSet) {
            if (str != null) {

                Comparative comp = parameters.get(str);
                if (comp != null) {
                    retMap.put(str, comp);
                }
            }
        }
        return retMap;
    }

    @Override
    public Comparative getColumnComparative(List<Object> arguments, String colName) {
        Comparative res = null;
        if (colName != null) {

            Comparative comp = parameters.get(colName);
            if (comp != null) {
                res = comp;
            }
        }

        return res;
    }

    public Map<String, Comparative> getParameters() {
        return parameters;
    }

    public ComparativeMapChoicer getCompMapChoicer() {
        return this;
    }
}
