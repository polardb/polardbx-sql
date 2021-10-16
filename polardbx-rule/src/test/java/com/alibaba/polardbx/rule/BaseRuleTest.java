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

package com.alibaba.polardbx.rule;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.utils.TreeMaps;

@Ignore
public class BaseRuleTest {

    protected Comparative or(Comparable... values) {
        ComparativeOR and = new ComparativeOR();
        for (Comparable obj : values) {
            and.addComparative(new Comparative(Comparative.Equivalent, obj));
        }
        return and;
    }

    protected Comparative and(Comparative... values) {
        ComparativeAND and = new ComparativeAND();
        for (Comparative obj : values) {
            and.addComparative(obj);
        }
        return and;
    }

    protected static class Choicer implements ComparativeMapChoicer {

        private Map<String, Comparative> comparatives = TreeMaps.caseInsensitiveMap();

        public Choicer(){

        }

        public Choicer(Map<String, Comparative> comparatives){
            this.comparatives = comparatives;
        }

        public void addComparative(String name, Comparative comparative) {
            this.comparatives.put(name, comparative);
        }

        public Comparative getColumnComparative(List<Object> arguments, String colName) {
            return comparatives.get(colName);
        }

        public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
            return null;
        }
    }
}
