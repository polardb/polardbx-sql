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

import java.util.Map;

import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.alibaba.polardbx.common.utils.TreeMaps;
import org.apache.commons.lang.builder.ToStringBuilder;

public class ExtraCmdRouteCondition implements RouteCondition {

    protected String virtualTableName;
    protected Map<String, Object> extraCmds = TreeMaps.synchronizeMap();
    protected ROUTE_TYPE routeType = ROUTE_TYPE.FLUSH_ON_EXECUTE;

    @Override
    public String getVirtualTableName() {
        return virtualTableName;
    }

    @Override
    public void setVirtualTableName(String virtualTableName) {
        this.virtualTableName = virtualTableName;
    }

    @Override
    public ROUTE_TYPE getRouteType() {
        return routeType;
    }

    @Override
    public void setRouteType(ROUTE_TYPE routeType) {
        this.routeType = routeType;
    }

    @Override
    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
    }

    public void putExtraCmd(String key, Object value) {
        this.extraCmds.put(key, value);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
