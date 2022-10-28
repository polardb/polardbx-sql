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

package com.alibaba.polardbx.gms.heatmap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

/**
 * @author ximing.yd
 * @date 2022/2/23 4:43 下午
 */
public class PartitionsHeatmapRecord implements SystemTableRecord {

    public Long id;
    private Integer layerNum;
    private Long timestamp;
    private String axis;

    @Override
    public PartitionsHeatmapRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.layerNum = rs.getInt("layer_num");
        this.timestamp = rs.getLong("timestamp");
        this.axis = rs.getString("axis");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        // MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.id);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.layerNum);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.timestamp);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.axis);
        return params;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getLayerNum() {
        return layerNum;
    }

    public void setLayerNum(Integer layerNum) {
        this.layerNum = layerNum;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getAxis() {
        return axis;
    }

    public void setAxis(String axis) {
        this.axis = axis;
    }
}
