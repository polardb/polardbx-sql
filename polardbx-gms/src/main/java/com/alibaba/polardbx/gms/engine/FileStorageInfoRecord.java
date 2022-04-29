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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Data
public class FileStorageInfoRecord implements SystemTableRecord {
    public long id;
    public String instId;
    public String engine;

    public String externalEndpoint;
    public String internalClassicEndpoint;
    public String internalVpcEndpoint;

    public String fileUri;
    public String fileSystemConf;

    public String accessKeyId;
    public String accessKeySecret;

    public long priority;

    public String regionId;
    public String availableZoneId;

    public String gmtCreated;
    public String gmtModified;

    public long cachePolicy;
    public long deletePolicy;
    public long status;

    public long endpointOrdinal;

    @Override
    public FileStorageInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.instId = rs.getString("inst_id");
        this.engine = rs.getString("engine");
        this.externalEndpoint = rs.getString("external_endpoint");
        this.internalClassicEndpoint = rs.getString("internal_classic_endpoint");
        this.internalVpcEndpoint = rs.getString("internal_vpc_endpoint");

        this.fileUri = rs.getString("file_uri");
        this.fileSystemConf = rs.getString("file_system_conf");

        this.accessKeyId = rs.getString("access_key_id");
        this.accessKeySecret = rs.getString("access_key_secret");

        this.priority = rs.getLong("priority");

        this.regionId = rs.getString("region_id");
        this.availableZoneId = rs.getString("azone_id");

        this.gmtCreated = rs.getString("gmt_created");
        this.gmtModified = rs.getString("gmt_modified");
        this.cachePolicy = rs.getLong("cache_policy");
        this.deletePolicy = rs.getLong("delete_policy");
        this.status = rs.getLong("status");

        this.endpointOrdinal = rs.getLong("endpoint_ordinal");

        return this;
    }

    public Map<Integer, ParameterContext> buildInsertParams() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.instId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.engine);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.externalEndpoint);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.internalClassicEndpoint);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.internalVpcEndpoint);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileUri);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.fileSystemConf);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.accessKeyId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.accessKeySecret);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.priority);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.regionId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.availableZoneId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.cachePolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.deletePolicy);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.status);
        return params;
    }
}
