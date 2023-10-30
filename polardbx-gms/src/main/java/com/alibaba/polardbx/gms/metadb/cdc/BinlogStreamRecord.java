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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class BinlogStreamRecord implements SystemTableRecord {
    private String groupName;
    private String streamName;
    private String fileName;
    private long position;
    private String host;
    private int port;

    @SuppressWarnings("unchecked")
    @Override
    public BinlogStreamRecord fill(ResultSet rs) throws SQLException {
        this.groupName = rs.getString("group_name");
        this.streamName = rs.getString("stream_name");
        String cursor = rs.getString("latest_cursor");
        String endpoint = rs.getString("endpoint");
        if (StringUtils.isNotEmpty(cursor)) {
            Map<String, String> map = JSON.parseObject(cursor, new TypeReference<Map<String, String>>() {
            });
            this.fileName = map.getOrDefault("fileName", "");
            this.position = Long.parseLong(map.getOrDefault("filePosition", "0"));
        }
        if (StringUtils.isNotEmpty(endpoint)) {
            JSONObject ep = JSON.parseObject(endpoint);
            this.host = ep.getString("host");
            this.port = ep.getInteger("port");
        }
        return this;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getFileName() {
        return fileName;
    }

    public long getPosition() {
        return position;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
