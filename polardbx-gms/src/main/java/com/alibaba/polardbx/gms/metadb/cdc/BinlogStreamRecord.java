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
