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

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Created by ziyang.lb
 **/
public class BinlogCommandRecord implements SystemTableRecord {

    public enum COMMAND_STATUS {
        /**
         * 初始状态
         */
        INITIAL(0),
        /**
         * 成功
         */
        SUCCESS(1),
        /**
         * 失败
         */
        FAIL(2);

        COMMAND_STATUS(int value) {
            this.value = value;
        }

        private final int value;

        public int getValue() {
            return value;
        }
    }

    public enum COMMAND_TYPE {
        /**
         * cdc初始化
         */
        CDC_START("CDC_START", true),
        /**
         * 增加存储节点
         */
        ADD_STORAGE("ADD_STORAGE", false),
        /**
         * 删除存储节点
         */
        REMOVE_STORAGE("REMOVE_STORAGE", false),
        /**
         * 触发创建一次全量镜像
         */
        BUILD_META_SNAPSHOT("BUILD_META_SNAPSHOT", true);

        COMMAND_TYPE(String value, boolean forScanner) {
            this.value = value;
            this.forScanner = forScanner;
        }

        private final String value;
        private final boolean forScanner;

        public String getValue() {
            return value;
        }

        public boolean isForScanner() {
            return forScanner;
        }
    }

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String cmdId;
    public String cmdType;
    public String cmdRequest;
    public String cmdReply;
    public int cmdStatus;

    @SuppressWarnings("unchecked")
    @Override
    public BinlogCommandRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.cmdId = rs.getString("cmd_id");
        this.cmdType = rs.getString("cmd_type");
        this.cmdRequest = rs.getString("cmd_request");
        this.cmdReply = rs.getString("cmd_reply");
        this.cmdStatus = rs.getInt("cmd_status");
        return this;
    }

    @Override
    public String toString() {
        return "PolarxCommandRecord{" +
            "id=" + id +
            ", gmtCreated=" + gmtCreated +
            ", gmtModified=" + gmtModified +
            ", cmdId='" + cmdId + '\'' +
            ", cmdType='" + cmdType + '\'' +
            ", cmdRequest='" + cmdRequest + '\'' +
            ", cmdReply='" + cmdReply + '\'' +
            ", cmdStatus=" + cmdStatus +
            '}';
    }
}
