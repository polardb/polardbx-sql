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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author chenghui.lch
 */
public class StorageInfoRecord implements SystemTableRecord {

    public static final int STORAGE_TYPE_XCLUSTER = 0;
    public static final int STORAGE_TYPE_MYSQL = 1;
    public static final int STORAGE_TYPE_POLARDB = 2;
    public static final int STORAGE_TYPE_RDS80_XCLUSTER = 3;
    public static final int STORAGE_TYPE_GALAXY_SINGLE = 4;
    public static final int STORAGE_TYPE_GALAXY_CLUSTER = 5;

    public static final int INST_KIND_MASTER = 0;
    public static final int INST_KIND_SLAVE = 1;
    public static final int INST_KIND_META_DB = 2;

    public static final int STORAGE_STATUS_READY = 0;
    public static final int STORAGE_STATUS_NOT_READY = 1;
    public static final int STORAGE_STATUS_REMOVED = 2;

    public static final int IS_VIP_FALSE = 0;
    public static final int IS_VIP_TRUE = 1;

    public long id;
    public Timestamp gmtCreated;
    public Timestamp gmtModified;
    public String instId;
    public String storageInstId;
    public String storageMasterInstId;
    public String ip;
    /**
     * port of jdbc
     */
    public int port;
    /**
     * port of x-protocol
     */
    public int xport;
    public String user;
    public String passwdEnc;

    /**
     * storage type
     * <pre>
     * type=0: x-cluster, means these node are x-cluster
     * type=1: mysql, means these node are just mysql
     * type=2: polardb, means these node are polardb
     * </pre>
     */
    public int storageType;

    /**
     * stoarge inst type
     * <pre>
     * kind=0: master, means these node are the master inst of storage
     * kind=1: slave, means these node are the slave inst of storage
     * kind=2: metadb, means these node are the meta inst of storage
     * </pre>
     */
    public int instKind;

    /**
     * stoarge type of storage inst
     * <pre>
     *  status=0: ready
     *  status=1: no ready (used by scale out in adding new storage inst)
     *  status=2: removed
     * </pre>
     */
    public int status;
    public String regionId;
    public String azoneId;
    public String idcId;
    public int maxConn;
    public int cpuCore;
    public int memSize;

    /**
     * label if the ip is a vip
     * <pre>
     *     is_vip=0: ip is NOT a vip
     *     is_vip=1: ip is a vip ,and port is a vip port
     * </pre>
     */
    public int isVip;
    public String extras;

    @Override
    public StorageInfoRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.instId = rs.getString("inst_id");
        this.storageInstId = rs.getString("storage_inst_id");
        this.storageMasterInstId = rs.getString("storage_master_inst_id");
        this.ip = rs.getString("ip");
        this.port = rs.getInt("port");
        this.xport = rs.getInt("xport");
        this.user = rs.getString("user");
        this.passwdEnc = rs.getString("passwd_enc");
        this.storageType = rs.getInt("storage_type");
        this.instKind = rs.getInt("inst_kind");
        this.status = rs.getInt("status");
        this.regionId = rs.getString("region_id");
        this.azoneId = rs.getString("azone_id");
        this.idcId = rs.getString("idc_id");
        this.maxConn = rs.getInt("max_conn");
        this.cpuCore = rs.getInt("cpu_core");
        this.memSize = rs.getInt("mem_size"); // Unit: MB
        this.isVip = rs.getInt("is_vip");
        this.extras = rs.getString("extras");
        return this;
    }

    public StorageInfoRecord copy() {
        StorageInfoRecord newRecord = new StorageInfoRecord();
        newRecord.id = this.id;
        newRecord.gmtCreated = this.gmtCreated;
        newRecord.gmtModified = this.gmtModified;

        newRecord.instId = this.instId;
        newRecord.storageInstId = this.storageInstId;
        newRecord.storageMasterInstId = this.storageMasterInstId;
        newRecord.ip = this.ip;
        newRecord.port = this.port;
        newRecord.xport = this.xport;

        newRecord.user = this.user;
        newRecord.passwdEnc = this.passwdEnc;

        newRecord.storageType = this.storageType;
        newRecord.instKind = this.instKind;
        newRecord.status = this.status;

        newRecord.regionId = this.regionId;
        newRecord.azoneId = this.azoneId;
        newRecord.idcId = this.idcId;

        newRecord.maxConn = this.maxConn;
        newRecord.cpuCore = this.cpuCore;
        newRecord.memSize = this.memSize;

        newRecord.isVip = this.isVip;
        newRecord.extras = this.extras;

        return newRecord;

    }

    public boolean isStatusReady() {
        return this.status == STORAGE_STATUS_READY;
    }

    public String getRegionId() {
        return regionId;
    }

    public String getAzoneId() {
        return azoneId;
    }

    public String getIdcId() {
        return idcId;
    }

    public String getIp() {
        return ip;
    }

    public String getHostPort() {
        return String.format("%s:%d", ip, port);
    }

    public String getInstanceId() {
        return this.storageInstId;
    }

    @Override
    public String toString() {
        return "StorageInfoRecord{" +
            "id=" + id +
            ", instId='" + instId + '\'' +
            ", storageInstId='" + storageInstId + '\'' +
            ", ip='" + ip + '\'' +
            ", port=" + port +
            ", xport=" + xport +
            ", status=" + status +
            '}';
    }

    public static String getInstKind(int instKind) {
        switch (instKind) {
        case StorageInfoRecord.INST_KIND_MASTER:
            return "MASTER";
        case StorageInfoRecord.INST_KIND_SLAVE:
            return "SLAVE";
        case StorageInfoRecord.INST_KIND_META_DB:
            return "META_DB";
        default:
            return "NA";
        }
    }
}
