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

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogDumperAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogDumperRecord;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogNodeInfoAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogNodeInfoRecord;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

/**
 * @author yudong
 * @since 2023/3/1 10:32
 **/
@Slf4j
public class CdcTargetUtil {
    public static String getDumperMasterTarget() {
        BinlogDumperAccessor binlogDumperAccessor = new BinlogDumperAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            binlogDumperAccessor.setConnection(metaDbConn);
            Optional<BinlogDumperRecord> dumperMaster = binlogDumperAccessor.getDumperMaster();
            return dumperMaster.map(CdcTargetUtil::getEndpointFromRecord)
                .orElseThrow(() -> new TddlNestableRuntimeException("can not find dumper master endpoint"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static String getDumperTarget(String streamName) {
        BinlogStreamAccessor cdcStreamAccessor = new BinlogStreamAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            cdcStreamAccessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> dumpers = cdcStreamAccessor.getStream(streamName);
            if (CollectionUtils.isEmpty(dumpers)) {
                throw new TddlNestableRuntimeException("can not find dumper-x endpoint of stream: " + streamName);
            }
            BinlogStreamRecord cdr = dumpers.get(0);
            return cdr.getHost() + ":" + cdr.getPort();
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static String getDaemonMasterTarget() {
        BinlogNodeInfoAccessor nodeInfoAccessor = new BinlogNodeInfoAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            nodeInfoAccessor.setConnection(metaDbConn);
            Optional<BinlogNodeInfoRecord> nodeInfoRecords = nodeInfoAccessor.getDaemonMaster();
            return nodeInfoRecords.map(CdcTargetUtil::getEndpointFromRecord)
                .orElseThrow(() -> new TddlNestableRuntimeException("can not find daemon master endpoint"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static String getDaemonMasterTarget(String groupName) {
        BinlogNodeInfoAccessor nodeInfoAccessor = new BinlogNodeInfoAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            nodeInfoAccessor.setConnection(metaDbConn);
            Optional<BinlogNodeInfoRecord> nodeInfoRecords = nodeInfoAccessor.getDaemonMaster(groupName);
            return nodeInfoRecords.map(CdcTargetUtil::getEndpointFromRecord)
                .orElseThrow(() -> new TddlNestableRuntimeException(
                    "can not find daemon master endpoint of group: " + groupName));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    private static String getEndpointFromRecord(BinlogNodeInfoRecord r) {
        return r.getIp() + ":" + r.getDaemonPort();
    }

    private static String getEndpointFromRecord(BinlogDumperRecord r) {
        return r.getIp() + ":" + r.getPort();
    }

    public static String getReplicaDaemonMasterTarget() {
        BinlogNodeInfoAccessor nodeInfoAccessor = new BinlogNodeInfoAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            nodeInfoAccessor.setConnection(metaDbConn);
            Optional<BinlogNodeInfoRecord> nodeInfoRecords = nodeInfoAccessor.getReplicaDaemonMaster();
            return nodeInfoRecords.map(CdcTargetUtil::getEndpointFromRecord)
                .orElseThrow(() -> new TddlNestableRuntimeException("can not find daemon master endpoint"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
