/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.metadb.columnar.ColumnarNodeInfoAccessor;
import com.alibaba.polardbx.gms.metadb.columnar.ColumnarNodeInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.Optional;

@Slf4j
public class ColumnarTargetUtil {

    public static String getDaemonMasterTarget() {
        ColumnarNodeInfoAccessor nodeInfoAccessor = new ColumnarNodeInfoAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            nodeInfoAccessor.setConnection(metaDbConn);
            Optional<ColumnarNodeInfoRecord> nodeInfoRecords = nodeInfoAccessor.getDaemonMaster();
            return nodeInfoRecords.map(ColumnarTargetUtil::getEndpointFromRecord)
                .orElseThrow(() -> new TddlNestableRuntimeException("can not find columnar daemon master endpoint"));
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    private static String getEndpointFromRecord(ColumnarNodeInfoRecord r) {
        return r.getIp() + ":" + r.getDaemonPort();
    }
}
