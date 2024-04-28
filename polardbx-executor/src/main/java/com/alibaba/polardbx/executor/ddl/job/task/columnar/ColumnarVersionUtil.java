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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.polardbx.common.utils.HttpClientHelper;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.net.util.ColumnarTargetUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ColumnarVersionUtil {
    private static final String RPM_PREFIX = "t-polardbx-columnar-";
    private static final String RPM_SUFFIX = ".noarch.rpm";

    public static String getVersion() {
        String result = null;
        try {
            String daemonEndpoint = ColumnarTargetUtil.getDaemonMasterTarget();
            String httpResult = HttpClientHelper.doGet("http://" + daemonEndpoint + "/columnar/system/getVersion");
            final ColumnarVersions columnarVersions = JSONObject.parseObject(httpResult, ColumnarVersions.class);

            result = TStringUtil.substring(
                columnarVersions.polardbxColumnarRpmVersion, RPM_PREFIX.length(),
                TStringUtil.indexOf(columnarVersions.polardbxColumnarRpmVersion, RPM_SUFFIX));
        } catch (Exception e) {
            log.error("get columnar version error", e);
        }
        return result;
    }

    @Getter
    @EqualsAndHashCode
    static class ColumnarVersions {
        private static final String FIELD_NAME_POLARDBX_COLUMNAR_IMAGE_VERSION = "polardbx_columnar_image_version";
        private static final String FIELD_NAME_POLARDBX_COLUMNAR_RPM_VERSION = "polardbx_columnar_rpm_version";
        private static final String FIELD_NAME_POLARDBX_COLUMNAR_DAEMON_VERSION = "polardbx_columnar_daemon_version";
        private static final String FIELD_NAME_POLARDBX_COLUMNAR_FW_BRANCH_NAME = "polardbx_columnar_fw_branch_name";
        private static final String FIELD_NAME_POLARDBX_VERSION = "polardbx_version";
        private static final String FIELD_NAME_POLARDBX_SQL_VERSION = "polardbx_sql_version";
        private static final String FIELD_NAME_POLARDBX_CDC_CLIENT_VERSION = "polardbx_cdc_client_version";

        @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_IMAGE_VERSION)
        private final String polardbxColumnarImageVersion;

        @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_RPM_VERSION)
        private final String polardbxColumnarRpmVersion;

        @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_DAEMON_VERSION)
        private final String polardbxColumnarDaemonVersion;

        @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_FW_BRANCH_NAME)
        private final String polardbxColumnarFwBranchName;

        @JSONField(name = FIELD_NAME_POLARDBX_VERSION)
        private final String polardbxColumnarVersion;

        @JSONField(name = FIELD_NAME_POLARDBX_SQL_VERSION)
        private final String polardbxColumnarSqlVersion;

        @JSONField(name = FIELD_NAME_POLARDBX_CDC_CLIENT_VERSION)
        private final String polardbxColumnarCdcClientVersion;

        @JSONCreator
        public ColumnarVersions(
            @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_IMAGE_VERSION)
            String polardbxColumnarImageVersion,
            @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_RPM_VERSION)
            String polardbxColumnarRpmVersion,
            @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_DAEMON_VERSION)
            String polardbxColumnarDaemonVersion,
            @JSONField(name = FIELD_NAME_POLARDBX_COLUMNAR_FW_BRANCH_NAME)
            String polardbxColumnarFwBranchName,
            @JSONField(name = FIELD_NAME_POLARDBX_VERSION)
            String polardbxColumnarVersion,
            @JSONField(name = FIELD_NAME_POLARDBX_SQL_VERSION)
            String polardbxColumnarSqlVersion,
            @JSONField(name = FIELD_NAME_POLARDBX_CDC_CLIENT_VERSION)
            String polardbxColumnarCdcClientVersion) {
            this.polardbxColumnarImageVersion = polardbxColumnarImageVersion;
            this.polardbxColumnarRpmVersion = polardbxColumnarRpmVersion;
            this.polardbxColumnarDaemonVersion = polardbxColumnarDaemonVersion;
            this.polardbxColumnarFwBranchName = polardbxColumnarFwBranchName;
            this.polardbxColumnarVersion = polardbxColumnarVersion;
            this.polardbxColumnarSqlVersion = polardbxColumnarSqlVersion;
            this.polardbxColumnarCdcClientVersion = polardbxColumnarCdcClientVersion;
        }
    }
}
