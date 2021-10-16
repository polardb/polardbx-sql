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

package com.alibaba.polardbx.gms.privilege.quarantine;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class QuarantineConfigAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(QuarantineConfigAccessor.class);

    private static final String SECURITY_IPS = "security_ips";
    private static final String SELECT_ALL_QUARNTINE_CONFIGS = "select * from quarantine_config where inst_id='%s'";
    private static final String DELETE_ALL_QUARNTINE_CONFIGS = "delete from quarantine_config where inst_id=?";

    public QuarantineConfigAccessor() {
    }

    public String getAllQuarntineConfigStr(String instId) {

        try {
            Statement statement = connection.createStatement();
            String config = "";
            String sql = String.format(SELECT_ALL_QUARNTINE_CONFIGS, instId);
            ResultSet rs1 = statement.executeQuery(sql);
            while (rs1.next()) {
                config += "," + rs1.getString(SECURITY_IPS);
            }
            return config;
        } catch (Throwable ex) {
            logger.error("Failed to query the system table 'quarantine_config'", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, ex, "query",
                "quarantine_config",
                ex.getMessage());

        }
    }

    public void deleteQuarntineConfigsByInstId(String instId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.delete(DELETE_ALL_QUARNTINE_CONFIGS, params, this.connection);
    }
}
