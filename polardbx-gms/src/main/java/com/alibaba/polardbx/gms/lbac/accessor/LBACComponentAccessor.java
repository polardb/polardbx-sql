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

package com.alibaba.polardbx.gms.lbac.accessor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class LBACComponentAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LBACComponentAccessor.class);

    private static final String COMPONENTS_TABLE = wrap(GmsSystemTables.LBAC_COMPONENTS);

    private static final String FROM_TABLE = " from " + COMPONENTS_TABLE;

    private static final String ALL_COLUMNS = "`component_name`,"
        + "`component_type`,"
        + "`component_content`";

    private static final String ALL_VALUES = "(?,?,?)";

    private static final String INSERT_TABLE =
        "insert into " + COMPONENTS_TABLE + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String SELECT_TABLE =
        "select * " + FROM_TABLE;

    private static final String DELETE_TABLE = "delete " + FROM_TABLE + " where component_name=?";

    public List<LBACSecurityLabelComponent> queryAll() {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE)) {
            ResultSet resultSet = statement.executeQuery();
            List<LBACSecurityLabelComponent> list = new ArrayList<>();
            while (resultSet.next()) {
                try {
                    list.add(LBACAccessorUtils.loadSLC(resultSet));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            return list;
        } catch (Exception e) {
            LOGGER.error("Failed to query " + COMPONENTS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", COMPONENTS_TABLE,
                e.getMessage());
        }
    }

    public int insert(LBACSecurityLabelComponent c) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, c.getComponentName());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, c.getType().name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, LBACAccessorUtils.getComponentContent(c));
            return MetaDbUtil.insert(INSERT_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert " + COMPONENTS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert", COMPONENTS_TABLE,
                e.getMessage());
        }

    }

    public int delete(LBACSecurityLabelComponent c) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, c.getComponentName());
            return MetaDbUtil.insert(DELETE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete " + COMPONENTS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete", COMPONENTS_TABLE,
                e.getMessage());
        }

    }

}
