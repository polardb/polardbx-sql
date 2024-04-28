package com.alibaba.polardbx.gms.lbac.accessor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
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
public class LBACPolicyAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LBACPolicyAccessor.class);

    private static final String POLICIES_TABLE = wrap(GmsSystemTables.LBAC_POLICIES);

    private static final String FROM_TABLE = " from " + POLICIES_TABLE;

    private static final String ALL_COLUMNS = "`policy_name`,"
        + "`policy_components`";

    private static final String ALL_VALUES = "(?,?)";

    private static final String INSERT_TABLE =
        "insert into " + POLICIES_TABLE + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String SELECT_TABLE =
        "select * " + FROM_TABLE;

    private static final String DELETE_TABLE = "delete " + FROM_TABLE + " where policy_name=?";

    public List<LBACSecurityPolicy> queryAll() {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE)) {
            ResultSet resultSet = statement.executeQuery();
            List<LBACSecurityPolicy> list = new ArrayList<>();
            while (resultSet.next()) {
                try {
                    list.add(LBACAccessorUtils.loadSP(resultSet));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            return list;
        } catch (Exception e) {
            LOGGER.error("Failed to query " + POLICIES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", POLICIES_TABLE,
                e.getMessage());
        }
    }

    public int insert(LBACSecurityPolicy p) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, p.getPolicyName());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, LBACAccessorUtils.getPolicyComponents(p));
            return MetaDbUtil.insert(INSERT_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert " + POLICIES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert", POLICIES_TABLE,
                e.getMessage());
        }
    }

    public int delete(LBACSecurityPolicy p) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, p.getPolicyName());
            return MetaDbUtil.insert(DELETE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete " + POLICIES_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete", POLICIES_TABLE,
                e.getMessage());
        }
    }

}
