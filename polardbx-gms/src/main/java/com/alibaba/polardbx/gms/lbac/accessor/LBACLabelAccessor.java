package com.alibaba.polardbx.gms.lbac.accessor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
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
public class LBACLabelAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LBACLabelAccessor.class);

    private static final String LABELS_TABLE = wrap(GmsSystemTables.LBAC_LABELS);

    private static final String FROM_TABLE = " from " + LABELS_TABLE;

    private static final String ALL_COLUMNS = "`label_name`,"
        + "`label_policy`,"
        + "`label_content`";

    private static final String ALL_VALUES = "(?,?,?)";

    private static final String INSERT_TABLE =
        "insert into " + LABELS_TABLE + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String SELECT_TABLE =
        "select * " + FROM_TABLE;

    private static final String DELETE_TABLE = "delete from " + LABELS_TABLE + " where label_name=?";

    public List<LBACSecurityLabel> queryAll() {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE)) {
            ResultSet resultSet = statement.executeQuery();
            List<LBACSecurityLabel> list = new ArrayList<>();
            while (resultSet.next()) {
                try {
                    list.add(LBACAccessorUtils.loadSL(resultSet));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            return list;
        } catch (Exception e) {
            LOGGER.error("Failed to query " + LABELS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", LABELS_TABLE,
                e.getMessage());
        }
    }

    public int insert(LBACSecurityLabel l) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, l.getLabelName());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, l.getPolicyName());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, LBACAccessorUtils.getLabelComponent(l));
            return MetaDbUtil.insert(INSERT_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert " + LABELS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert", LABELS_TABLE,
                e.getMessage());
        }
    }

    public int delete(LBACSecurityLabel l) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, l.getLabelName());
            return MetaDbUtil.insert(DELETE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete " + LABELS_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete", LABELS_TABLE,
                e.getMessage());
        }
    }

}
