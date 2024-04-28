package com.alibaba.polardbx.gms.lbac.accessor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
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
public class LBACEntityAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LBACEntityAccessor.class);

    private static final String ENTITY_SECURITY_ATTR_TABLE = wrap(GmsSystemTables.LBAC_ENTITY);

    private static final String FROM_TABLE = " from " + ENTITY_SECURITY_ATTR_TABLE;

    private static final String ALL_COLUMNS = "`entity_id`,"
        + "`entity_key`,"
        + "`entity_type`,"
        + "`security_attr`";

    private static final String ALL_VALUES = "(null,?,?,?)";

    private static final String INSERT_TABLE =
        "insert into " + ENTITY_SECURITY_ATTR_TABLE + " (" + ALL_COLUMNS + ") VALUES" + ALL_VALUES;

    private static final String REPLACE_TABLE =
        "replace into " + ENTITY_SECURITY_ATTR_TABLE + " (" + ALL_COLUMNS + ") VALUES" + ALL_VALUES;

    private static final String SELECT_TABLE =
        "select * " + FROM_TABLE;

    private static final String DELETE_TABLE_BY_KEY_AND_TYPE =
        "delete " + FROM_TABLE + "where entity_key=? and entity_type=?";

    private static final String DELETE_TABLE_BY_USER =
        "delete " + FROM_TABLE + "where entity_key like ? and entity_type in ('USER_READ', 'USER_WRITE')";

    public List<LBACSecurityEntity> queryAll() {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_TABLE)) {
            ResultSet resultSet = statement.executeQuery();
            List<LBACSecurityEntity> list = new ArrayList<>();
            while (resultSet.next()) {
                try {
                    list.add(LBACAccessorUtils.loadESA(resultSet));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
            return list;
        } catch (Exception e) {
            LOGGER.error("Failed to query " + ENTITY_SECURITY_ATTR_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                ENTITY_SECURITY_ATTR_TABLE,
                e.getMessage());
        }
    }

    public int insert(LBACSecurityEntity a) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString,
                LBACAccessorUtils.buildEntityKey(a.getEntityKey(), a.getType()));
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, a.getType().name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, a.getSecurityAttr());
            return MetaDbUtil.insert(INSERT_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert " + ENTITY_SECURITY_ATTR_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                ENTITY_SECURITY_ATTR_TABLE,
                e.getMessage());
        }
    }

    public int replace(LBACSecurityEntity a) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString,
                LBACAccessorUtils.buildEntityKey(a.getEntityKey(), a.getType()));
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, a.getType().name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, a.getSecurityAttr());
            return MetaDbUtil.insert(REPLACE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to replace " + ENTITY_SECURITY_ATTR_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert",
                ENTITY_SECURITY_ATTR_TABLE,
                e.getMessage());
        }
    }

    public int deleteByKeyAndType(LBACSecurityEntity esa) {
        return deleteByKeyAndType(esa.getEntityKey(), esa.getType());
    }

    public int deleteByKeyAndType(LBACSecurityEntity.EntityKey key, LBACSecurityEntity.EntityType type) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString,
                LBACAccessorUtils.buildEntityKey(key, type));
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, type.name());
            return MetaDbUtil.delete(DELETE_TABLE_BY_KEY_AND_TYPE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to delete " + ENTITY_SECURITY_ATTR_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                ENTITY_SECURITY_ATTR_TABLE,
                e.getMessage());
        }
    }

}
