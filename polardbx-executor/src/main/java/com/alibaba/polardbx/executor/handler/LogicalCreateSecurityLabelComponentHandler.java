package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.accessor.LBACAccessorUtils;
import com.alibaba.polardbx.gms.lbac.accessor.LBACComponentAccessor;
import com.alibaba.polardbx.gms.lbac.component.ComponentType;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSecurityLabelComponent;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author pangzhaoxing
 */
public class LogicalCreateSecurityLabelComponentHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateSecurityLabelComponentHandler.class);

    public LogicalCreateSecurityLabelComponentHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlCreateSecurityLabelComponent createComponent =
            (SqlCreateSecurityLabelComponent) ((LogicalDal) logicalPlan).getNativeSqlNode();

        String componentName = createComponent.getComponentName().getSimple().toLowerCase();//全部小写
        String componentContent = createComponent.getComponentContent().getNlsString().getValue().toLowerCase();//全部小写
        ComponentType componentType =
            ComponentType.valueOf(createComponent.getComponentType().getSimple().toUpperCase());//全部小写
        LBACSecurityLabelComponent
            component = LBACAccessorUtils.createSecurityLabelComponent(componentName, componentType, componentContent);
        if (!LBACSecurityManager.getInstance().validateComponent(component)) {
            throw new LBACException("security label component is invalid");
        }

        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            LBACComponentAccessor slcAccessor = new LBACComponentAccessor();
            slcAccessor.setConnection(connection);
            int affectRow = slcAccessor.insert(component);

            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
            return new AffectRowCursor(affectRow);
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
