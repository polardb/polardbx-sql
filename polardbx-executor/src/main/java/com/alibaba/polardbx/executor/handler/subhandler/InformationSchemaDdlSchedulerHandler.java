package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlScheduler;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFullTableGroup;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableGroup;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowDdlEngineStatusHandler;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap.DDL_DAG_EXECUTOR_MAP;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaDdlSchedulerHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaDdlSchedulerHandler.class);

    public InformationSchemaDdlSchedulerHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlScheduler;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String schemaName = executionContext.getSchemaName();
        return DdlEngineShowDdlEngineStatusHandler.handle(executionContext, schemaName, false, false);
    }
}


