package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlEngineResources;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlScheduler;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowDdlEngineStatusHandler;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaDdlEngineResourceHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaDdlEngineResourceHandler.class);

    public InformationSchemaDdlEngineResourceHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlEngineResources;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String schemaName = executionContext.getSchemaName();
        return DdlEngineShowDdlEngineStatusHandler.handle(executionContext, schemaName, false, true);
    }
}


