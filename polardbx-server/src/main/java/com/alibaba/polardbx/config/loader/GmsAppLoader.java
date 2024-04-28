package com.alibaba.polardbx.config.loader;

import com.alibaba.polardbx.PolarQuarantineManager;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author chenghui.lch
 */
public class GmsAppLoader extends AppLoader {

    public GmsAppLoader(String cluster, String unitName) {
        super(cluster, unitName);
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    public synchronized void initDbUserPrivsInfo(String instanceId) {
        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }

        List<String> dbs = new ArrayList<>(PolarPrivManager.getInstance().getAllDbs());
        // 触发一次装载
        this.loadApps(dbs);
    }

    /**
     * 装载app
     */
    @Override
    public synchronized void loadApp(final String dbName) {
        try {

            Map<String, String> dbNameAndAppNameInfo = PolarPrivManager.getInstance().getAllDbNameAndAppNameMap();
            String appName = dbNameAndAppNameInfo.get(dbName);
            if (appName == null) {
                throw new TddlNestableRuntimeException("Unknown database " + dbName);
            }

            logger.info("start loading app:" + dbName + " , appName:" + appName);
            this.loadSchema(dbName, appName);
            this.loadMdlManager(dbName);
            logger.info("finish loading app:" + dbName + " , appName:" + appName);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * 卸载app
     */
    @Override
    protected synchronized void unLoadApp(final String app) {
        try {
            logger.info("start unLoading app:" + app);
            this.unLoadSchema(app, app);
            this.loadMdlManager(app);
            logger.info("finish unLoading app:" + app);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    protected synchronized void unLoadSchema(final String dbName, final String appName) {
        super.unLoadSchema(dbName, appName);
        if (ConfigDataMode.isPolarDbX()) {
            // clean the config for db to be removed
            String dataId = MetaDbDataIdBuilder.getDbTopologyDataId(dbName);
            MetaDbConfigManager.getInstance().unbindListener(dataId);
            DbGroupInfoManager.getInstance().unbindListenersForAllGroupsOfDb(dbName);

            String scaleOutDataId = MetaDbDataIdBuilder.getDbComplexTaskDataId(dbName);
            MetaDbConfigManager.getInstance().unbindListener(scaleOutDataId);
        }
    }
}
