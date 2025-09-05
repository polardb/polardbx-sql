package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.cronutils.model.Cron;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.TddlConstants.COLUMNAR_AUTO_SNAPSHOT_CONFIG;
import static com.alibaba.polardbx.common.TddlConstants.CRON_EXPR;
import static com.alibaba.polardbx.common.TddlConstants.ZONE_ID;
import static com.cronutils.model.CronType.QUARTZ;

public class ColumnarAutoSnapshotConfigProcedure extends BaseInnerProcedure {
    private final String syntaxErrorMsg = "Bad arguments. Usage: "
        + "call polardbx.columnar_auto_snapshot_config('ENABLE', '0 0 * * * ?', '+08:00') "
        + "or call polardbx.columnar_auto_snapshot_config('SHOW') "
        + "or call polardbx.columnar_auto_snapshot_config('DISABLE')";

    private enum Action {
        SHOW, ENABLE, DISABLE;

        public static Action parse(String action) {
            if (SHOW.toString().equalsIgnoreCase(action)) {
                return SHOW;
            }
            if (ENABLE.toString().equalsIgnoreCase(action)) {
                return ENABLE;
            }
            if (DISABLE.toString().equalsIgnoreCase(action)) {
                return DISABLE;
            }
            return null;
        }
    }

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<SQLExpr> params = statement.getParameters();
        if (params.isEmpty()) {
            throw new RuntimeException(syntaxErrorMsg);
        }
        Action actionEnum = null;
        if (params.get(0) instanceof SQLIdentifierExpr) {
            actionEnum = Action.parse(params.get(0).toString());
        } else if (params.get(0) instanceof SQLCharExpr) {
            actionEnum = Action.parse(((SQLCharExpr) params.get(0)).getText());
        }
        if (null == actionEnum) {
            throw new RuntimeException(syntaxErrorMsg);
        }
        switch (actionEnum) {
        case SHOW:
            show(cursor);
            return;
        case ENABLE:
            enable(params, cursor);
            return;
        case DISABLE:
            disable(cursor);
            return;
        default:
            throw new RuntimeException(syntaxErrorMsg);
        }

    }

    private void disable(ArrayResultCursor cursor) {
        Map<String, String> beforeConfig = ExecUtils.getColumnarAutoSnapshotConfig();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
            accessor.setConnection(metaDbConn);
            accessor.deleteByTableIdAndKey(0, COLUMNAR_AUTO_SNAPSHOT_CONFIG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cursor.addColumn("result", DataTypes.StringType);
        cursor.addRow(new Object[] {"Disable auto generated snapshot."});
        if (null != beforeConfig) {
            cursor.addRow(new Object[] {
                "Before config: cron expression: " + beforeConfig.get(CRON_EXPR)
                    + " zone id: " + beforeConfig.get(ZONE_ID)});
        } else {
            cursor.addRow(new Object[] {"Before config: null"});
        }
    }

    private void enable(List<SQLExpr> params, ArrayResultCursor cursor) {
        if (params.size() != 3 || !(params.get(1) instanceof SQLCharExpr) || !(params.get(2) instanceof SQLCharExpr)) {
            throw new RuntimeException(syntaxErrorMsg);
        }
        String cronExpr = ((SQLCharExpr) params.get(1)).getText();
        String timeZone = ((SQLCharExpr) params.get(2)).getText();
        CronParser quartzCronParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        Cron cron = quartzCronParser.parse(cronExpr);
        ZoneId zoneId = TimeZoneUtils.zoneIdOf(timeZone);
        Map<String, String> config = new HashMap<>();
        config.put(CRON_EXPR, cron.asString());
        config.put(ZONE_ID, zoneId.getId());
        String configStr = JSON.toJSONString(config);
        Map<String, String> beforeConfig = ExecUtils.getColumnarAutoSnapshotConfig();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
            accessor.setConnection(metaDbConn);
            ColumnarConfigRecord record = new ColumnarConfigRecord();
            record.id = 0;
            record.configKey = COLUMNAR_AUTO_SNAPSHOT_CONFIG;
            record.configValue = configStr;
            accessor.insert(Collections.singletonList(record));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        cursor.addColumn("result", DataTypes.StringType);
        cursor.addRow(new Object[] {"Enable new auto generated snapshot."});
        if (null != beforeConfig) {
            cursor.addRow(new Object[] {
                "Before config: cron expression: " + beforeConfig.get(CRON_EXPR)
                    + " zone id: " + beforeConfig.get(ZONE_ID)});
        } else {
            cursor.addRow(new Object[] {"Before config: null"});
        }
        cursor.addRow(new Object[] {
            "Current config: cron expression: " + cron.asString() + " zone id: " + zoneId.getId()});
    }

    private void show(ArrayResultCursor cursor) {
        Map<String, String> config = ExecUtils.getColumnarAutoSnapshotConfig();
        cursor.addColumn("result", DataTypes.StringType);
        if (null == config) {
            cursor.addRow(new Object[] {"No config found."});
        } else {
            cursor.addRow(new Object[] {
                "Auto snapshot config: "
                    + "cron expression: " + config.get(CRON_EXPR) + " zone id: " + config.get(ZONE_ID)});
        }
    }
}
