package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.StandardToEnterpriseEditionUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportSequence;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlImportSequence;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalImportSequenceHandler extends HandlerCommon {
    public LogicalImportSequenceHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalImportSequence importSequence = (LogicalImportSequence) logicalPlan;
        final SqlImportSequence sqlImportSequence = (SqlImportSequence) importSequence.getNativeSqlNode();

        final String logicalDatabase = SQLUtils.normalize(sqlImportSequence.getLogicalDb());

        Map<String, String> result = handleImportSequenceInSchema(logicalDatabase, executionContext);

        return buildResult(result);
    }

    protected Map<String, String> handleImportSequenceInSchema(String logicalDatabase,
                                                               ExecutionContext executionContext) {
        Set<String> allLogicalTableWhichHasSeq =
            StandardToEnterpriseEditionUtil.queryTableWhichHasSequence(logicalDatabase, executionContext);

        String phyDatabase = StandardToEnterpriseEditionUtil.queryPhyDbNameByLogicalDbName(logicalDatabase);
        Map<String, BigInteger> sequences =
            StandardToEnterpriseEditionUtil.querySequenceValuesInPhysicalDatabase(phyDatabase, logicalDatabase);

        Map<String, String> result = new TreeMap<>(String::compareToIgnoreCase);

        for (String logicalTb : allLogicalTableWhichHasSeq) {
            if (!sequences.containsKey(logicalTb)) {
                result.put(logicalTb, "only contain logical table");
            }
        }

        Iterator<Map.Entry<String, BigInteger>> iter = sequences.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, BigInteger> entry = iter.next();
            String phyTb = entry.getKey();
            if (!allLogicalTableWhichHasSeq.contains(phyTb)) {
                result.put(phyTb, "only contain physical table");
                iter.remove();
            }
        }

        StandardToEnterpriseEditionUtil.updateSequence(sequences, logicalDatabase, result);

        return result;
    }

    Cursor buildResult(Map<String, String> resultMap) {
        ArrayResultCursor result = new ArrayResultCursor("Result");
        if (!resultMap.isEmpty()) {
            result.addColumn("table_name", DataTypes.StringType);
            result.addColumn("state", DataTypes.StringType);
            result.addColumn("err_msg", DataTypes.StringType);

            for (Map.Entry<String, String> entry : resultMap.entrySet()) {
                result.addRow(
                    new Object[] {
                        entry.getKey(),
                        "fail",
                        entry.getValue()
                    }
                );
            }
        } else {
            result.addColumn("table_name", DataTypes.StringType);
            result.addRow(new Object[] {"ALL SUCCESS"});
        }

        return result;
    }

}
