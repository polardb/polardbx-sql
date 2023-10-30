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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.privilege.Host;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.CclUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateCclRule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2020/10/27 1:28 下午
 */
public class LogicalCreateCclRuleHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateCclRuleHandler.class);

    public LogicalCreateCclRuleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalCcl plan = (LogicalCcl) logicalPlan;
        SqlCreateCclRule sqlNode = (SqlCreateCclRule) plan.getSqlDal();
        String ruleName = sqlNode.getRuleName().getSimple();
        Connection outMetaDbConn = null;
        try (Connection metaDbConn =  MetaDbUtil.getConnection()) {
            outMetaDbConn = metaDbConn;
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);
            MetaDbUtil.beginTransaction(metaDbConn);
            if (sqlNode.isIfNotExists()) {
                //check if not exists
                List<CclRuleRecord> cclRuleRecords = cclRuleAccessor.queryByIds(ImmutableList.of(ruleName));
                if (CollectionUtils.isNotEmpty(cclRuleRecords)) {
                    if (!executionContext.getExtraDatas().containsKey(ExecutionContext.FAILED_MESSAGE)) {
                        executionContext.getExtraDatas()
                            .put(ExecutionContext.FAILED_MESSAGE, Lists.newArrayListWithCapacity(1));
                    }
                    List<ExecutionContext.ErrorMessage> errorMessages =
                        (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas()
                            .get(ExecutionContext.FAILED_MESSAGE);
                    errorMessages.add(new ExecutionContext.ErrorMessage(ErrorCode.ERR_CCL.getCode(), null,
                        "The ccl rule has been existing"));
                    return new AffectRowCursor(new int[] {0});
                }
            }
            CclRuleRecord cclRuleRecord = new CclRuleRecord();
            cclRuleRecord.id = sqlNode.getRuleName().getSimple();
            cclRuleRecord.sqlType = sqlNode.getFOR().getSimple();
            cclRuleRecord.dbName = sqlNode.getDbName().getSimple();
            cclRuleRecord.tableName = sqlNode.getTableName().getSimple();
            cclRuleRecord.userName = null;
            cclRuleRecord.clientIp = "%";
            if (sqlNode.getUserName() != null) {
                cclRuleRecord.userName = sqlNode.getUserName().getUser();
                cclRuleRecord.clientIp = sqlNode.getUserName().getHost();
            }

            SqlNode query = sqlNode.getQuery();
            if (query != null) {
                if (StringUtils.isEmpty(cclRuleRecord.dbName)) {
                    throw new TddlNestableRuntimeException(
                        " Failed to create CCL_RULE, dbName should be provided when filter by query.");
                }
                String sql = ((SqlCharStringLiteral) query).toValue();
                SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
                Map<Integer, Object> posParamValueMap = CclUtils.getPosParamValueMap(sqlParameterized);
                String json = JSON.toJSONString(posParamValueMap);
                cclRuleRecord.query = sql;
                cclRuleRecord.params = json;
                int templateId = sqlParameterized.getSql().hashCode();
                cclRuleRecord.queryTemplateId = TStringUtil.int2FixedLenHexStr(templateId);
            }

            if (!Host.verify(cclRuleRecord.clientIp)) {
                logger.error("Invalid host. Failed to create ccl rule name " + ruleName);
                throw new TddlNestableRuntimeException(" Failed to create CCL_RULE");
            }

            cclRuleRecord.parallelism = CclRuleRecord.DEFAULT_PARALLELISM;
            cclRuleRecord.queueSize = CclRuleRecord.DEFAULT_QUEUE_SIZE;
            cclRuleRecord.waitTimeout = CclRuleRecord.DEFAULT_WAIT_TIMEOUT;
            cclRuleRecord.fastMatch = CclRuleRecord.DEFAULT_FAST_MATCH;
            cclRuleRecord.triggerPriority = CclRuleRecord.DEFAULT_TRIGGER_PRIORITY;
            cclRuleRecord.lightWait = CclRuleRecord.DEFAULT_LIGHT_WAIT;

            //convert keywords to json string
            SqlNodeList keywords = sqlNode.getKeywords();
            if (keywords != null) {
                List<String> keywordList = Lists.newArrayListWithCapacity(keywords.size());
                for (int i = 0; i < keywords.size(); ++i) {
                    String keyword = ((SqlCharStringLiteral) keywords.get(i)).toValue();
                    keyword = StringUtils.strip(keyword, "`");
                    keywordList.add(keyword);
                }
                String keywordJson = JSON.toJSONString(keywordList);
                cclRuleRecord.keywords = keywordJson;
            }

            List<Pair<SqlNode, SqlNode>> with = sqlNode.getWith();
            boolean hasMaxConcurrencyOption = false;
            for (Pair<SqlNode, SqlNode> pair : with) {
                String variableName = ((SqlIdentifier) pair.left).getSimple();
                int value = ((SqlNumericLiteral) pair.right).intValue(true);
                switch (variableName.toUpperCase()) {
                case "MAX_CONCURRENCY":
                    cclRuleRecord.parallelism = value;
                    hasMaxConcurrencyOption = true;
                    break;
                case "WAIT_QUEUE_SIZE":
                    cclRuleRecord.queueSize = value;
                    break;
                case "WAIT_TIMEOUT":
                    cclRuleRecord.waitTimeout = value;
                    break;
                case "FAST_MATCH":
                    cclRuleRecord.fastMatch = value;
                    break;
                case "LIGHT_WAIT":
                    cclRuleRecord.lightWait = value;
                    break;
                default:
                    throw new TddlNestableRuntimeException("Invalid WITH option of " + variableName);
                }
            }
            if (!hasMaxConcurrencyOption) {
                throw new TddlNestableRuntimeException("with MAX_CONCURRENCY option must be provided.");
            }
            if (cclRuleRecord.queueSize < 0 || cclRuleRecord.parallelism < 0 || cclRuleRecord.waitTimeout < 0
                || cclRuleRecord.fastMatch < 0 || cclRuleRecord.lightWait < 0) {
                throw new TddlNestableRuntimeException("Invalid number of WAIT_QUEUE_SIZE or parallelism or ");
            }

            SqlNodeList templateId = sqlNode.getTemplateId();
            if (templateId != null) {
                List<String> templateIdList = templateId.getList().stream().map((e) -> {
                    return ((SqlCharStringLiteral) e).toValue();
                }).collect(Collectors.toList());
                String templateIdStr = StringUtils.join(templateIdList, ",");
                cclRuleRecord.templateId = templateIdStr;
            }

            cclRuleAccessor.insert(cclRuleRecord);
            cclRuleAccessor.flush();
            logger.info("create ccl rule " + cclRuleRecord.toString());
            String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
            metaDbConfigManager.notify(dataId, metaDbConn);

            MetaDbUtil.commit(metaDbConn);

            metaDbConfigManager.sync(dataId);
            return new AffectRowCursor(new int[] {1});
        } catch (Exception e) {
            if (outMetaDbConn != null) {
                try {
                    outMetaDbConn.rollback();
                } catch (Throwable throwable) {
                    logger.error("Failed to rollback ", throwable);
                }
            }
            logger.error("Failed to create ccl rule name " + ruleName);
            String message = e.getMessage();
            if (message != null && message.contains("Duplicate")) {
                message = "Failed to create CCL_RULE. Duplicated ccl rule name " + ruleName;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CCL, message, e);
        }
    }

}
