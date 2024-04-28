package com.alibaba.polardbx.server.encdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.common.encdb.enums.MsgType;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprVisitor;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.handler.EncdbDeleteRuleHandler;
import com.alibaba.polardbx.server.encdb.handler.EncdbImportRuleHandler;
import com.alibaba.polardbx.server.encdb.handler.EncdbMekProvisionHandler;
import com.alibaba.polardbx.server.encdb.handler.EncdbServerInfoGetHandler;
import com.alibaba.polardbx.server.handler.ExecuteHandler;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class EncdbMsgProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EncdbMsgProcessor.class);

    private static final EncdbServerInfoGetHandler SERVER_INFO_GET_HANDLER = new EncdbServerInfoGetHandler();
    private static final EncdbMekProvisionHandler MEK_PROVISION_HANDLER = new EncdbMekProvisionHandler();
    private static final EncdbImportRuleHandler IMPORT_RULE_HANDLER = new EncdbImportRuleHandler();
    private static final EncdbDeleteRuleHandler DELETE_RULE_HANDLER = new EncdbDeleteRuleHandler();

    public static String process(String json, ServerConnection serverConnection) {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(json);
        } catch (Exception e) {
            try {
                ExecutionContext newExecutionContext = new ExecutionContext(serverConnection.getSchema());
                newExecutionContext.setParams(new Parameters());
                newExecutionContext.setTraceId("encdb_msg");
                ExecutionPlan plan = Planner.getInstance().plan("select " + json, newExecutionContext);
                Cursor cursor = ExecutorHelper.execute(plan.getPlan(), newExecutionContext);
                String res = cursor.next().getString(0);
                jsonObject = JSON.parseObject(res);
            } catch (Exception e1) {
                throw new EncdbException("invalid encdb msg format");
            }
        }

        int requestType = jsonObject.getIntValue(MsgKeyConstants.REQUEST_TYPE);
        JSONObject result;
        try {
            JSONObject resBody;
            switch (requestType) {
            case MsgType.SERVER_INFO_GET:
                resBody = SERVER_INFO_GET_HANDLER.handle(jsonObject, serverConnection);
                break;
            case MsgType.MEK_PROVISION:
                resBody = MEK_PROVISION_HANDLER.handle(jsonObject, serverConnection);
                break;
            case MsgType.ENC_RULE_IMPORT:
                resBody = IMPORT_RULE_HANDLER.handle(jsonObject, serverConnection);
                break;
            case MsgType.ENC_RULE_DELETE:
                resBody = DELETE_RULE_HANDLER.handle(jsonObject, serverConnection);
                break;
            default:
                throw new UnsupportedOperationException();
            }
            result = success(resBody);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            result = fail(e);
        }
        return Base64.getEncoder().encodeToString(result.toString().getBytes(StandardCharsets.UTF_8));

    }

    public static JSONObject success(JSONObject body) {
        JSONObject result = new JSONObject();
        result.put(MsgKeyConstants.STATUS, 0);
        result.put(MsgKeyConstants.BODY, body.toString());
        return result;
    }

    public static JSONObject fail(Exception e) {
        JSONObject result = new JSONObject();
        result.put(MsgKeyConstants.STATUS, 1);
        result.put(MsgKeyConstants.BODY, e.getMessage());
        return result;
    }

    public static boolean checkUserPrivileges(ServerConnection serverConnection, boolean throwException) {
        PolarPrivileges polarPrivileges = (PolarPrivileges) serverConnection.getPrivileges();
        PolarAccountInfo polarUserInfo =
            polarPrivileges.checkAndGetMatchUser(serverConnection.getUser(), serverConnection.getHost());
        if (!polarUserInfo.getAccountType().isSuperUser()
            && polarUserInfo.getAccountType() != AccountType.SSO) {
            if (throwException) {
                throw new EncdbException("check privilege failed");
            } else {
                return false;
            }
        }
        return true;
    }

}
