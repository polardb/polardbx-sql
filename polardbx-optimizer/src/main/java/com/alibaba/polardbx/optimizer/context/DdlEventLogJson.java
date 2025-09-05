package com.alibaba.polardbx.optimizer.context;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.TStringUtil;
import lombok.Getter;
import lombok.Setter;

/**
 * @author wumu
 */
@Getter
@Setter
public class DdlEventLogJson {
    public long jobId;

    public String schemaName;

    public String objectName;

    public String state;

    public String type;

    public String ddlStmt;

    public String jobFactoryName;

    public String taskName;

    public String errorMessage;

    public static DdlEventLogJson create(DdlContext ddlContext) {
        DdlEventLogJson ddlEventLogJson = new DdlEventLogJson();
        ddlEventLogJson.jobId = ddlContext.getJobId();
        ddlEventLogJson.schemaName = ddlContext.getSchemaName();
        ddlEventLogJson.objectName = ddlContext.getObjectName();
        ddlEventLogJson.state = ddlContext.getState().name();
        ddlEventLogJson.type = ddlContext.getDdlType().name();
        ddlEventLogJson.jobFactoryName = ddlContext.getDdlJobFactoryName();
        ddlEventLogJson.ddlStmt = TStringUtil.quoteString(ddlContext.getDdlStmt());
        return ddlEventLogJson;
    }

    public static DdlEventLogJson create(DdlContext ddlContext, String taskName, String errorMessage) {
        DdlEventLogJson ddlEventLogJson = create(ddlContext);
        ddlEventLogJson.taskName = taskName;
        ddlEventLogJson.errorMessage = errorMessage;
        return ddlEventLogJson;
    }

    public static DdlEventLogJson fromJson(String json) {
        return JSON.parseObject(json, DdlEventLogJson.class);
    }

    public static String toJson(DdlEventLogJson obj) {
        if (obj == null) {
            return "";
        }
        return JSON.toJSONString(obj);
    }
}
