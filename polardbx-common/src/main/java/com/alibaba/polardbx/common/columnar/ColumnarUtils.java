package com.alibaba.polardbx.common.columnar;

import com.alibaba.polardbx.common.cdc.CdcDDLContext;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_MARK_RECORD_COMMIT_TSO;
import static com.alibaba.polardbx.common.cdc.ICdcManager.DDL_ID;

public class ColumnarUtils {

    /**
     * 添加cdc打标事件,默认DdlType.IGNORED标记，后续cdc检测到该类打标，可以优化处理，不会改变元数据
     *
     * @param sql 打标sql
     * @return tso
     */
    public static Long AddCDCMarkEvent(String sql, String sqlKind) {
        Map<String, Object> params = new HashMap<>();
        params.put(CDC_MARK_RECORD_COMMIT_TSO, "true");
        //打标信息metadb会记录DDL_ID，但是没有应用到事件中
        params.put(DDL_ID, 1L);

        CdcDDLContext context = new CdcDDLContext("polardbx", "polardbx_function", sqlKind,
            sql, CdcDdlMarkVisibility.Protected, null, null,
            DdlType.IGNORED, true, params, null, false, null, null, null, null, false);
        CdcManagerHelper.getInstance().notifyDdlWithContext(context);

        return context.getCommitTso();
    }
}
