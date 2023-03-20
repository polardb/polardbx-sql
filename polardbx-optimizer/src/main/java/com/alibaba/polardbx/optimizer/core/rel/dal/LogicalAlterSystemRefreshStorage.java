package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlAlterSystemRefreshStorage;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import java.util.List;

public class LogicalAlterSystemRefreshStorage extends LogicalDal {

    protected String dnId;
    protected String vipAddr;
    protected String userName;
    protected String passwdEnc;

    public LogicalAlterSystemRefreshStorage(Dal dal) {
        super(dal, "", "", null);
        initPlanByDalAst((SqlAlterSystemRefreshStorage) dal.getAst());
    }

    protected void initPlanByDalAst(SqlAlterSystemRefreshStorage refreshStorage) {
        SqlNode targetDnAst = refreshStorage.getTargetStorage();
        String dnIdStr = targetDnAst.toString();
        String dnIdVal = SQLUtils.normalizeNoTrim(dnIdStr);
        this.dnId = dnIdVal;
        List<Pair<SqlNode, SqlNode>> kvPairs = refreshStorage.getAssignItems();
        for (int i = 0; i < kvPairs.size(); i++) {
            Pair<SqlNode, SqlNode> kv = kvPairs.get(i);
            SqlNode key = kv.getKey();
            SqlNode val = kv.getValue();
            String keyStr = key == null ? "" : SQLUtils.normalizeNoTrim(key.toString());
            String valStr = val == null ? "" : SQLUtils.normalizeNoTrim(val.toString());
            if (keyStr.equalsIgnoreCase("vip_port")) {
                this.vipAddr = valStr;
            } else if (keyStr.equalsIgnoreCase("user")) {
                this.userName = valStr;
            } else if (keyStr.equalsIgnoreCase("passwd_enc")) {
                this.passwdEnc = valStr;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Not allowed to set the property of %s by using refresh storage set", key));
            }
        }
    }

    public SqlDal getSqlDal() {
        return (SqlDal) getNativeSqlNode();
    }

    @Override
    protected String getExplainName() {
        return "LogicalAlterSystemRefreshStorage";
    }

    public static LogicalDal create(Dal dal) {
        LogicalAlterSystemRefreshStorage newRefreshStorage = new LogicalAlterSystemRefreshStorage(dal);
        return newRefreshStorage;
    }

    @Override
    public LogicalAlterSystemRefreshStorage copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        LogicalAlterSystemRefreshStorage newRefreshStorage =
            (LogicalAlterSystemRefreshStorage) LogicalAlterSystemRefreshStorage.create(this.dal);
        newRefreshStorage.setDnId(this.dnId);
        newRefreshStorage.setVipAddr(this.vipAddr);
        newRefreshStorage.setUserName(this.userName);
        newRefreshStorage.setPasswdEnc(this.passwdEnc);
        return newRefreshStorage;
    }

    public String getDnId() {
        return dnId;
    }

    public void setDnId(String dnId) {
        this.dnId = dnId;
    }

    public String getVipAddr() {
        return vipAddr;
    }

    public void setVipAddr(String vipAddr) {
        this.vipAddr = vipAddr;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPasswdEnc() {
        return passwdEnc;
    }

    public void setPasswdEnc(String passwdEnc) {
        this.passwdEnc = passwdEnc;
    }
}
