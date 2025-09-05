package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;

/**
 * @author chenghui.lch
 */
public class CalcNewPartBoundsByPivotBoundValWithTblMetaParams {

    protected CalcNewPartBoundsByPivotBoundValParams calcParams;
    protected TableMeta tarTblMeta;
    protected PartKeyLevel tarPartKeyLevel;

    public CalcNewPartBoundsByPivotBoundValWithTblMetaParams() {
    }

    public CalcNewPartBoundsByPivotBoundValParams getCalcParams() {
        return calcParams;
    }

    public void setCalcParams(CalcNewPartBoundsByPivotBoundValParams calcParams) {
        this.calcParams = calcParams;
    }

    public TableMeta getTarTblMeta() {
        return tarTblMeta;
    }

    public void setTarTblMeta(TableMeta tarTblMeta) {
        this.tarTblMeta = tarTblMeta;
    }

    public PartKeyLevel getTarPartKeyLevel() {
        return tarPartKeyLevel;
    }

    public void setTarPartKeyLevel(PartKeyLevel tarPartKeyLevel) {
        this.tarPartKeyLevel = tarPartKeyLevel;
    }
}
