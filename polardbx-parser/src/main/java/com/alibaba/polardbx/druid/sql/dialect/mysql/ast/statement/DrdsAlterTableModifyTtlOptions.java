/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author chenghui.lch
 */
public class DrdsAlterTableModifyTtlOptions extends MySqlObjectImpl implements SQLAlterTableItem {

    protected SQLExpr ttlEnable;
    protected SQLExpr ttlExpr;
    protected SQLExpr ttlJob;
    protected SQLExpr ttlFilter;
    protected SQLExpr ttlCleanup;
    protected SQLExpr ttlPartInterval;

    protected SQLExpr archiveTableSchema;
    protected SQLExpr archiveTableName;
    protected SQLExpr archiveKind;

    protected SQLExpr arcPreAllocate;
    protected SQLExpr arcPostAllocate;

//    protected SQLExpr ttlArchiveCci;
//    protected SQLExpr archiveView;

    public DrdsAlterTableModifyTtlOptions() {
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.ttlEnable != null) {
                this.ttlEnable.accept(visitor);
            }

            if (this.ttlExpr != null) {
                this.ttlExpr.accept(visitor);
            }

            if (this.ttlJob != null) {
                this.ttlJob.accept(visitor);
            }

            if (this.ttlFilter != null) {
                this.ttlFilter.accept(visitor);
            }

            if (this.ttlCleanup != null) {
                this.ttlCleanup.accept(visitor);
            }

            if (this.ttlPartInterval != null) {
                this.ttlPartInterval.accept(visitor);
            }

            if (this.archiveKind != null) {
                this.archiveKind.accept(visitor);
            }

            if (this.archiveTableSchema != null) {
                this.archiveTableSchema.accept(visitor);
            }

            if (this.archiveTableName != null) {
                this.archiveTableName.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTtlEnable() {
        return ttlEnable;
    }

    public void setTtlEnable(SQLExpr ttlEnable) {
        this.ttlEnable = ttlEnable;
    }

    public SQLExpr getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SQLExpr ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SQLExpr getTtlJob() {
        return ttlJob;
    }

    public void setTtlJob(SQLExpr ttlJob) {
        this.ttlJob = ttlJob;
    }

    public SQLExpr getArchiveTableSchema() {
        return archiveTableSchema;
    }

    public void setArchiveTableSchema(SQLExpr archiveTableSchema) {
        this.archiveTableSchema = archiveTableSchema;
    }

    public SQLExpr getArchiveTableName() {
        return archiveTableName;
    }

    public void setArchiveTableName(SQLExpr archiveTableName) {
        this.archiveTableName = archiveTableName;
    }

    public SQLExpr getArchiveKind() {
        return archiveKind;
    }

    public void setArchiveKind(SQLExpr archiveKind) {
        this.archiveKind = archiveKind;
    }

    public SQLExpr getArcPreAllocate() {
        return arcPreAllocate;
    }

    public void setArcPreAllocate(SQLExpr arcPreAllocate) {
        this.arcPreAllocate = arcPreAllocate;
    }

    public SQLExpr getArcPostAllocate() {
        return arcPostAllocate;
    }

    public void setArcPostAllocate(SQLExpr arcPostAllocate) {
        this.arcPostAllocate = arcPostAllocate;
    }

    public SQLExpr getTtlFilter() {
        return ttlFilter;
    }

    public void setTtlFilter(SQLExpr ttlFilter) {
        this.ttlFilter = ttlFilter;
    }

    public SQLExpr getTtlCleanup() {
        return ttlCleanup;
    }

    public void setTtlCleanup(SQLExpr ttlCleanup) {
        this.ttlCleanup = ttlCleanup;
    }

    public SQLExpr getTtlPartInterval() {
        return ttlPartInterval;
    }

    public void setTtlPartInterval(SQLExpr ttlPartInterval) {
        this.ttlPartInterval = ttlPartInterval;
    }
}
