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
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.Collections;
import java.util.List;

public class MySqlOutFileExpr extends MySqlObjectImpl implements SQLExpr {

    private SQLExpr        file;
    private String         charset;

    private SQLExpr        columnsTerminatedBy;
    private boolean        columnsEnclosedOptionally = false;
    private SQLExpr columnsEnclosedBy;
    private SQLExpr columnsEscaped;

    private SQLExpr linesStartingBy;
    private SQLExpr linesTerminatedBy;

    private SQLExpr        ignoreLinesNumber;

    public MySqlOutFileExpr(){
    }

    public MySqlOutFileExpr(SQLExpr file){
        this.file = file;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, file);
        }
        visitor.endVisit(this);
    }

    @Override
    public List getChildren() {
        return Collections.singletonList(file);
    }

    public SQLExpr getFile() {
        return file;
    }

    public void setFile(SQLExpr file) {
        this.file = file;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public SQLExpr getColumnsTerminatedBy() {
        return columnsTerminatedBy;
    }

    public void setColumnsTerminatedBy(SQLExpr columnsTerminatedBy) {
        this.columnsTerminatedBy = columnsTerminatedBy;
    }

    public boolean isColumnsEnclosedOptionally() {
        return columnsEnclosedOptionally;
    }

    public void setColumnsEnclosedOptionally(boolean columnsEnclosedOptionally) {
        this.columnsEnclosedOptionally = columnsEnclosedOptionally;
    }

    public SQLExpr getColumnsEnclosedBy() {
        return columnsEnclosedBy;
    }

    public void setColumnsEnclosedBy(SQLExpr columnsEnclosedBy) {
        this.columnsEnclosedBy = columnsEnclosedBy;
    }

    public SQLExpr getColumnsEscaped() {
        return columnsEscaped;
    }

    public void setColumnsEscaped(SQLExpr columnsEscaped) {
        this.columnsEscaped = columnsEscaped;
    }

    public SQLExpr getLinesStartingBy() {
        return linesStartingBy;
    }

    public void setLinesStartingBy(SQLExpr linesStartingBy) {
        this.linesStartingBy = linesStartingBy;
    }

    public SQLExpr getLinesTerminatedBy() {
        return linesTerminatedBy;
    }

    public void setLinesTerminatedBy(SQLExpr linesTerminatedBy) {
        this.linesTerminatedBy = linesTerminatedBy;
    }

    public SQLExpr getIgnoreLinesNumber() {
        return ignoreLinesNumber;
    }

    public void setIgnoreLinesNumber(SQLExpr ignoreLinesNumber) {
        this.ignoreLinesNumber = ignoreLinesNumber;
    }

    public SQLExpr clone() {
        MySqlOutFileExpr x = new MySqlOutFileExpr();

        if (file != null) {
            x.setFile(file.clone());
        }

        x.charset = charset;

        if (columnsTerminatedBy != null) {
            x.setColumnsTerminatedBy(columnsTerminatedBy.clone());
        }

        x.columnsEnclosedOptionally = columnsEnclosedOptionally;

        if (columnsEnclosedBy != null) {
            x.setColumnsEnclosedBy(columnsEnclosedBy.clone());
        }

        if (columnsEscaped != null) {
            x.setColumnsEscaped(columnsEscaped.clone());
        }

        if (linesStartingBy != null) {
            x.setLinesStartingBy(linesStartingBy.clone());
        }

        if (linesTerminatedBy != null) {
            x.setLinesTerminatedBy(linesTerminatedBy.clone());
        }

        if (ignoreLinesNumber != null) {
            x.setIgnoreLinesNumber(ignoreLinesNumber.clone());
        }

        return x;
    }

}
