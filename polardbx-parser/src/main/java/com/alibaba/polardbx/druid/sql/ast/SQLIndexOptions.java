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

package com.alibaba.polardbx.druid.sql.ast;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class SQLIndexOptions extends SQLObjectImpl {

    private String indexType; // Using btree/hash
    private SQLExpr keyBlockSize;
    private String parserName;
    private SQLExpr comment;
    private String algorithm;
    private String lock;
    private String dictionaryColumns;
    private String execComment;
    private List<SQLAssignItem> otherOptions = new ArrayList<SQLAssignItem>();

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public SQLExpr getKeyBlockSize() {
        return keyBlockSize;
    }

    public void setKeyBlockSize(SQLExpr keyBlockSize) {
        if (keyBlockSize != null) {
            if (getParent() != null && getParent().getParent() != null) {
                keyBlockSize.setParent(getParent().getParent());
            } else {
                keyBlockSize.setParent(this);
            }
        }
        this.keyBlockSize = keyBlockSize;

        // Old parser code put it in options list.
        if (keyBlockSize != null && getParent() != null && getParent() instanceof SQLIndexDefinition) {
            SQLIndexDefinition parent = (SQLIndexDefinition) getParent();
            SQLAssignItem assignItem = new SQLAssignItem(new SQLIdentifierExpr("KEY_BLOCK_SIZE"), keyBlockSize);
            if (getParent() != null && getParent().getParent() != null) {
                assignItem.setParent(getParent().getParent());
            } else {
                assignItem.setParent(this);
            }
            parent.getCompatibleOptions().add(assignItem);
        }
    }

    public String getParserName() {
        return parserName;
    }

    public void setParserName(String parserName) {
        this.parserName = parserName;
    }

    public SQLExpr getComment() {
        return comment;
    }

    public void setComment(SQLExpr comment) {
        if (comment != null) {
            if (getParent() != null && getParent().getParent() != null) {
                comment.setParent(getParent().getParent());
            } else {
                comment.setParent(this);
            }
        }
        this.comment = comment;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;

        // Old parser code put it in options list.
        if (algorithm != null && getParent() != null && getParent() instanceof SQLIndexDefinition) {
            SQLIndexDefinition parent = (SQLIndexDefinition) getParent();
            SQLAssignItem assignItem =
                new SQLAssignItem(new SQLIdentifierExpr("ALGORITHM"), new SQLIdentifierExpr(algorithm));
            if (getParent() != null && getParent().getParent() != null) {
                assignItem.setParent(getParent().getParent());
            } else {
                assignItem.setParent(this);
            }
            parent.getCompatibleOptions().add(assignItem);
        }
    }

    public String getLock() {
        return lock;
    }

    public void setLock(String lock) {
        this.lock = lock;

        // Old parser code put it in options list.
        if (lock != null && getParent() != null && getParent() instanceof SQLIndexDefinition) {
            SQLIndexDefinition parent = (SQLIndexDefinition) getParent();
            SQLAssignItem assignItem = new SQLAssignItem(new SQLIdentifierExpr("LOCK"), new SQLIdentifierExpr(lock));
            if (getParent() != null && getParent().getParent() != null) {
                assignItem.setParent(getParent().getParent());
            } else {
                assignItem.setParent(this);
            }
            parent.getCompatibleOptions().add(assignItem);
        }
    }

    public String getDictionaryColumns() {
        return dictionaryColumns;
    }

    public void setDictionaryColumns(String dictionaryColumns) {
        this.dictionaryColumns = dictionaryColumns;
    }

    public List<SQLAssignItem> getOtherOptions() {
        return otherOptions;
    }

    public String getExecComment() {
        return execComment;
    }

    public void setExecComment(String execComment) {
        this.execComment = execComment;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public void cloneTo(SQLIndexOptions options) {
        SQLObject parent;
        if (options.getParent() != null && options.getParent().getParent() != null) {
            parent = options.getParent().getParent();
        } else {
            parent = options;
        }
        options.indexType = indexType;
        if (keyBlockSize != null) {
            options.keyBlockSize = keyBlockSize.clone();
            options.keyBlockSize.setParent(parent);
        }
        options.parserName = parserName;
        if (comment != null) {
            options.comment = comment.clone();
            options.comment.setParent(parent);
        }
        options.algorithm = algorithm;
        options.lock = lock;
        options.dictionaryColumns = dictionaryColumns;
        for (SQLAssignItem item : otherOptions) {
            SQLAssignItem item1 = item.clone();
            item1.setParent(parent);
            options.otherOptions.add(item1);
        }
        options.execComment = execComment;
    }
}
