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
package com.alibaba.polardbx.druid.sql.visitor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;

import java.util.ArrayList;
import java.util.List;

public final class ExportParameterVisitorUtils {

    //private for util class not need new instance
    private ExportParameterVisitorUtils() {
        super();
    }

    public static ExportParameterVisitor createExportParameterVisitor(Appendable out, DbType dbType) {
        return new MySqlExportParameterVisitor(out);
    }

    public static boolean exportParamterAndAccept(final List<Object> parameters, List<SQLExpr> list) {
        for (int i = 0, size = list.size(); i < size; ++i) {
            SQLExpr param = list.get(i);

            SQLExpr result = exportParameter(parameters, param);
            if (result != param) {
                list.set(i, result);
            }
        }

        return false;
    }

    public static SQLExpr exportParameter(final List<Object> parameters, final SQLExpr param) {
        Object value = null;
        boolean replace = false;

        if (param instanceof SQLCharExpr) {
            if (param instanceof MySqlCharExpr && ((MySqlCharExpr) param).getBinary() != null) {
                value = ((MySqlCharExpr) param).getBinary();
            } else {
                value = ((SQLCharExpr) param).getText();
            }
            replace = true;
        } else if (param instanceof SQLNCharExpr) {
            value = ((SQLNCharExpr) param).getText();
            replace = true;
        } else if (param instanceof SQLBooleanExpr) {
            value = ((SQLBooleanExpr) param).getBooleanValue();
            replace = true;
        } else if (param instanceof SQLNumericLiteralExpr) {
            value = ((SQLNumericLiteralExpr) param).getNumber();
            replace = true;
        } else if (param instanceof SQLHexExpr) {
            value = ((SQLHexExpr) param).toBytes();
            replace = true;
        } else if (param instanceof SQLBinaryExpr) {
            value = ((SQLBinaryExpr) param).getValue();
            replace = true;
        } else if (param instanceof SQLTimestampExpr) {
            value = ((SQLTimestampExpr) param).getValue();
            replace = true;
        } else if (param instanceof SQLDateExpr) {
            value = ((SQLDateExpr) param).getValue();
            replace = true;
        } else if (param instanceof SQLTimeExpr) {
            value = ((SQLTimeExpr) param).getValue();
            replace = true;
        } else if (param instanceof SQLListExpr) {
            SQLListExpr list = ((SQLListExpr) param);

            List<Object> listValues = new ArrayList<>();
            for (SQLExpr listItem : list.getItems()) {
                if (listItem instanceof SQLCharExpr) {
                    if (listItem instanceof MySqlCharExpr && ((MySqlCharExpr) listItem).getBinary() != null) {
                        listValues.add(((MySqlCharExpr) listItem).getBinary());
                    } else {
                        listValues.add(((SQLCharExpr) listItem).getText());
                    }
                } else if (listItem instanceof SQLNCharExpr) {
                    listValues.add(((SQLNCharExpr) listItem).getText());
                } else if (listItem instanceof SQLBooleanExpr) {
                    listValues.add(((SQLBooleanExpr) listItem).getBooleanValue());
                } else if (listItem instanceof SQLNumericLiteralExpr) {
                    listValues.add(((SQLNumericLiteralExpr) listItem).getNumber());
                } else if (listItem instanceof SQLHexExpr) {
                    listValues.add(((SQLHexExpr) listItem).toBytes());
                } else if (listItem instanceof SQLBinaryExpr) {
                    listValues.add(((SQLBinaryExpr) listItem).getValue());
                } else if (listItem instanceof SQLTimestampExpr) {
                    listValues.add(((SQLTimestampExpr) listItem).getValue());
                } else if (listItem instanceof SQLDateExpr) {
                    listValues.add(((SQLDateExpr) listItem).getValue());
                } else if (listItem instanceof SQLTimeExpr) {
                    listValues.add(((SQLTimeExpr) listItem).getValue());
                } else if (listItem instanceof SQLNullExpr) {
                    listValues.add(null);
                }
            }

            if (listValues.size() == list.getItems().size()) {
                value = listValues;
                replace = true;
            }
        } else if (param instanceof SQLNullExpr) {
            value = null;
            replace = true;
        }

        if (replace) {
            SQLObject parent = param.getParent();
            if (parent != null) {
                List<SQLObject> mergedList = null;
                if (parent instanceof SQLBinaryOpExpr) {
                    mergedList = ((SQLBinaryOpExpr) parent).getMergedList();
                }
                if (mergedList != null) {
                    List<Object> mergedListParams = new ArrayList<Object>(mergedList.size() + 1);
                    for (int i = 0; i < mergedList.size(); ++i) {
                        SQLObject item = mergedList.get(i);
                        if (item instanceof SQLBinaryOpExpr) {
                            SQLBinaryOpExpr binaryOpItem = (SQLBinaryOpExpr) item;
                            exportParameter(mergedListParams, binaryOpItem.getRight());
                        }
                    }
                    if (mergedListParams.size() > 0) {
                        mergedListParams.add(0, value);
                        value = mergedListParams;
                    }
                }
            }

            if (parameters != null) {
                parameters.add(value);
            }

            return new SQLVariantRefExpr("?");
        }

        return param;
    }

    public static void exportParameter(final List<Object> parameters, SQLBinaryOpExpr x) {
        if (x.getLeft() instanceof SQLLiteralExpr
            && x.getRight() instanceof SQLLiteralExpr
            && x.getOperator().isRelational()) {
            return;
        }

        {
            SQLExpr leftResult = ExportParameterVisitorUtils.exportParameter(parameters, x.getLeft());
            if (leftResult != x.getLeft()) {
                x.setLeft(leftResult);
            }
        }

        {
            SQLExpr rightResult = exportParameter(parameters, x.getRight());
            if (rightResult != x.getRight()) {
                x.setRight(rightResult);
            }
        }
    }

    public static void exportParameter(final List<Object> parameters, SQLBetweenExpr x) {
        {
            SQLExpr result = exportParameter(parameters, x.getBeginExpr());
            if (result != x.getBeginExpr()) {
                x.setBeginExpr(result);
            }
        }

        {
            SQLExpr result = exportParameter(parameters, x.getEndExpr());
            if (result != x.getBeginExpr()) {
                x.setEndExpr(result);
            }
        }

    }
}
