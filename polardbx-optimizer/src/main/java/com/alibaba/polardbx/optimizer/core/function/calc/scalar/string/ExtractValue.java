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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.util.List;

public class ExtractValue extends AbstractScalarFunction {
    public ExtractValue(List<DataType> operandTypes,
                           DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"EXTRACTVALUE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object argv : args) {
            if (FunctionUtils.isNull(argv)) {
                return null;
            }
        }

        String xmlFlag = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String xPathExpr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);

        StringBuilder result = new StringBuilder();
        try {
            if (xPathExpr.isEmpty()) {
                throw new XPathExpressionException("''"); //align to MySQL
            }
            if (xmlFlag.isEmpty()) {
                result = new StringBuilder("");
                return result;
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xmlFlag)));
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            //假设有标签<b:c> MySQL中可以直接用//b:c这种格式匹配，但是XPath中需要指定local-name
            xPathExpr = xPathExpr.replaceAll("(\\w+):(\\w+)", "*[local-name()='$2']");
            //count可以计算满足条件的Node个数
            if (xPathExpr.contains("[") || xPathExpr.contains("]")) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "ExtractValue index visit is not support");
            } else if (xPathExpr.contains("$@")) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "ExtractValue variable visit is not support");
            }
            XPathExpression expr = xpath.compile(xPathExpr);
            if (xPathExpr.contains("count(")) {
                Long count = ((Double) expr.evaluate(doc, XPathConstants.NUMBER)).longValue();
                result = new StringBuilder(String.valueOf(count));
            } else {
                NodeList nodeList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node node = nodeList.item(i);
                    Node curChild = node.getFirstChild();
                    //获取TEXT_NODE的值作为答案
                    while (curChild != null) {
                        if (curChild.getNodeType() == Node.TEXT_NODE) {
                            result.append(curChild.getNodeValue()).append(" ");
                            break;
                        }
                        curChild = curChild.getNextSibling();
                    }
                }
            }
        } catch (XPathExpressionException xPathExpressionException) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "XPATH syntax error:" + xPathExpressionException.getMessage());
        } catch (SAXParseException saxParseException) {
            //aligns to MySQL: NULL is returned if xml_frag contains elements which are not properly nested or closed
            result = new StringBuilder("NULL");
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        return result.toString().trim();
    }
}