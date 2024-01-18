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
import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

public class UpdateXML extends AbstractScalarFunction {

    public UpdateXML(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"UPDATEXML"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object argv : args) {
            if (FunctionUtils.isNull(argv)) {
                return "NULL";
            }
        }

        String xmlTarget = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        String xPathExpr = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        String newXml = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);

        String result;
        try {
            if (xPathExpr.isEmpty()) {
                throw new XPathExpressionException("''"); //align to MySQL
            }
            if (xmlTarget.isEmpty()) {
                result = "";
                return result;
            }
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            Document doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xmlTarget)));
            XPathFactory xPathfactory = XPathFactory.newInstance();
            XPath xpath = xPathfactory.newXPath();
            if (xPathExpr.contains("[") || xPathExpr.contains("]")) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "UpdateXML index visit is not support");
            } else if (xPathExpr.contains("@")) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "UpdateXML variable visit is not support");
            }
            //假设有标签<b:c> MySQL中可以直接用//b:c这种格式匹配，但是XPath中需要指定local-name
            xPathExpr = xPathExpr.replaceAll("(\\w+):(\\w+)", "*[local-name()='$2']");
            XPathExpression expr = xpath.compile(xPathExpr);
            NodeList nodeList = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
            if (nodeList.getLength() == 1) { //MySQL在匹配超过两个节点或者没有匹配节点时不会替换
                Node node = nodeList.item(0);
                //需要创建CDATA节点，而不是用String，因为String会被转义，例如如果newXml含有<和>，那么会被转义为&lt;和&gt;
                CDATASection cdata = doc.createCDATASection(newXml);
                if (node.getParentNode() != doc) { //非根节点直接替换掉原节点
                    Node parent = node.getParentNode();
                    parent.replaceChild(cdata, node);
                } else {
                    //如果找到的是根节点，需要新建一个Document，因为根节点不允许调用replaceChild方法
                    doc = factory.newDocumentBuilder().parse(new InputSource(new StringReader(newXml)));
                }
            }
            result = convertDocumentToString(doc);
        } catch (XPathExpressionException xPathExpressionException) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "XPATH syntax error:" + xPathExpressionException.getMessage());
        } catch (SAXParseException saxParseException) {
            //aligns to MySQL: NULL is returned if xml_frag contains elements which are not properly nested or closed
            result = "NULL";
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        return result;
    }

    public static String convertDocumentToString(Document doc) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes"); // 禁止输出XML头信息
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        //第一/二个正则是因为CDATA Node在输出时会带有CDATA标记，这里为了和MySQL对齐，需要使用正则表达式替换
        //第三个正则是因为XML在解析式会将空标签<a></a>解析为<a/>, 而MySQL不会, 所以需要用正则替换回来
        return writer.getBuffer().toString().
            replaceAll("<!\\[CDATA\\[", "").
            replaceAll("\\]\\]>", "").
            replaceAll("<(\\w+)/>", "<$1></$1>");
    }
}

