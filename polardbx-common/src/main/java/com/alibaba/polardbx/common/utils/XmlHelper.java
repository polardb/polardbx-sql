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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.Writer;


public class XmlHelper {

    public static Document createDocument(InputStream xml, InputStream schema, EntityResolver entityResolver) {
        try {
            DocumentBuilderFactory bf = DocumentBuilderFactory.newInstance();
            if (entityResolver == null) {
                bf.setValidating(false);
                bf.setNamespaceAware(true);
                bf.setAttribute("http://xml.org/sax/features/namespaces", Boolean.FALSE);
                bf.setAttribute("http://xml.org/sax/features/validation", Boolean.FALSE);
                bf.setAttribute("http://apache.org/xml/features/nonvalidating/load-dtd-grammar", Boolean.FALSE);
                bf.setAttribute("http://apache.org/xml/features/nonvalidating/load-external-dtd", Boolean.FALSE);
            }

            DocumentBuilder builder = bf.newDocumentBuilder();
            if (entityResolver != null) {
                builder.setEntityResolver(entityResolver);
            }
            builder.setErrorHandler(new ErrorHandler() {

                @Override
                public void warning(SAXParseException exception) throws SAXException {
                    throw GeneralUtil.nestedException(exception);
                }

                @Override
                public void fatalError(SAXParseException exception) throws SAXException {
                    throw GeneralUtil.nestedException(exception);
                }

                @Override
                public void error(SAXParseException exception) throws SAXException {
                    throw GeneralUtil.nestedException(exception);
                }
            });
            return builder.parse(xml);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, e, "Xml Parser Error.");
        }
    }

    public static Document createDocument(InputStream xml) {
        return createDocument(xml, null, null);
    }

    public static Document createDocument(InputStream xml, InputStream schema) {
        return createDocument(xml, schema, null);
    }

    public static void callWriteXmlFile(Document doc, Writer w, String encoding) {
        try {
            Source source = new DOMSource(doc);
            Result result = new StreamResult(w);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();

            Transformer xformer = transformerFactory.newTransformer();
            xformer.setOutputProperty(OutputKeys.INDENT, "yes");
            xformer.setOutputProperty(OutputKeys.ENCODING, encoding);
            xformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");

            DocumentType doctype = doc.getDoctype();
            if (doctype != null) {
                xformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, doctype.getPublicId());
                xformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, doctype.getSystemId());
            }

            xformer.transform(source, result);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
