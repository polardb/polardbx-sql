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

package com.alibaba.polardbx.gms.metadb.schema;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.XmlHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaChangeBuilder extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerInit.TDDL_DYNAMIC_CONFIG;

    private static final String DEFAULT_DDL_DIR = "ddl";
    private static final String FILE_SEPARATOR = "/";
    private static final String DDL_INDEX_FILE = "__index__";

    private static final String CHARSET = "UTF-8";

    private static final String TAG_SYSTEM_TABLES = "SystemTables";
    private static final String TAG_SYSTEM_TABLE = "SystemTable";
    private static final String TAG_DDL_CREATE = "Create";
    private static final String TAG_DDL_CHANGE = "Change";
    private static final String ATTR_NAME = "name";

    private static final String TEST_IGNORED = "ignored";

    private Map<String, Pair<String, List<String>>> schemaChanges = new HashMap<>();

    private final String ddlDir;

    public SchemaChangeBuilder() {
        this.ddlDir = DEFAULT_DDL_DIR;
    }

    public SchemaChangeBuilder(String ddlDir) {
        this.ddlDir = ddlDir;
    }

    @Override
    protected void doInit() {
        super.doInit();
        build();
    }

    public Map<String, Pair<String, List<String>>> getSchemaChanges() {
        return schemaChanges;
    }

    private void build() {
        final String ddlDirPrefix = ddlDir + FILE_SEPARATOR;

        InputStream ddlIndexFileStream = getInputStream(ddlDirPrefix + DDL_INDEX_FILE);

        if (ddlIndexFileStream == null) {
            String errMsg = "Not found the DDL Index File: " + ddlDir;
            LOGGER.error(errMsg);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "read DDL Index File", errMsg);
        }

        List<String> xmlFiles = new ArrayList<>();
        try {
            BufferedReader ddlIndexFileReader = new BufferedReader(new InputStreamReader(ddlIndexFileStream));
            for (String line = ddlIndexFileReader.readLine(); line != null; line = ddlIndexFileReader.readLine()) {
                if (validXmlFileName(line)) {
                    xmlFiles.add(line);
                }
            }
        } catch (IOException e) {
            String errMsg = "Failed to collect DDL XML files. Caused by: " + e.getMessage();
            LOGGER.error(errMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, e, "read DDL XML Files", errMsg);
        }

        long totalTime = 0L;

        for (String xmlFile : xmlFiles) {
            InputStream ddlFileInputStream = getInputStream(ddlDirPrefix + xmlFile);
            totalTime += resolveXML(ddlFileInputStream, xmlFile);
        }

        LOGGER.info("Already processed " + xmlFiles.size() + " DDL XML files for Schema Change (" + totalTime + "ms)");
    }

    private long resolveXML(InputStream inputStream, String xmlFileName) {
        if (inputStream != null && TStringUtil.isNotBlank(xmlFileName)) {
            String xmlContent = readXML(inputStream, xmlFileName);
            if (TStringUtil.isNotBlank(xmlContent)) {
                Element systemTables = generateSystemTables(xmlContent, xmlFileName);
                return resolveSystemTables(systemTables, xmlFileName);
            }
        }
        return 0L;
    }

    private long resolveSystemTables(Element systemTables, String xmlFileName) {
        String unexpectedContent = "resolve schema change file content";

        // The SystemTables element is actual root.
        if (systemTables == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, unexpectedContent,
                "the root element '" + TAG_SYSTEM_TABLES + "' doesn't exist");
        }

        long startTime = System.currentTimeMillis();

        NodeList systemTableList = systemTables.getElementsByTagName(TAG_SYSTEM_TABLE);
        if (systemTableList != null && systemTableList.getLength() > 0) {
            for (int i = 0; i < systemTableList.getLength(); i++) {
                Element systemTableElem = (Element) systemTableList.item(i);

                // The system table name.
                String systemTableName = systemTableElem.getAttribute(ATTR_NAME);
                if (TStringUtil.isBlank(systemTableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, unexpectedContent,
                        "No " + ATTR_NAME + " is provided for the element '" + TAG_SYSTEM_TABLE + "'");
                }

                if (TStringUtil.equalsIgnoreCase(systemTableName, TEST_IGNORED)) {
                    // For test only.
                    continue;
                }

                // The only full table creation statement.
                String createDDL;
                NodeList createList = systemTableElem.getElementsByTagName(TAG_DDL_CREATE);
                if (createList != null && createList.getLength() == 1) {
                    Element createElem = (Element) createList.item(0);
                    createDDL = TStringUtil.trim(createElem.getTextContent());
                    if (TStringUtil.isBlank(createDDL)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, unexpectedContent,
                            "the 'Create' element shouldn't be empty");
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, unexpectedContent,
                        "there should be only one 'Create' element");
                }

                // The schema change statements that may be empty.
                List<String> changeDDLs = null;
                NodeList changeList = systemTableElem.getElementsByTagName(TAG_DDL_CHANGE);
                if (changeList != null && changeList.getLength() > 0) {
                    changeDDLs = new ArrayList<>();
                    for (int j = 0; j < changeList.getLength(); j++) {
                        Element changeElem = (Element) changeList.item(j);
                        String changeDDL = TStringUtil.trim(changeElem.getTextContent());
                        if (TStringUtil.isBlank(changeDDL)) {
                            // Ignore the empty element without any exception.
                            continue;
                        }
                        changeDDLs.add(changeDDL);
                    }
                }

                // Build schema changes.
                schemaChanges.put(systemTableName, new Pair<>(createDDL, changeDDLs));
            }

            long endTime = System.currentTimeMillis();

            LOGGER.info("Processed " + systemTableList.getLength() + " system tables in "
                + xmlFileName + " (" + (endTime - startTime) + "ms)");

            return endTime - startTime;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, unexpectedContent,
                "there should be at least one '" + TAG_SYSTEM_TABLE + "' element");
        }
    }

    private Element generateSystemTables(String xmlContent, String xmlFileName) {
        try {
            InputStream inputStream = new ByteArrayInputStream(xmlContent.getBytes());
            Document document = XmlHelper.createDocument(inputStream);
            return document.getDocumentElement();
        } catch (Exception e) {
            LOGGER.error("Failed to create an XML document from :\n" + xmlContent, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_SCHEMA_CHANGE, e, "create an XML document for",
                xmlFileName,
                e.getMessage());
        }
    }

    private String readXML(InputStream inputStream, String xmlFileName) {
        try {
            StringBuilder xmlContent = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, CHARSET));
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                xmlContent.append(line);
            }
            return xmlContent.toString();
        } catch (IOException e) {
            LOGGER.error("Failed to read the schema change file '" + xmlFileName + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_SCHEMA_CHANGE, e, "read", xmlFileName, e.getMessage());
        }
    }

    private boolean validXmlFileName(String line) {
        return TStringUtil.endsWithIgnoreCase(line, ".xml");
    }

    private InputStream getInputStream(String fileName) {
        ClassLoader classLoader = SchemaChangeBuilder.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);
        if (inputStream == null) {
            inputStream = ClassLoader.getSystemResourceAsStream(fileName);
        }
        return inputStream;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        schemaChanges.clear();
    }
}
