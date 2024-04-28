package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.HttpClientHelper;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.ColumnarVersionUtil.ColumnarVersions;
import com.alibaba.polardbx.net.util.ColumnarTargetUtil;
import com.google.common.truth.Truth;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarVersionUtilTest {

    private static final String COLUMNAR_VERSIONS =
        "{ \"polardbx_columnar_image_version\": \"5.4.19-20240320_17109018-5322410_20240321_7c453d3c-cloud-normal\", \"polardbx_columnar_rpm_version\": \"t-polardbx-columnar-5.4.19-20240320_17109018.noarch.rpm\", \"polardbx_columnar_daemon_version\": \"t-polardbx-cdc-5.4.19-20240319_17108478.noarch.rpm\", \"polardbx_columnar_fw_branch_name\": \"master\", \"polardbx_version\": \"5.4.19\", \"polardbx_sql_version\": \"5.4.19-SNAPSHOT\", \"polardbx_cdc_client_version\": \"2.0.32\" }\n";
    private static final ColumnarVersions COLUMNAR_VERSIONS_OBJECT =
        JSONObject.parseObject(COLUMNAR_VERSIONS, ColumnarVersions.class);
    private static final String COLUMNAR_RPM_VERSION = "5.4.19-20240320_17109018";

    @Test
    public void testGetVersion() {
        try (
            final MockedStatic<ColumnarTargetUtil> columnarTargetUtilMockedStatic = mockStatic(
                ColumnarTargetUtil.class);
            final MockedStatic<HttpClientHelper> httpClientHelperMockedStatic = mockStatic(
                HttpClientHelper.class);) {
            final AtomicBoolean getDaemonMasterTargetFailed = new AtomicBoolean(false);
            final String[] queryVersionResult = new String[] {COLUMNAR_VERSIONS};
            columnarTargetUtilMockedStatic.when(ColumnarTargetUtil::getDaemonMasterTarget)
                .thenAnswer(invocation -> {
                    if (getDaemonMasterTargetFailed.get()) {
                        throw new RuntimeException("Mock getDaemonMasterTarget failed");
                    } else {
                        return "127.0.0.1:3007";
                    }
                });
            httpClientHelperMockedStatic.when(
                    () -> HttpClientHelper.doGet(ArgumentMatchers.contains("/columnar/system/getVersion")))
                .thenAnswer(invocation -> queryVersionResult[0]);
            Truth.assertThat(ColumnarVersionUtil.getVersion()).isEqualTo(COLUMNAR_RPM_VERSION);

            // getDaemonMasterTarget failed
            getDaemonMasterTargetFailed.set(true);
            Truth.assertThat(ColumnarVersionUtil.getVersion()).isNull();

            // getDaemonMasterTarget returns non-json result
            getDaemonMasterTargetFailed.set(false);
            queryVersionResult[0] = "xxx";
            Truth.assertThat(ColumnarVersionUtil.getVersion()).isNull();
        }
    }

    @Test
    public void testColumnarVersions() {
        final String jsonString = JSONObject.toJSONString(COLUMNAR_VERSIONS_OBJECT);
        final ColumnarVersions columnarVersions = JSONObject.parseObject(jsonString, ColumnarVersions.class);

        EqualsVerifier
            .forClass(ColumnarVersions.class)
            .suppress(Warning.STRICT_INHERITANCE)
            .verify();
        Truth.assertThat(columnarVersions.hashCode()).isEqualTo(COLUMNAR_VERSIONS_OBJECT.hashCode());
    }
}