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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableList;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.Filterable;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.manipulation.Sortable;
import org.junit.runner.manipulation.Sorter;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.TestClass;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

import java.lang.annotation.Annotation;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A common test case runner in qa-test module.
 * 1. Unify the parameterized and no-parameterized test case runner.
 * 2. Enable customizing test annotations.
 * 3. Enable qa-test properties to interfere the behavior of test case runner.
 */
public class CommonCaseRunner extends Parameterized implements Filterable, Sortable {
    /**
     * Test case ignored on file-storage.
     */
    public static class AnnotationBasedFilter<T extends Annotation> extends Filter {
        /**
         * Annotation in test class header.
         */
        private final T typeAnnotation;

        private final Class<T> klass;

        public AnnotationBasedFilter(T typeAnnotation, Class<T> klass) {
            this.typeAnnotation = typeAnnotation;
            this.klass = klass;
        }

        @Override
        public boolean shouldRun(Description description) {
            // run anyway if not in file-storage mode and not in columnar mode
            if (!PropertiesUtil.useFileStorage() && !PropertiesUtil.columnarMode()) {
                return true;
            }

            // check annotation in test class
            if (typeAnnotation != null && (PropertiesUtil.useFileStorage() || PropertiesUtil.columnarMode())) {
                return false;
            }

            if (description.isTest()) {
                // check annotation in single test method.
                T annotation = description.getAnnotation(klass);
                boolean hitAnnotation =
                    annotation != null && (PropertiesUtil.useFileStorage() || PropertiesUtil.columnarMode());
                return !hitAnnotation;
            } else {
                // explicitly check if any children want to run
                for (Description each : description.getChildren()) {
                    if (shouldRun(each)) {
                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public String describe() {
            return "handle test case if annotation-based ignored";
        }
    }

    /**
     * In file-storage mode, run test case only if matching the specific case list.
     */
    public static class FileStorageCaseFilter extends Filter {
        private Class<?> klass;

        public FileStorageCaseFilter(Class<?> klass) {
            this.klass = klass;
        }

        @Override
        public boolean shouldRun(Description description) {
            if (PropertiesUtil.useFileStorage()
                && !ClassHelper.getFileStorageTestCases().contains(this.klass)) {
                return false;
            }
            if (PropertiesUtil.columnarMode()
                && !ClassHelper.getColumnarTestCases().contains(this.klass)) {
                return false;
            }
            return true;
        }

        @Override
        public String describe() {
            return "In file-storage mode, run test case only if matching the specific case list.";
        }
    }

    // 不管是binlog实验室，还是replica实验室，使用该注解的用例，都会被ignore
    public static class CdcIgnoreCaseFilter extends Filter {

        private final CdcIgnore typeAnnotation;

        public CdcIgnoreCaseFilter(CdcIgnore typeAnnotation) {
            this.typeAnnotation = typeAnnotation;
        }

        static Class<CdcIgnore> clazz = CdcIgnore.class;

        static boolean isCdcTesting() {
            String value = System.getProperty("skip_cdc");
            return StringUtils.equalsIgnoreCase(value, "false");
        }

        @Override
        public boolean shouldRun(Description description) {
            if (isCdcTesting()) {
                return !(typeAnnotation != null || description.getAnnotation(clazz) != null);
            }
            return true;
        }

        @Override
        public String describe() {
            return "handle test case if cdc-based ignored";
        }
    }

    public static class BinlogIgnoreCaseFilter extends Filter {

        private final BinlogIgnore typeAnnotation;

        public BinlogIgnoreCaseFilter(BinlogIgnore typeAnnotation) {
            this.typeAnnotation = typeAnnotation;
        }

        static Class<BinlogIgnore> clazz = BinlogIgnore.class;

        static boolean isBinlogTesting() {
            String value = System.getProperty("cdc_test_type");
            return StringUtils.equalsIgnoreCase(value, "binlog");
        }

        @Override
        public boolean shouldRun(Description description) {
            if (isBinlogTesting()) {
                return !(typeAnnotation != null || description.getAnnotation(clazz) != null);
            }
            return true;
        }

        @Override
        public String describe() {
            return "handle test case if binlog-based ignored";
        }
    }

    public static class ReplicaIgnoreCaseFilter extends Filter {

        private final ReplicaIgnore typeAnnotation;

        public ReplicaIgnoreCaseFilter(ReplicaIgnore typeAnnotation) {
            this.typeAnnotation = typeAnnotation;
        }

        static Class<ReplicaIgnore> clazz = ReplicaIgnore.class;

        static boolean isReplicaTesting() {
            String value = System.getProperty("cdc_test_type");
            return StringUtils.equalsIgnoreCase(value, "replica");
        }

        @Override
        public boolean shouldRun(Description description) {
            if (isReplicaTesting()) {
                return !(typeAnnotation != null || description.getAnnotation(clazz) != null);
            }
            return true;
        }

        @Override
        public String describe() {
            return "handle test case if replica-based ignored";
        }
    }

    private final TestClass testClass;
    private final Runner internalRunner;

    /**
     * Only called reflectively. Do not use programmatically.
     */
    public CommonCaseRunner(Class<?> klass) throws Throwable {
        super(MockParameterizedTest.class);
        this.testClass = new TestClass(klass);

        // Automatically choose the proper runner according to parameterized method existence.
        Runner runner;
        try {
            if (hasParametersMethod()) {
                runner = new ParameterizedRunnerV2(klass);
            } else {
                runner = new BlockJUnit4ClassRunner(klass);
            }
        } catch (InitializationError e) {
            if (e.getCauses().stream().anyMatch(t -> "No runnable methods".equals(t.getMessage()))) {
                // for test base class that should not run test.
                runner = new IgnoredClassRunner(klass);
            } else {
                throw e;
            }
        }
        this.internalRunner = runner;

        if (PropertiesUtil.useFileStorage() || PropertiesUtil.columnarMode()) {
            // check if designated.
            Filter fileStorageCaseFilter = new FileStorageCaseFilter(klass);

            // check if ignored.
            // use file-storage case filter if configured.
            FileStoreIgnore typeAnnotation = this.testClass.getAnnotation(FileStoreIgnore.class);
            Filter annotationBasedFilter = new AnnotationBasedFilter(typeAnnotation, FileStoreIgnore.class);

            Filter addtionalFilter = PropertiesUtil.columnarMode() ? getColumnarFilter() : Filter.ALL;

            try {
                fileStorageCaseFilter.apply(this.internalRunner);
                annotationBasedFilter.apply(this.internalRunner);
                addtionalFilter.apply(this.internalRunner);
            } catch (NoTestsRemainException ex) {
                // ignore the whole case.
            }
        }

        if (CdcIgnoreCaseFilter.isCdcTesting()) {
            try {
                CdcIgnore typeAnnotation = testClass.getAnnotation(CdcIgnore.class);
                Filter cdcIgnoreCaseFilter = new CdcIgnoreCaseFilter(typeAnnotation);
                cdcIgnoreCaseFilter.apply(this.internalRunner);
            } catch (NoTestsRemainException ex) {
                // ignore the whole case.
            }

            if (BinlogIgnoreCaseFilter.isBinlogTesting()) {
                try {
                    BinlogIgnore typeAnnotation = testClass.getAnnotation(BinlogIgnore.class);
                    Filter binlogIgnoreCaseFilter = new BinlogIgnoreCaseFilter(typeAnnotation);
                    binlogIgnoreCaseFilter.apply(this.internalRunner);
                } catch (NoTestsRemainException ex) {
                    // ignore the whole case.
                }
            } else if (ReplicaIgnoreCaseFilter.isReplicaTesting()) {
                try {
                    ReplicaIgnore typeAnnotation = testClass.getAnnotation(ReplicaIgnore.class);
                    Filter replicaIgnoreCaseFilter = new ReplicaIgnoreCaseFilter(typeAnnotation);
                    replicaIgnoreCaseFilter.apply(this.internalRunner);
                } catch (NoTestsRemainException ex) {
                    // ignore the whole case.
                }
            }
        }

    }

    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
        ((Filterable) this.internalRunner).filter(filter);
    }

    @Override
    public void sort(Sorter sorter) {
        ((Sortable) this.internalRunner).sort(sorter);
    }

    @Override
    public Description getDescription() {
        return this.internalRunner.getDescription();
    }

    @Override
    public void run(RunNotifier notifier) {
        this.internalRunner.run(notifier);
    }

    @Override
    public int testCount() {
        return this.internalRunner.testCount();
    }

    private boolean hasParametersMethod() throws Exception {
        List<FrameworkMethod> methods = this.testClass.getAnnotatedMethods(
            Parameterized.Parameters.class);
        for (FrameworkMethod each : methods) {
            if (each.isStatic() && each.isPublic()) {
                return true;
            }
        }

        return false;
    }

    private Filter getColumnarFilter() {
        ColumnarIgnore typeAnnotation = this.testClass.getAnnotation(ColumnarIgnore.class);
        Filter annotationBasedFilter = new AnnotationBasedFilter(typeAnnotation, ColumnarIgnore.class);
        return annotationBasedFilter;
    }

    /**
     * Copy the parameterized logic code from org.junit.runners.Parameterized
     */
    private static class ParameterizedRunnerV2 extends Suite {
        private static final ParametersRunnerFactory DEFAULT_FACTORY =
            new BlockJUnit4ClassRunnerWithParametersFactory();

        private static final List<Runner> NO_RUNNERS = Collections.<Runner>emptyList();

        private List<Runner> runners;

        public ParameterizedRunnerV2(Class<?> klass) throws Throwable {
            super(klass, NO_RUNNERS);
            ParametersRunnerFactory runnerFactory = getParametersRunnerFactory(
                klass);
            Parameterized.Parameters parameters = getParametersMethod().getAnnotation(
                Parameterized.Parameters.class);
            runners = Collections.unmodifiableList(createRunnersForParameters(
                allParameters(), parameters.name(), runnerFactory));
        }

        private ParametersRunnerFactory getParametersRunnerFactory(Class<?> klass)
            throws InstantiationException, IllegalAccessException {
            Parameterized.UseParametersRunnerFactory annotation = klass
                .getAnnotation(Parameterized.UseParametersRunnerFactory.class);
            if (annotation == null) {
                return DEFAULT_FACTORY;
            } else {
                Class<? extends ParametersRunnerFactory> factoryClass = annotation
                    .value();
                return factoryClass.newInstance();
            }
        }

        @Override
        protected List<Runner> getChildren() {
            return runners;
        }

        private TestWithParameters createTestWithNotNormalizedParameters(
            String pattern, int index, Object parametersOrSingleParameter) {
            Object[] parameters =
                (parametersOrSingleParameter instanceof Object[]) ? (Object[]) parametersOrSingleParameter
                    : new Object[] {parametersOrSingleParameter};
            return createTestWithParameters(getTestClass(), pattern, index,
                parameters);
        }

        @SuppressWarnings("unchecked")
        private Iterable<Object> allParameters() throws Throwable {
            if (PropertiesUtil.useFileStorage()
                && (getTestClass().getAnnotation(FileStoreIgnore.class) != null
                || !ClassHelper.getFileStorageTestCases().contains(getTestClass().getJavaClass()))) {
                return ImmutableList.of();
            }

            if (PropertiesUtil.columnarMode()
                && (getTestClass().getAnnotation(FileStoreIgnore.class) != null
                || getTestClass().getAnnotation(ColumnarIgnore.class) != null
                || !ClassHelper.getColumnarTestCases().contains(getTestClass().getJavaClass()))
            ) {
                return ImmutableList.of();
            }

            Object parameters = getParametersMethod().invokeExplosively(null);
            if (parameters instanceof Iterable) {
                return (Iterable<Object>) parameters;
            } else if (parameters instanceof Object[]) {
                return Arrays.asList((Object[]) parameters);
            } else {
                throw parametersMethodReturnedWrongType();
            }
        }

        private FrameworkMethod getParametersMethod() throws Exception {
            List<FrameworkMethod> methods = getTestClass().getAnnotatedMethods(
                Parameterized.Parameters.class);
            for (FrameworkMethod each : methods) {
                if (each.isStatic() && each.isPublic()) {
                    return each;
                }
            }

            throw new Exception("No public static parameters method on class "
                + getTestClass().getName());
        }

        private List<Runner> createRunnersForParameters(
            Iterable<Object> allParameters, String namePattern,
            ParametersRunnerFactory runnerFactory)
            throws InitializationError,
            Exception {
            try {
                List<TestWithParameters> tests = createTestsForParameters(
                    allParameters, namePattern);
                List<Runner> runners = new ArrayList<Runner>();
                for (TestWithParameters test : tests) {
                    runners.add(runnerFactory
                        .createRunnerForTestWithParameters(test));
                }
                return runners;
            } catch (ClassCastException e) {
                throw parametersMethodReturnedWrongType();
            }
        }

        private List<TestWithParameters> createTestsForParameters(
            Iterable<Object> allParameters, String namePattern)
            throws Exception {
            int i = 0;
            List<TestWithParameters> children = new ArrayList<TestWithParameters>();
            for (Object parametersOfSingleTest : allParameters) {
                children.add(createTestWithNotNormalizedParameters(namePattern,
                    i++, parametersOfSingleTest));
            }
            return children;
        }

        private Exception parametersMethodReturnedWrongType() throws Exception {
            String className = getTestClass().getName();
            String methodName = getParametersMethod().getName();
            String message = MessageFormat.format(
                "{0}.{1}() must return an Iterable of arrays.",
                className, methodName);
            return new Exception(message);
        }

        private static TestWithParameters createTestWithParameters(
            TestClass testClass, String pattern, int index, Object[] parameters) {
            String finalPattern = pattern.replaceAll("\\{index\\}",
                Integer.toString(index));
            String name = MessageFormat.format(finalPattern, parameters);
            return new TestWithParameters("[" + name + "]", testClass,
                Arrays.asList(parameters));
        }
    }

    /**
     * Ignore cases.
     */
    public class IgnoredClassRunner extends Runner implements Filterable, Sortable {
        private final Class<?> fTestClass;

        public IgnoredClassRunner(Class<?> testClass) {
            this.fTestClass = testClass;
        }

        public void run(RunNotifier notifier) {
            notifier.fireTestIgnored(this.getDescription());
        }

        public Description getDescription() {
            return Description.createSuiteDescription(this.fTestClass);
        }

        @Override
        public void filter(Filter filter) throws NoTestsRemainException {
            // ignore
        }

        @Override
        public void sort(Sorter sorter) {
            // ignore
        }
    }

    /**
     * For Parameterized (super class) constructor
     */
    public static class MockParameterizedTest {
        @Parameterized.Parameters
        public static List mockPrepare() {
            return ImmutableList.of();
        }
    }
}
