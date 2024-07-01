/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerChangeStreamsToBigQuery Spanner Change Streams to BigQuery}
 * template.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToBigQueryIT.class);

  private static final int MESSAGES_COUNT = 5;

  private SpannerResourceManager spannerResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final AtomicInteger counter = new AtomicInteger(0);

  private LaunchInfo launchInfo;

  @Before
  public void setup() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testSpannerChangeStreamsToBigQueryBasic() throws IOException {
    String spannerTable = testName + RandomStringUtils.randomAlphanumeric(1, 5);
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable);

    String cdcTable = spannerTable + "_changelog";
    spannerResourceManager.executeDdlStatement(createTableStatement);
    String createChangeStreamStatement =
        String.format(
            "CREATE CHANGE STREAM %s_stream FOR %s OPTIONS (value_capture_type = 'NEW_ROW')",
            testName, spannerTable);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    ;

    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("spannerProjectId", PROJECT)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("rpcPriority", "HIGH")
                    .addParameter("disableDlqRetries", "true")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    List<Pair> colValPairs =
        Arrays.asList(new Pair("FirstName", firstName), new Pair("LastName", lastName));
    Mutation expectedData = generateTableRow(spannerTable, key, colValPairs);
    spannerResourceManager.write(Collections.singletonList(expectedData));
    String query = queryCdcTable2(cdcTable, key);
    waitForQueryToReturnRows(query, 1, true, 10);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    for (FieldValueList row : tableResult.iterateAll()) {
      assertTrue(validateChangeLogTableRow(row, colValPairs));
    }
  }

  @Test
  public void testFlakySpannerChangeStreamsToBigQueryAddColumn() throws Exception {
    String spannerTable = testName + RandomStringUtils.randomAlphanumeric(1, 5);
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable);
    String cdcTable = spannerTable + "_changelog";
    spannerResourceManager.executeDdlStatement(createTableStatement);
    String createChangeStreamStatement =
        String.format(
            "CREATE CHANGE STREAM %s_stream FOR %s OPTIONS (value_capture_type = 'NEW_ROW')",
            testName, spannerTable);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    ;

    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("spannerProjectId", PROJECT)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("rpcPriority", "HIGH")
                    .addParameter("disableDlqRetries", "true")
                    .addParameter("deadLetterQueueDirectory", "gs://shuranzhang-test/dlq")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    ArrayList<Pair> colValPairs =
        new ArrayList<>(
            Arrays.asList(new Pair("FirstName", firstName), new Pair("LastName", lastName)));
    Mutation expectedData = generateTableRow(spannerTable, key, colValPairs);
    spannerResourceManager.write(Collections.singletonList(expectedData));
    String query = queryCdcTable2(cdcTable, key);
    waitForQueryToReturnRows(query, 1, false, 10);

    String alterTableStatement =
        String.format("ALTER TABLE %s ADD Password STRING(MAX)", spannerTable);
    spannerResourceManager.executeDdlStatement(alterTableStatement);
    addEmptyColumn("Password", cdcTable);
    waitForQueryToReturnRows(queryNewColInCdcTable(cdcTable), 1, false, 1);
    TimeUnit.MINUTES.sleep(15);

    int key2 = nextValue();
    String firstName2 = UUID.randomUUID().toString();
    String lastName2 = UUID.randomUUID().toString();
    String passWord = UUID.randomUUID().toString();
    List<Pair> colValPairs2 =
        Arrays.asList(
            new Pair("FirstName", firstName2),
            new Pair("LastName", lastName2),
            new Pair("Password", passWord));
    Mutation expectedData2 = generateTableRow(spannerTable, key2, colValPairs2);
    spannerResourceManager.write(Collections.singletonList(expectedData2));

    String query2 = queryCdcTable2(cdcTable, key2);
    waitForQueryToReturnRows(query2, 1, false, 2);
    TableResult tableResult2 = bigQueryResourceManager.runQuery(query2);
    assertEquals(1, tableResult2.getTotalRows());
    for (FieldValueList row : tableResult2.iterateAll()) {
      assertTrue(validateChangeLogTableRow(row, colValPairs2));
    }

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    assertEquals(1, tableResult.getTotalRows());
    colValPairs.add(new Pair("Password", "NULL"));
    for (FieldValueList row : tableResult.iterateAll()) {
      assertTrue(validateChangeLogTableRow(row, colValPairs));
    }
  }

  @Test
  public void testSpannerChangeStreamsToBigQueryAddTable() throws Exception {
    String spannerTable = testName + RandomStringUtils.randomAlphanumeric(1, 5);
    String createTableStatement =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable);
    String cdcTable = spannerTable + "_changelog";
    spannerResourceManager.executeDdlStatement(createTableStatement);
    String createChangeStreamStatement =
        String.format(
            "CREATE CHANGE STREAM %s_stream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW')",
            testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    ;

    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("spannerProjectId", PROJECT)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("rpcPriority", "HIGH")
                    .addParameter("disableDlqRetries", "true")));

    assertThatPipeline(launchInfo).isRunning();

    int key = nextValue();
    String firstName = UUID.randomUUID().toString();
    String lastName = UUID.randomUUID().toString();
    ArrayList<Pair> colValPairs =
        new ArrayList<>(
            Arrays.asList(new Pair("FirstName", firstName), new Pair("LastName", lastName)));
    Mutation expectedData = generateTableRow(spannerTable, key, colValPairs);
    spannerResourceManager.write(Collections.singletonList(expectedData));
    String query = queryCdcTable2(cdcTable, key);
    waitForQueryToReturnRows(query, 1, false, 10);

    String spannerTable2 = spannerTable + "_new";
    String cdcTable2 = spannerTable2 + "_changelog";
    String createTableStatement2 =
        String.format(
            "CREATE TABLE %s (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + ") PRIMARY KEY(Id)",
            spannerTable2);
    spannerResourceManager.executeDdlStatement(createTableStatement2);

    int key2 = nextValue();
    String firstName2 = UUID.randomUUID().toString();
    String lastName2 = UUID.randomUUID().toString();
    List<Pair> colValPairs2 =
        Arrays.asList(new Pair("FirstName", firstName2), new Pair("LastName", lastName2));
    Mutation expectedData2 = generateTableRow(spannerTable2, key2, colValPairs2);
    spannerResourceManager.write(Collections.singletonList(expectedData2));

    String query2 = queryCdcTable2(cdcTable2, key2);
    waitForQueryToReturnRows(query2, 1, false, 1);
    TableResult tableResult2 = bigQueryResourceManager.runQuery(query2);
    assertEquals(1, tableResult2.getTotalRows());
    for (FieldValueList row : tableResult2.iterateAll()) {
      assertTrue(validateChangeLogTableRow(row, colValPairs2));
    }
  }

  boolean validateChangeLogTableRow(FieldValueList row, List<Pair> pairs) {
    for (Pair pair : pairs) {
      try {
        if (pair.val == "NULL") {
          if (!row.get(pair.col).isNull()) return false;
        } else {
          if (!row.get(pair.col).getStringValue().equals(pair.val)) {
            LOG.info(
                "shuran bq changelog table val for col "
                    + pair.col
                    + " is: "
                    + row.get(pair.col).getStringValue()
                    + ", which is different from spanner val: "
                    + pair.val);
            return false;
          }
        }
      } catch (BigQueryException e) {
        LOG.info("Error when trying to read the value for column " + pair.col + ": " + e);
        return false;
      }
    }
    return true;
  }

  class Pair {
    String col;
    String val;

    Pair(String col, String val) {
      this.col = col;
      this.val = val;
    }
  }

  public static int nextValue() {
    return counter.getAndIncrement();
  }

  private static Mutation generateTableRow(String tableId, int key, List<Pair> pairs) {
    Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
    mutation.set("Id").to(key);
    for (Pair pair : pairs) {
      mutation.set(pair.col).to(pair.val);
    }
    return mutation.build();
  }

  private String queryCdcTable(String cdcTable, int key, String firstName) {
    return "SELECT * FROM `"
        + bigQueryResourceManager.getDatasetId()
        + "."
        + cdcTable
        + "`"
        + String.format(" WHERE Id = %d", key)
        + (firstName != null ? String.format(" AND FirstName = '%s'", firstName) : "");
  }

  private String queryCdcTable2(String cdcTable, int key) {
    return "SELECT * FROM `"
        + bigQueryResourceManager.getDatasetId()
        + "."
        + cdcTable
        + "`"
        + String.format(" WHERE Id = %d", key);
  }

  private String queryNewColInCdcTable(String cdcTable) {
    return "SELECT Password FROM `" + bigQueryResourceManager.getDatasetId() + "." + cdcTable + "`";
  }

  @NotNull
  private Supplier<Boolean> dataShownUp(String query, int minRows) {
    return () -> {
      try {
        return bigQueryResourceManager.runQuery(query).getTotalRows() >= minRows;
      } catch (Exception e) {
        if (ExceptionUtils.containsMessage(e, "Not found: Table")
            || ExceptionUtils.containsMessage(e, "Unrecognized name")) {
          return false;
        } else {
          LOG.info("shuran data failed to show up:" + e);
          throw e;
        }
      }
    };
  }

  private void waitForQueryToReturnRows(
      String query, int resultsRequired, boolean cancelOnceDone, int timeOut) throws IOException {
    Config config = createConfig(launchInfo, Duration.ofMinutes(timeOut));
    Result result =
        cancelOnceDone
            ? pipelineOperator()
                .waitForConditionAndCancel(config, dataShownUp(query, resultsRequired))
            : pipelineOperator().waitForCondition(config, dataShownUp(query, resultsRequired));
    assertThatResult(result).meetsConditions();
  }

  public void addEmptyColumn(String newColumnName, String tableId) {
    try {

      Table table = bigQueryResourceManager.getTableIfExists(tableId);
      Schema schema = table.getDefinition().getSchema();
      FieldList fields = schema.getFields();

      // Create the new field/column
      Field newField = Field.of(newColumnName, LegacySQLTypeName.STRING);

      // Create a new schema adding the current fields, plus the new one
      List<Field> fieldList = new ArrayList<Field>();
      fields.forEach(fieldList::add);
      fieldList.add(newField);
      Schema newSchema = Schema.of(fieldList);

      // Update the table with the new schema
      Table updatedTable =
          table.toBuilder().setDefinition(StandardTableDefinition.of(newSchema)).build();
      updatedTable.update();
      LOG.info("shuran Empty column successfully added to table");
    } catch (BigQueryException e) {
      LOG.info("shuran Empty column was not added. \n" + e.toString());
    }
  }
}
