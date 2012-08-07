/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pl.edu.icm.coansys.heeut;

import java.io.IOException;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestHBaseMapReduce {

    private static final Log LOG = LogFactory.getLog(TestHBaseMapReduce.class);
    private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
    // some prefefinied names
    final protected long TEST_ROW_COUNT = 100;
    final protected String S_ROW_PREFIX = "row";
    final protected String S_COLUMN_FAMILY = "cf";
    final protected String S_COLUMN_QUALIFIER = "cq";
    final protected byte[] B_COLUMN_FAMILY = Bytes.toBytes(S_COLUMN_FAMILY);
    final protected byte[] B_COLUMN_QUALIFIER = Bytes.toBytes(S_COLUMN_QUALIFIER);
    final protected byte[] B_VALUE = Bytes.toBytes("value");

    private String getCurrentDateAppended(String name) {
        return name + "-" + new Date().getTime();
    }

    private void dropTable(String tableName) {
        try {
            UTIL.deleteTable(Bytes.toBytes(tableName));
        } catch (IOException ex) {
            LOG.info("Table can not be deleted: " + tableName + "\n" + ex.getLocalizedMessage());
        }
    }

    private HTable createAndPopulateDefaultTable(String tableName, long rowCount) throws IOException, InterruptedException {
        HTable htable = UTIL.createTable(Bytes.toBytes(tableName), B_COLUMN_FAMILY);
        List<Row> putList = new ArrayList<Row>();
        for (long i = 0; i < rowCount; ++i) {
            Put put = new Put(Bytes.toBytes(S_ROW_PREFIX + i));
            put.add(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER, B_VALUE);
            putList.add(put);
        }
        htable.batch(putList);
        return htable;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UTIL.startMiniCluster();
        UTIL.startMiniMapReduceCluster();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UTIL.shutdownMiniMapReduceCluster();
        UTIL.shutdownMiniCluster();
    }

   
    @Test(timeout = 1800000)
    public void testRowCounter() throws Exception {
        String tableInitName = getCurrentDateAppended("testRowCounter");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        Job job = RowCounter.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableInitName});
        job.waitForCompletion(true);
        long count = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS").getValue();
        Assert.assertEquals(TEST_ROW_COUNT, count);

        dropTable(tableInitName);
    }

    @Test(timeout = 1800000)
    public void testCopy() throws Exception {

        String tableInitName = getCurrentDateAppended("testCopy");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        final String tableCopyName = tableInitName + "Copy";
        HTable htableCopy = UTIL.createTable(Bytes.toBytes(tableCopyName), B_COLUMN_FAMILY);

        Job job = CopyTable.createSubmittableJob(UTIL.getConfiguration(), new String[]{"--new.name=" + tableCopyName, tableInitName});
        job.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) UTIL.countRows(htableCopy));

        dropTable(tableInitName);
        dropTable(tableCopyName);
    }

    @Test(timeout = 1800000)
    public void testExportImport() throws Exception {

        String tableInitName = getCurrentDateAppended("testExportImport");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();
        Path qualifiedTempDir = dfs.makeQualified(new Path("export-import-temp-dir"));
        Assert.assertFalse(dfs.exists(qualifiedTempDir));

        Job jobExport = Export.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableInitName, qualifiedTempDir.toString()});
        jobExport.waitForCompletion(true);

        Assert.assertTrue(dfs.exists(qualifiedTempDir));

        final String tableImportName = tableInitName + "Import";
        HTable htableImport = UTIL.createTable(Bytes.toBytes(tableImportName), B_COLUMN_FAMILY);

        Job jobImport = Import.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableImportName, qualifiedTempDir.toString()});
        jobImport.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) UTIL.countRows(htableImport));

        dropTable(tableInitName);
        dropTable(tableImportName);
    }

}
