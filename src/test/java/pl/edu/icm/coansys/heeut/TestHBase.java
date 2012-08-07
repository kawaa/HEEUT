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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;

public class TestHBase {

    private static final Log LOG = LogFactory.getLog(TestHBase.class);
    private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private static String S_TABLE_PREFIX = "table";
    private static String S_ROW_PREFIX = "row";
    private static byte[] B_COLUMN_FAMILY = Bytes.toBytes("cf");
    private static byte[] B_COLUMN_QUALIFIER = Bytes.toBytes("cq");
    private static byte[] B_VALUE = Bytes.toBytes("value");

    @BeforeClass
    public static void beforeClass() throws Exception {
        UTIL.startMiniCluster();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UTIL.shutdownMiniCluster();
    }

    private void dropTable(String tableName) {
        try {
            UTIL.deleteTable(Bytes.toBytes(tableName));
        } catch (IOException ex) {
            LOG.info("Table can not be deleted: " + tableName + "\n" + ex.getLocalizedMessage());
        }
    }

    @Test(timeout = 1800000)
    public void testPutAndGet() throws Exception {

        HTable htable = UTIL.createTable(Bytes.toBytes(S_TABLE_PREFIX), B_COLUMN_FAMILY);
        byte[] ROW = Bytes.toBytes(S_ROW_PREFIX);

        Put put = new Put(ROW);
        put.add(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER, B_VALUE);
        htable.put(put);

        Get get = new Get(ROW);
        get.addColumn(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER);
        Result result = htable.get(get);

        Assert.assertEquals(1, UTIL.countRows(htable));
        Assert.assertEquals(Bytes.toString(ROW), Bytes.toString(result.getRow()));
        Assert.assertEquals(Bytes.toString(B_VALUE), Bytes.toString(result.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));

        dropTable(S_TABLE_PREFIX);
    }
}
