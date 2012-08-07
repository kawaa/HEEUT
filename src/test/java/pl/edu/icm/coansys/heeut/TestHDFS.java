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

import java.util.Date;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHDFS {
    
    private static final Log LOG = LogFactory.getLog(TestHDFS.class);
     private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
     
    @BeforeClass
    public static void beforeClass() throws Exception {
        UTIL.startMiniDFSCluster(1);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UTIL.shutdownMiniDFSCluster();
    }
    
    private String getCurrentDateAppended(String name) {
        return name + new Date().getTime();
    }
    
    @Test(timeout = 1800000)
    public void testMkdir() throws Exception {
         
        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();
        String tmpDirName = getCurrentDateAppended("tmp");
        
        Path qualifiedTmpDir = dfs.makeQualified(new Path(tmpDirName));
        Assert.assertFalse(dfs.exists(qualifiedTmpDir));
        
        dfs.create(qualifiedTmpDir);
        Assert.assertTrue(dfs.exists(qualifiedTmpDir));
    }
}
