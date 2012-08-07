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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.util.ToolRunner;

public class TestMapReduce {

     private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

    
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

    private String getCurrentDateAppended(String name) {
        return name + new Date().getTime();
    }

    @Test(timeout = 1800000)
    public void testWordCount() throws Exception {

        String prefix = getCurrentDateAppended("wordcount");
        String inputDirName = prefix + "-input";
        String outputDirName = prefix + "-output";

        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();
        Path inputDir = new Path(inputDirName);
        Path qualifiedInputDir = dfs.makeQualified(inputDir);

        dfs.copyFromLocalFile(new Path("src/test/resource/input/wordcount/apache_projects.dat"), qualifiedInputDir);
        ToolRunner.run(UTIL.getConfiguration(), new WordCount(), new String[]{inputDirName, outputDirName});

        InputStream contentStream = dfs.open(new Path(outputDirName + "/part-00000"));
        BufferedReader contentReader = new BufferedReader(new InputStreamReader(contentStream));
        Assert.assertEquals("Apache\t3", contentReader.readLine());
        Assert.assertEquals("HBase\t1", contentReader.readLine());
        Assert.assertEquals("Hadoop\t1", contentReader.readLine());
        Assert.assertEquals("Pig\t1", contentReader.readLine());

        Assert.assertNull(contentReader.readLine());
        contentReader.close();
    }

    @Test(timeout = 1800000)
    public void testWordCountDiff() throws Exception {

        String prefix = getCurrentDateAppended("wordcount");
        String inputDirName = prefix + "-input";
        String outputDirName = prefix + "-output";

        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();
        Path inputDir = new Path(inputDirName);
        Path qualifiedInputDir = dfs.makeQualified(inputDir);

        String inputFileName = "src/test/resource/input/wordcount/apache_projects.dat";
        dfs.copyFromLocalFile(new Path(inputFileName), qualifiedInputDir);
        ToolRunner.run(UTIL.getConfiguration(), new WordCount(), new String[]{inputDirName, outputDirName});

        Path outputDir = new Path(outputDirName);
        Path qualifiedOutputDir = dfs.makeQualified(outputDir);

        String localOutputDir = "src/test/resource/output/wordcount/" + prefix;
        dfs.copyToLocalFile(qualifiedOutputDir, new Path(localOutputDir));

        File outputFile = new File(localOutputDir + "/part-00000");
        File expectedFile = new File("src/test/resource/exp/wordcount/apache_projects.exp");
        boolean isEqual = FileUtils.contentEquals(outputFile, expectedFile);
        Assert.assertTrue(isEqual);
    }
}
