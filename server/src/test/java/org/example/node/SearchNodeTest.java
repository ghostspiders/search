package org.example.node;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author gaoyvfeng
 * @ClassName SearchNodeTest
 * @description:
 * @datetime 2024年 02月 02日 18:02
 * @version: 1.0
 */
public class SearchNodeTest {

    @Test
    public void configTest(){
        SearchNode searchNode = new SearchNode("local_test", "configTest");
        assertNotNull(searchNode.getName());
        assertNotNull(searchNode.getWorkingDir());
        assertNotNull(searchNode.getConfigFile());
        assertNotNull(searchNode.getConfPathLogs());
        assertNotNull(searchNode.getTransportPortFile());
        assertNotNull(searchNode.getEsInputFile());
        assertNotNull(searchNode.getEsOutputFile());
        assertNotNull(searchNode.getTmpDir());
        assertNotNull(searchNode.getConfPathData());
        assertNotNull(searchNode.getDefaultConfig());
    }

}
