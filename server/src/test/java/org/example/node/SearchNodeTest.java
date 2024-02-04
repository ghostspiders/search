package org.example.node;

import org.junit.Test;

import java.util.Map;

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
        Map<String, String> settings = searchNode.getSettings();
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println("Key: " + key + ", Value: " + value);
        }
    }
}


