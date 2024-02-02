package org.example.node;

import lombok.Data;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;

/**
 * @author gaoyvfeng
 * @ClassName SearchNode
 * @description:
 * @datetime 2024年 02月 02日 17:09
 * @version: 1.0
 */
@Data
public class SearchNode {
    private final String name;
    private final Path workingDir;
    private final Path configFile;
    private final Path confPathLogs;
    private final Path transportPortFile;
    private final Path httpPortsFile;
    private final Path esOutputFile;
    private final Path esInputFile;
    private final Path tmpDir;
    private Path confPathData;
    final LinkedHashMap<String, String> defaultConfig = new LinkedHashMap<>();
    SearchNode(
            String clusterName,
            String name
    ) {
        this.name = name;
        workingDir = getClassPath().toPath().resolve(safeName(name)).toAbsolutePath();
        configFile = workingDir.resolve("config/elasticsearch.yml");
        confPathData = workingDir.resolve("data");
        confPathLogs = workingDir.resolve("logs");
        transportPortFile = confPathLogs.resolve("transport.ports");
        httpPortsFile = confPathLogs.resolve("http.ports");
        esOutputFile = confPathLogs.resolve("es.out");
        esInputFile = workingDir.resolve("es.in");
        tmpDir = workingDir.resolve("tmp");
        defaultConfig.put("cluster.name", clusterName);
    }
    String safeName(String name) {
        return name.replaceAll("^[^a-zA-Z0-9]+", "").replaceAll("[^a-zA-Z0-9\\.]+", "-");
    }
    File getClassPath(){
        ClassLoader classLoader = SearchNode.class.getClassLoader();
        URL resource = classLoader.getResource(".");
        File directory = new File(resource.getPath());
        return directory;
    }

}
