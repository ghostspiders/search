package org.example.node;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author gaoyvfeng
 * @ClassName SearchNode
 * @description:
 * @datetime 2024年 02月 02日 17:09
 * @version: 1.0
 */
@Data
public class SearchNode {
    private static final Logger logger = LogManager.getLogger(SearchNode.class);

    private final String name;
    private final Path workingDir;
    private final Properties  configProp;
    private final Path confPathLogs;
    private final Path esOutputFile;
    private final Path esInputFile;
    private final Path tmpDir;
    private Path confPathData;
    private Map<String, String> settings = new HashMap<>();
    final LinkedHashMap<String, String> defaultConfig = new LinkedHashMap<>();
    SearchNode(
            String clusterName,
            String name
    ) {
        this.name = name;
        workingDir = Paths.get(getProjectPath()).resolve(safeName(name)).toAbsolutePath();
        configProp = readConfigProp(getProjectPath()+"/config/search.properties");
        confPathData = workingDir.resolve("data");
        confPathLogs = workingDir.resolve("logs");
        esOutputFile = confPathLogs.resolve("es.out");
        esInputFile = workingDir.resolve("es.in");
        tmpDir = workingDir.resolve("tmp");
        defaultConfig.put("cluster.name", clusterName);
        createConfiguration();
    }
    private void createConfiguration() {
        String nodeName = safeName(name);
        Map<String, String> baseConfig = new HashMap<>(defaultConfig);
        if (nodeName != null) {
            baseConfig.put("node.name", nodeName);
        }
        baseConfig.put("path.data", confPathData.toAbsolutePath().toString());
        baseConfig.put("path.logs", confPathLogs.toAbsolutePath().toString());
        baseConfig.put("http.port", configProp.getProperty("http.port"));
        baseConfig.put("transport.tcp.port", configProp.getProperty("transport.tcp.port"));
        baseConfig.put("discovery.seed_hosts", configProp.getProperty("discovery.seed_hosts"));
        baseConfig.put("indices.memory.index_buffer_size", configProp.getProperty("indices.memory.index_buffer_size"));
        baseConfig.put("index.refresh_interval", configProp.getProperty("index.refresh_interval"));
        baseConfig.put("index.number_of_shards", configProp.getProperty("index.number_of_shards"));
        baseConfig.put("index.number_of_replicas", configProp.getProperty("index.number_of_replicas"));
        baseConfig.put("index.analysis.analyzer.default", configProp.getProperty("index.analysis.analyzer.default"));
        baseConfig.put("index.analysis.tokenizer", configProp.getProperty("index.analysis.tokenizer"));
        baseConfig.put("index.analysis.filter", configProp.getProperty("index.analysis.filter"));
        settings.putAll(baseConfig);
        logger.info("Written config file:{} for {}", configProp, this);
    }
    private Properties readConfigProp(String path){
        Properties prop = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            prop.load(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return prop;
    }

    String safeName(String name) {
        return name.replaceAll("^[^a-zA-Z0-9]+", "").replaceAll("[^a-zA-Z0-9\\.]+", "-");
    }
    public File getClassPath(){
        ClassLoader classLoader = SearchNode.class.getClassLoader();
        URL resource = classLoader.getResource(".");
        File directory = new File(resource.getPath());
        return directory;
    }
    public String getProjectPath(){
        String currentDirectory = System.getProperty("user.dir");
        return new File(currentDirectory).getParent();
    }
    public Map<String, String> getSettings(){
        return this.settings;
    }


}
