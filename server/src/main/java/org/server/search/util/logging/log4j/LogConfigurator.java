/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.server.search.util.logging.log4j;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.server.search.env.Environment;
import org.server.search.env.FailedToResolveConfigException;
import org.server.search.util.MapBuilder;
import org.server.search.util.settings.ImmutableSettings;
import org.server.search.util.settings.Settings;

import java.util.Map;
import java.util.Properties;

import static org.server.search.util.settings.ImmutableSettings.*;

/**
 * 
 */
public class LogConfigurator {
    private static final Logger logger = LogManager.getLogger("HelloWorld");

    private static boolean loaded;

    private static ImmutableMap<String, String> replacements = new MapBuilder<String, String>()
            .put("console", "org.apache.logging.log4j.core.appender.ConsoleAppender")
            .put("async", "org.apache.log4j.AsyncAppender")
            .put("dailyRollingFile", "org.apache.log4j.DailyRollingFileAppender")
            .put("externallyRolledFile", "org.apache.log4j.ExternallyRolledFileAppender")
            .put("file", "org.apache.log4j.FileAppender")
            .put("jdbc", "org.apache.log4j.JDBCAppender")
            .put("jms", "org.apache.log4j.JMSAppender")
            .put("lf5", "org.apache.log4j.LF5Appender")
            .put("ntevent", "org.apache.log4j.NTEventLogAppender")
            .put("null", "org.apache.log4j.NullAppender")
            .put("rollingFile", "org.apache.log4j.RollingFileAppender")
            .put("smtp", "org.apache.log4j.SMTPAppender")
            .put("socket", "org.apache.log4j.SocketAppender")
            .put("socketHub", "org.apache.log4j.SocketHubAppender")
            .put("syslog", "org.apache.log4j.SyslogAppender")
            .put("telnet", "org.apache.log4j.TelnetAppender")
                    // layouts
            .put("simple", "org.apache.log4j.SimpleLayout")
            .put("html", "org.apache.log4j.HTMLLayout")
            .put("pattern", "org.apache.log4j.PatternLayout")
            .put("consolePattern", "org.server.search.util.logging.JLinePatternLayout")
            .put("ttcc", "org.apache.log4j.TTCCLayout")
            .put("xml", "org.apache.log4j.XMLLayout")
            .immutableMap();

    public static void configure(Settings settings) {
        if (loaded) {
            return;
        }
        loaded = true;
        Environment environment = new Environment(settings);
        Settings.Builder settingsBuilder = settingsBuilder().putAll(settings);
        try {
            ((ImmutableSettings.Builder) settingsBuilder).loadFromUrl(environment.resolveConfig("logging.yml"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        } catch (NoClassDefFoundError e) {
            // ignore, no yaml
        }
        try {
            ((ImmutableSettings.Builder) settingsBuilder).loadFromUrl(environment.resolveConfig("logging.json"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        }
        try {
            ((ImmutableSettings.Builder) settingsBuilder).loadFromUrl(environment.resolveConfig("logging.properties"));
        } catch (FailedToResolveConfigException e) {
            // ignore
        }
        ((ImmutableSettings.Builder) settingsBuilder)
                .putProperties("Search.", System.getProperties())
                .putProperties("es.", System.getProperties())
                .replacePropertyPlaceholders();
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : settingsBuilder.build().getAsMap().entrySet()) {
            String key = "log4j." + entry.getKey();
            String value = entry.getValue();
            if (replacements.containsKey(value)) {
                value = replacements.get(value);
            }
            if (key.endsWith(".value")) {
                props.setProperty(key.substring(0, key.length() - ".value".length()), value);
            } else if (key.endsWith(".type")) {
                props.setProperty(key.substring(0, key.length() - ".type".length()), value);
            } else {
                props.setProperty(key, value);
            }
        }
//        PropertyConfigurator.configure(props);
    }

    public static void main(String[] args) {
//        Logger logger = LogManager.getLogger(LogConfigurator.class);
        logger.error("Error level message.");
        logger.warn("Warn level message.");
        logger.info("Info level message.");
        logger.debug("Debug level message.");
        logger.trace("Trace level message.");
    }
}
