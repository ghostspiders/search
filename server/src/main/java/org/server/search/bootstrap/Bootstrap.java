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

package org.server.search.bootstrap;

import com.google.inject.CreationException;
import com.google.inject.spi.Message;
import org.server.search.ExceptionsHelper;
import org.server.search.Version;
import org.server.search.env.Environment;
import org.server.search.jmx.JmxService;
import org.server.search.server.Server;
import org.server.search.server.ServerBuilder;
import org.server.search.server.internal.InternalSettingsPrepare;
import org.server.search.util.Classes;
import org.server.search.util.Tuple;
import org.server.search.util.logging.Loggers;
import org.server.search.util.settings.Settings;
import org.slf4j.Logger;

import java.util.Set;
import static com.google.common.collect.Sets.*;
import static org.server.search.util.settings.ImmutableSettings.Builder.*;
import static org.server.search.util.settings.ImmutableSettings.*;


public class Bootstrap {
    private Server server;
    private void setup(boolean addShutdownHook){
        Tuple<Settings, Environment> tuple = InternalSettingsPrepare.prepareSettings(EMPTY_SETTINGS, true);
        try {
            Classes.getDefaultClassLoader().loadClass("org.slf4j.Logger");
        } catch (Exception e) {
            System.err.println("Failed to configure logging...");
            e.printStackTrace();
        }

        if (tuple.v1().get(JmxService.SettingsConstants.CREATE_CONNECTOR) == null) {
            // automatically create the connector if we are bootstrapping
            Settings updated = settingsBuilder().putAll(tuple.v1()).putBoolean(JmxService.SettingsConstants.CREATE_CONNECTOR, true).build();
            tuple = new Tuple<Settings, Environment>(updated, tuple.v2());
        }

        ServerBuilder serverBuilder = ServerBuilder.serverBuilder().settings(tuple.v1()).loadConfigSettings(false);
        server = serverBuilder.build();
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override public void run() {
                    try {
                        destroy();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * hook for JSVC
     */
    public void init(boolean addShutdownHook) throws Exception {
        setup(addShutdownHook);
    }

    /**
     * hook for JSVC
     */
    public void start() throws Exception {
        server.start();
    }

    /**
     * hook for JSVC
     */
    public void stop() throws InterruptedException {
        server.stop();
    }


    /**
     * hook for JSVC
     */
    public void destroy() throws Exception {
        server.close();
    }

    public static void main(String[] args) {
        Bootstrap bootstrap = new Bootstrap();
        String stage = "Initialization";
        try {
            bootstrap.init(true);
            stage = "Startup";
            bootstrap.start();
        } catch (Throwable e) {
            Logger logger = Loggers.getLogger(Bootstrap.class);
            if (bootstrap.server != null) {
                logger = Loggers.getLogger(Bootstrap.class, bootstrap.server.settings().get("name"));
            }
            StringBuilder errorMessage = new StringBuilder("{").append(Version.full()).append("}: ");
            errorMessage.append(stage).append(" Failed ...\n");
            if (e instanceof CreationException) {
                CreationException createException = (CreationException) e;
                Set<String> seenMessages = newHashSet();
                int counter = 1;
                for (Message message : createException.getErrorMessages()) {
                    String detailedMessage;
                    if (message.getCause() == null) {
                        detailedMessage = message.getMessage();
                    } else {
                        detailedMessage = ExceptionsHelper.detailedMessage(message.getCause(), true,  0);
                    }
                    if (detailedMessage == null) {
                        detailedMessage = message.getMessage();
                    }
                    if (seenMessages.contains(detailedMessage)) {
                        continue;
                    }
                    seenMessages.add(detailedMessage);
                    errorMessage.append(counter++).append(") ").append(detailedMessage);
                }
            } else {
                errorMessage.append("- ").append(ExceptionsHelper.detailedMessage(e, true, 0));
            }
            logger.error(errorMessage.toString());
            if (logger.isDebugEnabled()) {
                logger.debug("Exception", e);
            }
            System.exit(3);
        }
    }
}
