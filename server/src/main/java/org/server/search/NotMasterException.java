/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.server.search;


/**
 * Thrown when a node join request or a master ping reaches a node which is not
 * currently acting as a master or when a cluster state update task is to be executed
 * on a node that is no longer master.
 */
public class NotMasterException extends RuntimeException {

    public NotMasterException(String msg) {
        super(msg);
    }

    @Override
    public Throwable fillInStackTrace() {
        return null;
    }
}
