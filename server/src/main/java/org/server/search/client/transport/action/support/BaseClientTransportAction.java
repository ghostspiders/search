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

package org.server.search.client.transport.action.support;

import org.server.search.ElasticSearchException;
import org.server.search.ElasticSearchIllegalArgumentException;
import org.server.search.ElasticSearchIllegalStateException;
import org.server.search.action.ActionFuture;
import org.server.search.action.ActionListener;
import org.server.search.action.ActionRequest;
import org.server.search.action.ActionResponse;
import org.server.search.action.support.PlainActionFuture;
import org.server.search.client.transport.action.ClientTransportAction;
import org.server.search.cluster.node.Node;
import org.server.search.transport.BaseTransportResponseHandler;
import org.server.search.transport.RemoteTransportException;
import org.server.search.transport.TransportService;
import org.server.search.util.Nullable;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.settings.Settings;

import java.lang.reflect.Constructor;

import static org.server.search.action.support.PlainActionFuture.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class BaseClientTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent implements ClientTransportAction<Request, Response> {

    protected final TransportService transportService;

    private final Constructor<Response> responseConstructor;

    protected BaseClientTransportAction(Settings settings, TransportService transportService, Class<Response> type) {
        super(settings);
        this.transportService = transportService;
        try {
            this.responseConstructor = type.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new ElasticSearchIllegalArgumentException("No default constructor is declared for [" + type.getName() + "]");
        }
        responseConstructor.setAccessible(true);
    }

    @Override public ActionFuture<Response> submit(Node node, Request request) throws ElasticSearchException {
        return submit(node, request, null);
    }

    @Override public ActionFuture<Response> submit(Node node, Request request, @Nullable ActionListener<Response> listener) {
        PlainActionFuture<Response> future = newFuture(listener);
        if (listener == null) {
            // since we don't have a listener, and we release a possible lock with the future
            // there is no need to execute it under a listener thread
            request.listenerThreaded(false);
        }
        execute(node, request, future);
        return future;
    }

    @Override public void execute(Node node, final Request request, final ActionListener<Response> listener) {
        transportService.sendRequest(node, action(), request, new BaseTransportResponseHandler<Response>() {
            @Override public Response newInstance() {
                return BaseClientTransportAction.this.newInstance();
            }

            @Override public void handleResponse(Response response) {
                listener.onResponse(response);
            }

            @Override public void handleException(RemoteTransportException exp) {
                listener.onFailure(exp);
            }

            @Override public boolean spawn() {
                return request.listenerThreaded();
            }
        });
    }

    protected abstract String action();

    protected Response newInstance() {
        try {
            return responseConstructor.newInstance();
        } catch (Exception e) {
            throw new ElasticSearchIllegalStateException("Failed to create a new instance");
        }
    }
}
