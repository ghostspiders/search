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
package org.server.search.discovery.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

public class JoinHelper {

    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";

    // the timeout for each join attempt
    public static final Setting<TimeValue> JOIN_TIMEOUT_SETTING =
        Setting.timeSetting("cluster.join.timeout",
            TimeValue.timeValueMillis(60000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final MasterService masterService;
    private final TransportService transportService;
    private final JoinTaskExecutor joinTaskExecutor;
    private final TimeValue joinTimeout;

    final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = ConcurrentCollections.newConcurrentSet();

    public JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService,
                      TransportService transportService, LongSupplier currentTermSupplier,
                      BiConsumer<JoinRequest, JoinCallback> joinHandler, Function<StartJoinRequest, Join> joinLeaderInTerm) {
        this.masterService = masterService;
        this.transportService = transportService;
        this.joinTimeout = JOIN_TIMEOUT_SETTING.get(settings);
        this.joinTaskExecutor = new JoinTaskExecutor(allocationService, logger) {

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm = currentTermSupplier.getAsLong();
                if (currentState.term() != currentTerm) {
                    final CoordinationMetaData coordinationMetaData =
                            CoordinationMetaData.builder(currentState.coordinationMetaData()).term(currentTerm).build();
                    final MetaData metaData = MetaData.builder(currentState.metaData()).coordinationMetaData(coordinationMetaData).build();
                    currentState = ClusterState.builder(currentState).metaData(metaData).build();
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false, JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(request, transportJoinCallback(request, channel)));

        transportService.registerRequestHandler(MembershipAction.DISCOVERY_JOIN_ACTION_NAME, MembershipAction.JoinRequest::new,
            ThreadPool.Names.GENERIC, false, false,
            (request, channel, task) -> joinHandler.accept(new JoinRequest(request.getNode(), Optional.empty()), // treat as non-voting join
                transportJoinCallback(request, channel)));

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(MembershipAction.DISCOVERY_JOIN_VALIDATE_ACTION_NAME,
            () -> new MembershipAction.ValidateJoinRequest(), ThreadPool.Names.GENERIC,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)); // TODO: implement join validation

        transportService.registerRequestHandler(
            ZenDiscovery.DISCOVERY_REJOIN_ACTION_NAME, ZenDiscovery.RejoinClusterRequest::new, ThreadPool.Names.SAME,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)); // TODO: do we need to implement anything here?

        transportService.registerRequestHandler(
            MembershipAction.DISCOVERY_LEAVE_ACTION_NAME, MembershipAction.LeaveRequest::new, ThreadPool.Names.SAME,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)); // TODO: do we need to implement anything here?
    }

    private JoinCallback transportJoinCallback(TransportRequest request, TransportChannel channel) {
        return new JoinCallback() {

            @Override
            public void onSuccess() {
                try {
                    channel.sendResponse(Empty.INSTANCE);
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.warn("failed to send back failure on join request", inner);
                }
            }

            @Override
            public String toString() {
                return "JoinCallback{request=" + request + "}";
            }
        };
    }

    public void sendJoinRequest(DiscoveryNode destination, Optional<Join> optionalJoin) {
        sendJoinRequest(destination, optionalJoin, () -> {
        });
    }

    public void sendJoinRequest(DiscoveryNode destination, Optional<Join> optionalJoin, Runnable onCompletion) {
        assert destination.isMasterNode() : "trying to join master-ineligible " + destination;
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);
        if (pendingOutgoingJoins.add(dedupKey)) {
            logger.debug("attempting to join {} with {}", destination, joinRequest);
            final String actionName;
            final TransportRequest transportRequest;
            if (Coordinator.isZen1Node(destination)) {
                actionName = MembershipAction.DISCOVERY_JOIN_ACTION_NAME;
                transportRequest = new MembershipAction.JoinRequest(transportService.getLocalNode());
            } else {
                actionName = JOIN_ACTION_NAME;
                transportRequest = joinRequest;
            }
            transportService.sendRequest(destination, actionName, transportRequest,
                TransportRequestOptions.builder().withTimeout(joinTimeout).build(),
                new TransportResponseHandler<Empty>() {
                    @Override
                    public Empty read(StreamInput in) {
                        return Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(Empty response) {
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.debug("successfully joined {} with {}", destination, joinRequest);
                        onCompletion.run();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        pendingOutgoingJoins.remove(dedupKey);
                        logger.info(() -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest), exp);
                        onCompletion.run();
                    }

                    @Override
                    public String executor() {
                        return Names.SAME;
                    }
                });
        } else {
            logger.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    public void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isMasterNode()
            : "sending start-join request for master-ineligible " + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME,
            startJoinRequest, new TransportResponseHandler<Empty>() {
                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    logger.debug("successful response to {} from {}", startJoinRequest, destination);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
    }

    public interface JoinCallback {
        void onSuccess();

        void onFailure(Exception e);
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final JoinCallback joinCallback;

        JoinTaskListener(JoinTaskExecutor.Task task, JoinCallback joinCallback) {
            this.task = task;
            this.joinCallback = joinCallback;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinCallback.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinCallback.onSuccess();
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback);

        default void close(Mode newMode) {
        }
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            masterService.submitStateUpdateTask("node-join", task, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(task, joinCallback));
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert false : "unexpected join from " + sender + " during initialisation";
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            joinCallback.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, JoinCallback joinCallback) {
            assert closed == false : "CandidateJoinAccumulator closed";
            JoinCallback prev = joinRequestAccumulator.put(sender, joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";

                pendingAsTasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {
                });
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else {
                assert newMode == Mode.FOLLOWER : newMode;
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() +
                ", closed=" + closed + '}';
        }
    }
}
