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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.client.transport.TransportClient;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.DefaultClusterService;
import org.server.search.cluster.node.DiscoveryNode;
import org.server.search.cluster.node.Node;
import org.server.search.cluster.node.Nodes;
import org.server.search.cluster.routing.strategy.ShardsRoutingStrategy;
import org.server.search.discovery.Discovery;
import org.server.search.gateway.GatewayService;
import org.server.search.threadpool.ThreadPool;
import org.server.search.transport.TransportService;
import org.server.search.util.component.AbstractComponent;
import org.server.search.util.component.Lifecycle;
import org.server.search.util.settings.Settings;
import java.util.*;
import java.util.function.Supplier;

public class Coordinator extends AbstractComponent implements Discovery {

    // 定义了使用新版Zen2发现机制的字符串常量
    public static final String ZEN2_DISCOVERY_TYPE = "zen";
    // 定义了单节点发现机制的字符串常量，用于单一节点运行Elasticsearch时
    public static final String SINGLE_NODE_DISCOVERY_TYPE = "single-node";
    // 发现类型设置，指定了默认值为ZEN2_DISCOVERY_TYPE，并且设置了作用域为节点级别
    public static final String DISCOVERY_TYPE_SETTING = ZEN2_DISCOVERY_TYPE;
    // 种子节点提供者设置，用于配置新版Zen2发现的种子节点
    private Map<String, Supplier<SettingsBasedSeedHostsProvider>> hostProviders = new HashMap<>();
    private List<String> seedProviderNames;
    private TransportService transportService;
    private DefaultClusterService masterService;
    private ShardsRoutingStrategy allocationService;
    // 是否使用单节点发现模式
    private final boolean singleNodeDiscovery;
    // 互斥锁，用于同步对共享资源的访问
    final Object mutex = new Object();
    // 集群协调状态，初始化在启动时
    private final SetOnce<CoordinationState> coordinationState = new SetOnce<>();
    // 当前模式，如跟随者、候选者或领导者
    private Mode mode;
    private  JoinHelper joinHelper;
    private JoinHelper.JoinAccumulator joinAccumulator;
    // 对等发现器，用于发现集群中的其他节点
    private final PeerFinder peerFinder;
    // 上一次加入信息
    private Optional<Join> lastJoin;
    private Optional<CoordinatorPublication> currentPublication = Optional.empty();
    private  FollowersChecker followersChecker;

    public Coordinator(Settings settings, ThreadPool threadPool, TransportService transportService, TransportClient networkService, DefaultClusterService masterService,
                       ShardsRoutingStrategy allocationService, GatewayService gatewayMetaState) {
        super(settings);
        this.seedProviderNames = componentSettings.getListStr("discovery.seed_providers",Collections.emptyList());
        // 初始化种子主机提供者的映射
        hostProviders.put("settings", () -> new SettingsBasedSeedHostsProvider(settings, transportService));
        this.transportService = transportService;
        // 主服务，处理主节点相关逻辑
        this.masterService = masterService;
        // 资源分配服务，处理集群中资源的分配
        this.allocationService = allocationService;
        // 注册加入验证器，包括内置和自定义验证器
        // 是否启用单节点发现模式
        this.singleNodeDiscovery = SINGLE_NODE_DISCOVERY_TYPE.equals(componentSettings.get("discovery.type"));
        // 初始化节点加入帮助类
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService,
                this::getCurrentTerm, this::getStateForMasterService, this::handleJoinRequest, this::joinLeaderInTerm, this.onJoinValidators);
        // 持久化状态供应器
        this.persistedStateSupplier = persistedStateSupplier;
        // 无主节点阻塞服务
        this.noMasterBlockService = new NoMasterBlockService(settings, clusterSettings);
        // 上一个已知的主节点
        this.lastKnownLeader = Optional.empty();
        // 上一次加入信息
        this.lastJoin = Optional.empty();
        // 节点加入累加器
        this.joinAccumulator = new InitialJoinAccumulator();
        // 集群状态发布超时设置
        this.publishTimeout = PUBLISH_TIMEOUT_SETTING.get(settings);
        // 随机数生成器
        this.random = random;
        // 选举调度工厂
        this.electionSchedulerFactory = new ElectionSchedulerFactory(settings, random, transportService.getThreadPool());
        // 预投票收集器
        this.preVoteCollector = new PreVoteCollector(transportService, this::startElection, this::updateMaxTermSeen);
        // 配置的种子主机解析器
        this.configuredHostsResolver = new SeedHostsResolver(nodeName, settings, transportService, seedHostsProvider);
        // 对等发现器
        this.peerFinder = new CoordinatorPeerFinder(settings, transportService,
                new HandshakingTransportAddressConnector(settings, transportService), configuredHostsResolver);
        // 状态发布传输处理器
        this.publicationHandler = new PublicationTransportHandler(transportService, namedWriteableRegistry,
                this::handlePublishRequest, this::handleApplyCommit);
        // 主节点检查器
        this.leaderChecker = new LeaderChecker(settings, transportService, getOnLeaderFailure());
        // 跟随者节点检查器
        this.followersChecker = new FollowersChecker(settings, transportService, this::onFollowerCheckRequest, this::removeNode);
        // 节点移除执行器
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        // 集群应用者
        this.clusterApplier = clusterApplier;
        // 设置集群状态供应器
        masterService.setClusterStateSupplier(this::getStateForMasterService);
        // 重新配置器
        this.reconfigurator = new Reconfigurator(settings, clusterSettings);
        // 集群引导服务
        this.clusterBootstrapService = new ClusterBootstrapService(settings, transportService, this::getFoundPeers,
                this::isInitialConfigurationSet, this::setInitialConfiguration);
        // 发现升级服务
        this.discoveryUpgradeService = new DiscoveryUpgradeService(settings, transportService,
                this::isInitialConfigurationSet, joinHelper, peerFinder::getFoundPeers, this::setInitialConfiguration);
        // 延迟检测器
        this.lagDetector = new LagDetector(settings, transportService.getThreadPool(), n -> removeNode(n, "lagging"),
                transportService::getLocalNode);
        // 集群形成失败帮助器
        this.clusterFormationFailureHelper = new ClusterFormationFailureHelper(settings, this::getClusterFormationState,
                transportService.getThreadPool(), joinHelper::logLastFailedJoinAttempt);
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {

    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public Coordinator start() throws Exception {
        return this;
    }

    @Override
    public Discovery stop() throws SearchException, InterruptedException {
        return null;
    }

    @Override
    public void close() throws SearchException, InterruptedException {

    }
    /**
     * 定义节点在分布式系统中可能处于的模式。
     */
    public enum Mode {
        CANDIDATE, // 候选人模式，表示节点可以参与领导者选举。
        LEADER,   // 领导者模式，表示节点是集群的领导者。
        FOLLOWER   // 跟随者模式，表示节点遵循领导者的指令。
    }
    /**
     * 获取当前任期。
     * 需要同步互斥锁来确保线程安全。
     *
     * @return 当前任期。
     */
    long getCurrentTerm() {
        synchronized (mutex) {
            return coordinationState.get().getCurrentTerm();
        }
    }
    /**
     * 获取用于主服务的集群状态。
     * 此方法返回协调状态中最后被接受的集群状态，作为主服务计算下一个集群状态更新的基础。
     *
     * @return 用于主服务的集群状态。
     */
    ClusterState getStateForMasterService() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            final ClusterState clusterState = coordinationState.get().getLastAcceptedState();
            // 如果当前模式不是领导者，或者集群状态的任期与当前任期不一致，
            // 则返回带有无主节点块的集群状态。
            if (mode != Mode.LEADER || clusterState.term() != getCurrentTerm()) {
                return clusterStateWithNoMasterBlock(clusterState);
            }
            // 如果是领导者并且任期匹配，则直接返回集群状态。
            return clusterState;
        }
    }
    /**
     * 创建一个没有主节点的集群状态。
     * 如果原始集群状态包含主节点ID，则此方法会移除主节点ID并添加一个没有主节点的全局块。
     *
     * @param clusterState 原始集群状态。
     * @return 包含没有主节点块的新集群状态，或者如果原始状态中没有主节点ID，则返回原始状态。
     */
    private ClusterState clusterStateWithNoMasterBlock(ClusterState clusterState) {
        // 检查原始集群状态是否包含主节点ID。
        if (clusterState.nodes().getMasterNodeId() != null) {
            // 如果存在主节点ID，则添加没有主节点的全局块。
            String masterNodeId = clusterState.nodes().getMasterNodeId();
            final Nodes build = Nodes.newNodesBuilder().putAll(clusterState.nodes()).remove(masterNodeId).build();
            // 构建并返回一个新的ClusterState，包含更新后的blocks和nodes。
            return ClusterState.builder(clusterState).nodes(build).build();
        } else {
            // 如果原始集群状态中没有主节点ID，直接返回原始状态。
            return clusterState;
        }
    }
    /**
     * 处理来自其他节点的加入请求。
     *
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    private void handleJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        // 断言当前线程不持有互斥锁。
        assert Thread.holdsLock(mutex) == false;
        // 断言本地节点具有成为主节点的资格。
        assert getLocalNode().isMasterNode() : getLocalNode() + " received a join but is not master-eligible";

        // 记录日志，显示当前模式和正在处理的加入请求。
        logger.trace("handleJoinRequest: as {}, handling {}", mode, joinRequest);

        // 如果是单节点发现模式并且请求节点不是本地节点，则调用回调的onFailure方法。
        if (singleNodeDiscovery && joinRequest.getSourceNode().equals(getLocalNode()) == false) {
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + "discovery.type" +
                    "] set to [" + "single-node"  + "] discovery"));
            return;
        }

        // 连接到请求节点。
        transportService.connectToNode(joinRequest.getSourceNode());

        // 获取当前集群状态，用于加入验证。
        final ClusterState stateForJoinValidation = getStateForMasterService();

        // 如果本地节点已被选举为主节点，则进行一系列验证。
        if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
            // 发送验证加入请求。
            sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);

        } else {
            // 如果本地节点未被选举为主节点，则处理加入请求。
            processJoinRequest(joinRequest, joinCallback);
        }
    }
    /**
     * 发送验证加入请求。
     * 此方法在接收到一个节点的加入请求时调用，用于验证该节点是否可以加入当前集群。
     *
     * @param stateForJoinValidation 用于验证的集群状态。
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    void sendValidateJoinRequest(ClusterState stateForJoinValidation, JoinRequest joinRequest,
                                 JoinHelper.JoinCallback joinCallback) {
        // 使用joinHelper发送验证请求到请求加入的节点。
        // 这个操作会异步执行，结果通过ActionListener<Empty>回调处理。
        joinHelper.sendValidateJoinRequest(joinRequest.getSourceNode(), stateForJoinValidation, new ActionListener<Empty>() {
            // 当验证请求成功响应时调用此方法。
            @Override
            public void onResponse(Empty empty) {
                try {
                    // 如果验证成功，进一步处理加入请求。
                    processJoinRequest(joinRequest, joinCallback);
                } catch (Exception e) {
                    // 如果处理加入请求时出现异常，通过回调通知失败。
                    joinCallback.onFailure(e);
                }
            }

            // 如果验证请求失败或超时，调用此方法。
            @Override
            public void onFailure(Throwable e) {
                // 记录警告日志，显示验证失败的节点和异常信息。
                logger.debug("failed to validate incoming join request from node [{}]",joinRequest.getSourceNode()), e);

                // 通过回调通知验证请求失败，并提供异常信息。
                joinCallback.onFailure(new IllegalStateException("failure when sending a validation request to node", e));
            }
        });
    }
    /**
     * 处理加入请求。
     * 当一个节点请求加入集群时，此方法会被调用以处理该请求。
     *
     * @param joinRequest 加入请求，包含请求节点和相关信息。
     * @param joinCallback 回调接口，用于在处理完成后发送响应或失败通知。
     */
    private void processJoinRequest(JoinRequest joinRequest, JoinHelper.JoinCallback joinCallback) {
        // 获取加入请求中可能包含的可选Join对象。
        final Optional<Join> optionalJoin = joinRequest.getOptionalJoin();

        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 获取当前协调状态。
            final CoordinationState coordState = coordinationState.get();

            // 记录之前是否赢得了选举。
            final boolean prevElectionWon = coordState.electionWon();

            // 如果存在Join对象，则处理它。
            optionalJoin.ifPresent(this::handleJoin);

            // 处理加入请求累积器的逻辑。
            joinAccumulator.handleJoinRequest(joinRequest.getSourceNode(), joinCallback);

            // 如果之前没有赢得选举，但现在赢得了，则变为领导者。
            if (prevElectionWon == false && coordState.electionWon()) {
                becomeLeader("handleJoinRequest");
            }
        }
    }
    /**
     * 将当前节点状态设置为领导者。
     *
     * @param method 触发成为领导者状态的方法名称，用于日志记录。
     */
    void becomeLeader(String method) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 断言当前模式应为候选人模式。
        assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
        // 断言本地节点应具有成为主节点的资格。
        assert getLocalNode().isMasterNode() : getLocalNode() + " became a leader but is not master-eligible";

        // 记录日志，显示当前方法、任期、之前和最后已知的领导者模式。
        logger.debug("{}: coordinator becoming LEADER in term {} (was {}, lastKnownLeader was [{}])",
                method, getCurrentTerm(), mode, lastKnownLeader);

        // 设置当前模式为领导者。
        mode = Mode.LEADER;

        // 关闭现有的加入累加器并创建一个新的领导者加入累加器。
        joinAccumulator.close(mode);
        joinAccumulator = joinHelper.new LeaderJoinAccumulator();

        // 更新最后已知的领导者为本地节点。
        lastKnownLeader = Optional.of(getLocalNode());

        // 停用peerFinder，因为当前节点已成为领导者。
        peerFinder.deactivate(getLocalNode());

        // 停用发现服务升级。
        discoveryUpgradeService.deactivate();

        // 停止集群形成失败帮助器。
        clusterFormationFailureHelper.stop();

        // 关闭预投票和选举调度器。
        closePrevotingAndElectionScheduler();

        // 更新预投票收集器，将当前节点设置为领导者。
        preVoteCollector.update(getPreVoteResponse(), getLocalNode());

        // 断言当前没有其他领导者。
        assert leaderChecker.leader() == null : leaderChecker.leader();

        // 更新追随者检查器的快速响应状态。
        followersChecker.updateFastResponseState(getCurrentTerm(), mode);
    }

    /**
     * 处理节点加入请求。
     * 此方法在接收到节点的加入请求时被调用。
     *
     * @param join 加入请求。
     */
    private void handleJoin(Join join) throws Exception {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 确保当前任期至少与加入请求中的任期一致，如果有更新，则处理加入请求。
            ensureTermAtLeast(getLocalNode(), join.getTerm()).ifPresent(this::handleJoin);

            // 如果我们已经赢得了选举，则实际的加入请求对选举结果没有影响，因此可以忽略任何异常。
            if (coordinationState.get().electionWon()) {
                // 处理加入请求，忽略可能的异常。
                final boolean isNewJoin = handleJoinIgnoringExceptions(join);

                // 如果我们已经完全成为主节点，并且没有发布操作正在进行，则根据需要安排重配置任务。
                final boolean establishedAsMaster = mode == Mode.LEADER && getLastAcceptedState().term() == getCurrentTerm();
                if (isNewJoin && establishedAsMaster && publicationInProgress() == false) {
                    scheduleReconfigurationIfNeeded();
                }
            } else {
                // 处理加入请求，这可能会失败并抛出异常。
                coordinationState.get().handleJoin(join);
            }
        }
    }
    /**
     * 检查是否有发布操作正在进行。
     * 需要同步互斥锁来确保线程安全。
     * @return 如果有发布操作正在进行返回true，否则返回false。
     */
    boolean publicationInProgress() {
        synchronized (mutex) {
            return currentPublication.isPresent();
        }
    }
    private void scheduleReconfigurationIfNeeded() {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        assert mode == Mode.LEADER : mode;
        assert currentPublication.isPresent() == false : "Expected no publication in progress";

        final ClusterState state = getLastAcceptedState();
        if (improveConfiguration(state) != state && reconfigurationTaskScheduled.compareAndSet(false, true)) {
            logger.trace("scheduling reconfiguration");
            masterService.submitStateUpdateTask("reconfigure", new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    reconfigurationTaskScheduled.set(false);
                    synchronized (mutex) {
                        return improveConfiguration(currentState);
                    }
                }

                @Override
                public void onFailure(String source, Exception e) {
                    reconfigurationTaskScheduled.set(false);
                    logger.debug("reconfiguration failed", e);
                }
            });
        }
    }
    /**
     * 获取最后被接受的集群状态。
     * 此方法返回协调状态中最后被接受的集群状态。
     *
     * @return 最后被接受的集群状态。
     */
    public ClusterState getLastAcceptedState() {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            return coordinationState.get().getLastAcceptedState();
        }
    }

    /**
     * 处理节点加入请求，忽略可能的异常。
     *
     * @param join 加入请求。
     * @return 如果加入请求来自新节点，并且已成功添加，则返回true。
     */
    private boolean handleJoinIgnoringExceptions(Join join) {
        try {
            // 处理加入请求。
            return coordinationState.get().handleJoin(join);
        } catch (Exception e) {
            // 如果处理加入请求失败，记录调试日志并忽略。
            logger.debug("failed to add {} - ignoring", join, e);
            return false;
        }
    }
    /**
     * 确保当前任期至少达到目标任期，如果不是，则加入领导者。
     *
     * @param sourceNode 触发任期更新的源节点。
     * @param targetTerm 目标任期。
     * @return 如果任期更新导致加入领导者，则返回包含Join对象的Optional；如果当前任期已满足或更高，则返回空的Optional。
     */
    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        // 如果当前任期小于目标任期，则创建一个新的StartJoinRequest并加入领导者。
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        // 如果当前任期已经满足或高于目标任期，则返回空的Optional。
        return Optional.empty();
    }
    /**
     * 加入指定任期中的领导者。
     * 此方法处理加入请求，并更新节点状态以反映新的任期和加入信息。
     *
     * @param startJoinRequest 包含领导者节点和目标任期的开始加入请求。
     * @return 返回一个Join对象，包含加入操作的结果。
     */
    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        // 进入同步块以确保线程安全。
        synchronized (mutex) {
            // 记录日志，显示正在加入的领导者节点和任期。
            logger.debug("joinLeaderInTerm: for [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());

            // 处理开始加入请求，并获取Join对象。
            final Join join = coordinationState.get().handleStartJoin(startJoinRequest);

            // 更新lastJoin为当前的Join对象。
            lastJoin = Optional.of(join);

            // 将peerFinder中的当前任期设置为最新的任期。
            peerFinder.setCurrentTerm(getCurrentTerm());

            // 检查当前模式是否不是候选人模式，如果不是，则变为候选人模式。
            if (mode != Mode.CANDIDATE) {
                becomeCandidate("joinLeaderInTerm"); // 这会更新followersChecker和preVoteCollector
            } else {
                // 如果已经是候选人模式，则更新快速响应状态和预投票收集器。
                followersChecker.updateFastResponseState(getCurrentTerm(), mode);
                preVoteCollector.update(getPreVoteResponse(), null);
            }

            // 返回Join对象，包含加入领导者的结果。
            return join;

        }
    }
    /**
     * 将当前节点状态设置为候选人。
     *
     * @param method 触发成为候选人状态的方法名称，用于日志记录。
     */
    void becomeCandidate(String method) {
        // 断言当前线程持有协调器的互斥锁。
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";

        // 记录日志，显示当前方法、任期、之前和最后已知的领导者模式。
        logger.debug("{}: coordinator becoming CANDIDATE in term {} (was {}, lastKnownLeader was [{}])",
                method, getCurrentTerm(), mode, lastKnownLeader);

        // 检查当前模式是否不是候选人模式，如果不是，则执行状态转换逻辑。
        if (mode != Mode.CANDIDATE) {
            // 记录之前的模式。
            final Mode prevMode = mode;
            // 设置当前模式为候选人。
            mode = Mode.CANDIDATE;

            // 取消任何活跃的发布操作。
            cancelActivePublication("become candidate: " + method);

            // 关闭现有的加入累加器并创建一个新的候选人加入累加器。
            joinAccumulator.close(mode);
            joinAccumulator = joinHelper.new CandidateJoinAccumulator();

            // 激活peerFinder，使用最后接受的集群状态中的节点列表。
            peerFinder.activate(coordinationState.get().getLastAcceptedState().nodes());

            // 启动集群形成失败帮助器。
            clusterFormationFailureHelper.start();

            // 如果当前任期是向后兼容的任期，则激活发现服务升级。
            if (getCurrentTerm() == ZEN1_BWC_TERM) {
                discoveryUpgradeService.activate(lastKnownLeader, coordinationState.get().getLastAcceptedState());
            }

            // 更新leaderChecker为没有当前节点，并将领导者设置为null。
            leaderChecker.setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
            leaderChecker.updateLeader(null);

            // 清除followersChecker中的当前节点并更新快速响应状态。
            followersChecker.clearCurrentNodes();
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);

            // 清除lagDetector跟踪的节点。
            lagDetector.clearTrackedNodes();

            // 如果之前是领导者模式，则清理主服务。
            if (prevMode == Mode.LEADER) {
                cleanMasterService();
            }

            // 如果应用者状态有主节点ID，则更新应用者状态为没有主节点，并应用新的集群状态。
            if (applierState.nodes().getMasterNodeId() != null) {
                applierState = clusterStateWithNoMasterBlock(applierState);
                clusterApplier.onNewClusterState("becoming candidate: " + method, () -> applierState, (source, e) -> {
                });
            }
        }

        // 更新预投票收集器。
        preVoteCollector.update(getPreVoteResponse(), null);
    }
    /**
     * 获取本地节点信息。
     * 此方法不要求同步，因为假设transportService的本地节点获取是线程安全的。
     *
     * @return 本地节点信息。
     */
    DiscoveryNode getLocalNode() {
        Node localNode = masterService.state().nodes().getLocalNode();
        return new DiscoveryNode(localNode.id(),localNode.address());
    }
    class CoordinatorPublication extends Publication {

        // 发布请求，包含要发布的集群状态信息。
        private  PublishRequest publishRequest;
        // 本地节点确认事件的ListenableFuture。
        private  ListenableFuture<Void> localNodeAckEvent;
        // 确认监听器。
        private  AckListener ackListener;
        // 发布操作的ActionListener。
        private  ActionListener<Void> publishListener;
        // 发布上下文。
        private  PublicationTransportHandler.PublicationContext publicationContext;
        // 计划的可取消任务。
        private  ThreadPool scheduledCancellable;

        // 存储在当前节点接受自身状态之前收到的其他节点的加入请求。
        private  List<Join> receivedJoins = new ArrayList<>();
        // 标记是否已处理收到的加入请求。
        private boolean receivedJoinsProcessed;

    }
}
