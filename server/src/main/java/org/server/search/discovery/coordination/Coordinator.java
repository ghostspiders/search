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

import org.apache.lucene.util.SetOnce;
import org.server.search.SearchException;
import org.server.search.action.ActionListener;
import org.server.search.client.transport.TransportClient;
import org.server.search.cluster.ClusterChangedEvent;
import org.server.search.cluster.ClusterState;
import org.server.search.cluster.DefaultClusterService;
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
            joinCallback.onFailure(new IllegalStateException("cannot join node with [" + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey() +
                    "] set to [" + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE  + "] discovery"));
            return;
        }

        // 连接到请求节点。
        transportService.connectToNode(joinRequest.getSourceNode());

        // 获取当前集群状态，用于加入验证。
        final ClusterState stateForJoinValidation = getStateForMasterService();

        // 如果本地节点已被选举为主节点，则进行一系列验证。
        if (stateForJoinValidation.nodes().isLocalNodeElectedMaster()) {
            // 遍历onJoinValidators并接受验证。
            onJoinValidators.forEach(a -> a.accept(joinRequest.getSourceNode(), stateForJoinValidation));
            // 检查集群状态是否有全局未恢复的块。
            if (stateForJoinValidation.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false) {
                // 确保请求节点的版本至少与集群中最小节点版本一致。
                // 这在多个地方执行，这里主要是为了尽快失败。
                JoinTaskExecutor.ensureMajorVersionBarrier(joinRequest.getSourceNode().getVersion(),
                        stateForJoinValidation.getNodes().getMinNodeVersion());
            }
            // 发送验证加入请求。
            sendValidateJoinRequest(stateForJoinValidation, joinRequest, joinCallback);

        } else {
            // 如果本地节点未被选举为主节点，则处理加入请求。
            processJoinRequest(joinRequest, joinCallback);
        }
    }
}
