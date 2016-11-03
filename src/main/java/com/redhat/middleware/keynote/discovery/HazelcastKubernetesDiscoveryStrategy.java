/*
 * Copyright (c) 2011-2016 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package com.redhat.middleware.keynote.discovery;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.vertx.servicediscovery.kubernetes.KubernetesUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Discovery SPI implementation of Hazelcast to support Kubernetes-based discovery. This implementation
 * is very close to the "official" hazelcast plugin, when remove some limitations such as the Kubernetes
 * master url, and does not rely on DNS (but on service lookup), and is <strong>specific</strong> to vert.x.
 * <p>
 * It works as follows:
 * <p>
 * * when the discovery strategy is instantiated, it resolved the known nodes
 * * known nodes are found by doing a Kubernetes query: it looks for all endpoints (~services) with a specific label
 * (`vertx-cluster`=`true`). The query is made on the label name and label value.
 * <p>
 * By default it uses the port 5701 to connected. If the endpoints defines the
 * <code>hazelcast-service-port</code>, the indicated value is used.
 * <p>
 * Can be configured:
 * <p>
 * * "namespace" : the kubernetes namespace / project, by default it tries to read the
 * {@code OPENSHIFT_BUILD_NAMESPACE} environment variable. If not defined, it uses "default"
 * * "service-label-name" : the name of the label to look for, "vertx-cluster" by default.
 * * "service-label-name" : the name of the label to look for, "true" by default.
 * * "kubernetes-master" : the url of the Kubernetes master, by default it builds the url from the
 * {@code KUBERNETES_SERVICE_HOST} and {@code KUBERNETES_SERVICE_PORT}.
 * * "kubernetes-token" : the bearer token to use to connect to Kubernetes, it uses the content of the
 * {@code /var/run/secrets/kubernetes.io/serviceaccount/token} file by default.
 * <p>
 * If you use Openshift and follow the service name convention, you just need to configure the
 * {@code namespace}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
class HazelcastKubernetesDiscoveryStrategy extends AbstractDiscoveryStrategy {

  public static final String KUBERNETES_SYSTEM_PREFIX = "hazelcast.kubernetes.";
  private static final String HAZELCAST_SERVICE_PORT = "hazelcast-service-port";

  private final String namespace;
  private final String label;
  private final String labelValue;

  private final DefaultKubernetesClient client;

  /**
   * Creates a new instance of {@link HazelcastKubernetesDiscoveryStrategy}. This constructor is
   * called by {@link HazelcastKubernetesDiscoveryStrategyFactory}.
   *
   * @param logger        the logger
   * @param configuration the configuration
   */
  HazelcastKubernetesDiscoveryStrategy(ILogger logger, Map<String, Comparable> configuration) {
    super(logger, configuration);

    this.namespace = getOrDefault(KUBERNETES_SYSTEM_PREFIX,
        HazelcastKubernetesDiscoveryStrategyFactory.NAMESPACE, getNamespaceOrDefault());

    String master = getOrDefault(KUBERNETES_SYSTEM_PREFIX,
        HazelcastKubernetesDiscoveryStrategyFactory.KUBERNETES_MASTER,
        KubernetesUtils.getDefaultKubernetesMasterUrl());

    this.label = getOrDefault(KUBERNETES_SYSTEM_PREFIX,
        HazelcastKubernetesDiscoveryStrategyFactory.POD_LABEL_NAME,
        getPodLabelName());

    this.labelValue = getOrDefault(KUBERNETES_SYSTEM_PREFIX,
        HazelcastKubernetesDiscoveryStrategyFactory.POD_LABEL_VALUE,
        getPodLabelValue());

    String token = getOrDefault(KUBERNETES_SYSTEM_PREFIX,
        HazelcastKubernetesDiscoveryStrategyFactory.KUBERNETES_TOKEN, null);

    if (token == null) {
      token = KubernetesUtils.getTokenFromFile();
    }

    Config config = new ConfigBuilder()
        .withOauthToken(token)
        .withMasterUrl(master)
        .withTrustCerts(true)
        .build();
    client = new DefaultKubernetesClient(config);
  }

  private String getPodLabelName() {
    final String labelName = System.getenv("POD_LABEL_NAME");
    return (labelName == null ? "vertx-cluster" : labelName);
  }

  private String getPodLabelValue() {
    final String labelName = System.getenv("POD_LABEL_VALUE");
    return (labelName == null ? "true" : labelName);
  }

  private String getNamespaceOrDefault() {
    // Kubernetes with Fabric8 build
    String namespace = System.getenv("KUBERNETES_NAMESPACE");
    if (namespace == null) {
      // oc / docker build
      namespace = System.getenv("OPENSHIFT_BUILD_NAMESPACE");
      if (namespace == null) {
        namespace = "default";
      }
    }
    return namespace;
  }

  /**
   * Just starts the Kubernetes discovery.
   */
  @Override
  public void start() {
    getLogger().info("Starting the Kubernetes-based discovery");
    getLogger().info("Looking for pods from namespace " + namespace + " with label '" + label + "' set to '" +
        labelValue + "'.");
  }

  /**
   * Discovers the nodes. It queries all endpoints (services) with a label `label` set to `labelValue`. By default,
   * it's `vertx-cluster=true`.
   *
   * @return the list of discovery nodes
   */
  @Override
  public Iterable<DiscoveryNode> discoverNodes() {
    PodList list = client.pods().inNamespace(namespace).withLabel(label, labelValue).list();

    if (list == null || list.getItems() == null || list.getItems().isEmpty()) {
      getLogger().info("No pods for service " + " in namespace " + namespace + " with label: `vertx-cluster=true`");
      return Collections.emptyList();
    }

    List<DiscoveryNode> nodes = new ArrayList<>();
    List<Pod> podList = list.getItems();
    for (Pod pod : podList) {
      final SimpleDiscoveryNode node = getSimpleDiscoveryNode(pod);
      if (node != null) {
          nodes.add(node);
      }
    }

    getLogger().info("Resolved nodes: " + nodes.stream().map(DiscoveryNode::getPublicAddress).collect(Collectors.toList()));

    return nodes;
  }

  private SimpleDiscoveryNode getSimpleDiscoveryNode(Pod pod) {
    Map<String, Object> properties = pod.getAdditionalProperties();
    String ip = pod.getStatus().getPodIP();
    if (ip != null) {
        InetAddress inetAddress = extractAddress(ip);
        int port = getServicePort(properties);
        getLogger().info("Resolved node: " +"port: " + port + " inetAddress: " + inetAddress.toString() + " ip:" + ip + " properties: " + properties);
        Address address = new Address(inetAddress, port);
        return new SimpleDiscoveryNode(address, properties);
    } else {
        return null;
    }
  }


  @Override
  public void destroy() {
    // Do nothing.
  }

  private InetAddress extractAddress(String address) {
    if (address == null) {
      return null;
    }

    try {
      return InetAddress.getByName(address);
    } catch (UnknownHostException e) {
      getLogger().warning("Address '" + address + "' could not be resolved");
    }
    return null;
  }

  private int getServicePort(Map<String, Object> properties) {
    int port = NetworkConfig.DEFAULT_PORT;
    if (properties != null) {
      String servicePort = (String) properties.get(HAZELCAST_SERVICE_PORT);
      if (servicePort != null) {
        port = Integer.parseInt(servicePort);
      }
    }
    return port;
  }
}