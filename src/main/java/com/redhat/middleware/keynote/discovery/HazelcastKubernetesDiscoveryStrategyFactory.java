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

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.*;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;


/**
 * Discovery SPI implementation of Hazelcast to support Kubernetes-based discovery. This implementation
 * is very close to the "official" hazelcast plugin, when remove some limitations such as the Kubernetes
 * master url, and does not rely on DNS and is <strong>specific</strong> to vert.x.
 * <p>
 * It works as follows:
 * <p>
 * * when the discovery strategy is instantiated, it resolved the known nodes
 * * known nodes are found by doing a Kubernetes query: it looks for all pods with a specific label
 * (`vertx-cluster`=`true`). The query is made on the label name and label value.
 * <p>
 * By default it uses the port 5701 to connected. If the endpoints defines the
 * <code>hazelcast-service-port</code>, the indicated value is used.
 * <p>
 * Can be configured:
 * <p>
 * * "namespace" : the kubernetes namespace / project, by default it tries to read the
 * {@code OPENSHIFT_BUILD_NAMESPACE} environment variable. If not defined, it uses "default"
 * * "pod-label-name" : the name of the label to look for, "vertx-cluster" by default.
 * * "pod-label-name" : the name of the label to look for, "true" by default.
 * * "kubernetes-master" : the url of the Kubernetes master, by default it builds the url from the
 * {@code KUBERNETES_SERVICE_HOST} and {@code KUBERNETES_SERVICE_PORT}.
 * * "kubernetes-token" : the bearer token to use to connect to Kubernetes, it uses the content of the
 * {@code /var/run/secrets/kubernetes.io/serviceaccount/token} file by default.
 * <p>
 * If you use Openshift and follow the convention, you just need to configure the
 * {@code namespace}.
 *
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class HazelcastKubernetesDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

  private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

  static final PropertyDefinition POD_LABEL_NAME = property("pod-label-name", STRING);

  static final PropertyDefinition POD_LABEL_VALUE = property("pod-label-value", STRING);

  static final PropertyDefinition NAMESPACE = property("namespace", STRING);

  static final PropertyDefinition KUBERNETES_MASTER = property("kubernetes-master", STRING);

  static final PropertyDefinition KUBERNETES_TOKEN = property("kubernetes-token", STRING);

  static {
    List<PropertyDefinition> propertyDefinitions = new ArrayList<>();
    propertyDefinitions.add(POD_LABEL_NAME);
    propertyDefinitions.add(POD_LABEL_VALUE);
    propertyDefinitions.add(NAMESPACE);
    propertyDefinitions.add(KUBERNETES_MASTER);
    propertyDefinitions.add(KUBERNETES_TOKEN);
    PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(propertyDefinitions);
  }

  /**
   * @return the {@link HazelcastKubernetesDiscoveryStrategy} class.
   */
  @Override
  public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
    return HazelcastKubernetesDiscoveryStrategy.class;
  }

  /**
   * Creates a new instance of the strategy.
   *
   * @param discovery     the discovery node
   * @param logger        the logger
   * @param configuration the configuration
   * @return the created instance
   */
  @Override
  public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discovery, ILogger logger,
                                                Map<String, Comparable> configuration) {
    return new HazelcastKubernetesDiscoveryStrategy(logger, configuration);
  }

  /**
   * @return the configuration properties.
   */
  @Override
  public Collection<PropertyDefinition> getConfigurationProperties() {
    return PROPERTY_DEFINITIONS;
  }

  private static PropertyDefinition property(String key, TypeConverter typeConverter) {
    return new SimplePropertyDefinition(key, true, typeConverter);
  }
}