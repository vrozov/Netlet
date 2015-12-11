/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.netlet.benchmark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

public class BenchmarkConfiguration
{
  private static final Logger logger = LoggerFactory.getLogger(BenchmarkConfiguration.class);

  private static final String prefix = "com.datatorrent.netlet.benchmark.";
  public static final String portName = prefix + "port";
  public static final String sleepName = prefix + "sleep";
  public static final String messageCountName = prefix + "message.count";
  public static final String messageSizeName = prefix + "message.size";
  public static final String bytesCountName = prefix + "bytes.count";
  public static final String receiveBufferSizeName = prefix + "receiveBufferSize";
  public static final String sendBufferSizeName = prefix + "sendBufferSize";
  public static final String socketReceiveBufferSizeName = prefix + "SO_RCVBUF";
  public static final String socketSendBufferSizeName = prefix + "SO_SNDBUF";
  public static final String socketKeepAliveName = prefix + "SO_KEEPALIVE";
  public static final String tcpNoDelayName = prefix + "TCP_NODELAY";

  private static final int defaultPort = 8080;
  private static final int defaultSleep = 10;
  private static final int defaultMessageCount = 1000000;
  private static final int defaultMessageSize = 256;
  private static final long GIGABYTE = 0x40000000;
  private static final long defaultBytesCount = GIGABYTE;
  private static final int defaultReceiveBufferSize = 0x10000;
  private static final int defaultSendBufferSize = 0x10000;

  private static final CompositeConfiguration configuration = new CompositeConfiguration();

  static {
    configuration.addConfiguration(new SystemConfiguration());
    configuration.addConfiguration(new EnvironmentConfiguration());
    try {
      configuration.addConfiguration(new PropertiesConfiguration("benchmark.properties"));
    } catch (ConfigurationException e) {
      logger.warn("", e);
    }
  }

  public static final int port = getInt(portName, defaultPort);
  public static final long sleep = getInt(sleepName, defaultSleep);
  public static final int messageCount = getInt(messageCountName, defaultMessageCount);
  public static final int messageSize = getInt(messageSizeName, defaultMessageSize);
  public static final long bytesCount = configuration.getLong(bytesCountName, defaultBytesCount);
  public static final int receiveBufferSize = getInt(receiveBufferSizeName, defaultReceiveBufferSize);
  public static final int sendBufferSize = getInt(sendBufferSizeName, defaultSendBufferSize);

  public static final Integer SO_RCVBUF = configuration.getInteger(socketReceiveBufferSizeName, null);
  public static final Integer SO_SNDBUF = configuration.getInteger(socketSendBufferSizeName, null);
  public static final Boolean SO_KEEPALIVE = configuration.getBoolean(socketKeepAliveName, null);
  public static final Boolean TCP_NODELAY = configuration.getBoolean(tcpNoDelayName, null);

  private static int getInt(final String key, final int defaultValue)
  {
    return configuration.getInt(key, defaultValue);
  }

}
