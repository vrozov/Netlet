/**
 * Copyright (C) 2016 DataTorrent, Inc.
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

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOption;
import java.net.SocketOptions;
import java.nio.channels.NetworkChannel;

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;

import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.getOption;

@SuppressWarnings("Since15")
public class DelayedOperation
{

  public static DelayedOperation getDelayedOperation(final Socket socket, int socketOption)
  {
    return new SocketDelayedOperation(socket, socketOption);
  }

  public static DelayedOperation[] getDelayedOperations(final Socket socket, int... socketOptions)
  {
    final DelayedOperation[] delayedOperations = new DelayedOperation[socketOptions.length];
    for (int i = 0; i < delayedOperations.length; i++) {
      delayedOperations[i] = getDelayedOperation(socket, socketOptions[i]);
    }
    return delayedOperations;
  }

  public static DelayedOperation getDelayedOperation(final ServerSocket serverSocket, int socketOption)
  {
    return new ServerSocketDelayedOperation(serverSocket, socketOption);
  }

  public static DelayedOperation[] getDelayedOperations(final ServerSocket serverSocket, int... socketOptions)
  {
    final DelayedOperation[] delayedOperations = new DelayedOperation[socketOptions.length];
    for (int i = 0; i < delayedOperations.length; i++) {
      delayedOperations[i] = getDelayedOperation(serverSocket, socketOptions[i]);
    }
    return delayedOperations;
  }

  public static DelayedOperation getDelayedOperation(final NetworkChannel channel, SocketOption<?> name)
  {
    return new NetworkChannelDelayedOperation(channel, name);
  }

  public static DelayedOperation[] getDelayedOperations(final NetworkChannel channel, SocketOption<?>... names)
  {
    final DelayedOperation[] delayedOperations = new DelayedOperation[names.length];
    for (int i = 0; i < delayedOperations.length; i++) {
      delayedOperations[i] = getDelayedOperation(channel, names[i]);
    }
    return delayedOperations;
  }

  public static DelayedOperation getDelayedOperation(final ChannelConfig config, ChannelOption<?> name)
  {
    return new ChannelConfigDelayedOperation(config, name);
  }

  public static DelayedOperation[] getDelayedOperations(final SocketChannel channel, ChannelOption<?>... names)
  {
    ChannelConfig config = channel.config();
    final DelayedOperation[] delayedOperations = new DelayedOperation[names.length];
    for (int i = 0; i < names.length; i++) {
      delayedOperations[i] = getDelayedOperation(config, names[i]);
    }
    return delayedOperations;
  }

  private static class SocketDelayedOperation extends DelayedOperation
  {
    private final Socket socket;
    private final int socketOption;

    SocketDelayedOperation(final Socket socket, final int socketOption)
    {
      this.socket = socket;
      this.socketOption = socketOption;
    }

    @Override
    public String toString()
    {
      try {
        switch (socketOption) {
          case SocketOptions.TCP_NODELAY:
            return Boolean.toString(socket.getTcpNoDelay());
          case SocketOptions.SO_KEEPALIVE:
            return Boolean.toString(socket.getKeepAlive());
          case SocketOptions.SO_SNDBUF:
            return Integer.toString(socket.getSendBufferSize());
          case SocketOptions.SO_RCVBUF:
            return Integer.toString(socket.getReceiveBufferSize());
          default:
            throw new RuntimeException("Not a valid option " + socketOption);

        }
      } catch (SocketException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class ServerSocketDelayedOperation extends DelayedOperation
  {
    private final ServerSocket serverSocket;
    private final int socketOption;

    ServerSocketDelayedOperation(final ServerSocket serverSocket, final int socketOption)
    {
      this.serverSocket = serverSocket;
      this.socketOption = socketOption;
    }

    @Override
    public String toString()
    {
      try {
        switch (socketOption) {
          case SocketOptions.SO_RCVBUF:
            return Integer.toString(serverSocket.getReceiveBufferSize());
          default:
            throw new RuntimeException("Not a valid option " + socketOption);

        }
      } catch (SocketException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class NetworkChannelDelayedOperation extends DelayedOperation
  {
    private final NetworkChannel channel;
    private final SocketOption<?> name;

    NetworkChannelDelayedOperation(final NetworkChannel channel, SocketOption<?> name)
    {
      this.channel = channel;
      this.name = name;
    }

    @Override
    public String toString()
    {
      return getOption(channel, name).toString();
    }
  }

  private static class ChannelConfigDelayedOperation extends DelayedOperation
  {
    private final ChannelConfig config;
    private final ChannelOption<?> name;

    ChannelConfigDelayedOperation(final ChannelConfig config, ChannelOption<?> name)
    {
      this.config = config;
      this.name = name;
    }

    @Override
    public String toString()
    {
      return config.getOption(name).toString();
    }
  }
}
