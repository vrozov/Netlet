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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Since15")
public class SocketOptionsHelper
{
  private static final Logger logger = LoggerFactory.getLogger(SocketOptionsHelper.class);

  public static <T> T getOption(final NetworkChannel channel, final SocketOption<T> name)
  {
    try {
      return channel.getOption(name);
    } catch (IOException e) {
      logger.error("channel {}, socket option name {}", channel, name, e);
      throw new RuntimeException(e);
    }
  }

  public static <T> void setOption(final NetworkChannel channel, final SocketOption<T> name, final T value)
  {
    if (value != null) {
      try {
        channel.setOption(name, value);
      } catch (IOException e) {
        logger.error("channel {}, socket option name {}, value {}", channel, name, value, e);
        throw new RuntimeException(e);
      }
    }
  }

  public static void setReceiveBufferSize(final ServerSocket socket, final Integer size)
  {
    if (size != null) {
      try {
        socket.setReceiveBufferSize(size);
      } catch (SocketException e) {
        logger.error("socket {}, size {}", socket, size, e);
        throw new RuntimeException(e);
      }
    }
  }

  public static void setSendBufferSize(final Socket socket, final Integer size)
  {
    if (size != null) {
      try {
        socket.setSendBufferSize(size);
      } catch (SocketException e) {
        logger.error("socket {}, size {}", socket, size, e);
        throw new RuntimeException(e);
      }
    }
  }

  public static void setKeepAlive(final Socket socket, final Boolean on)
  {
    if (on != null) {
      try {
        socket.setKeepAlive(on);
      } catch (SocketException e) {
        logger.error("socket {}, keepAlive {}", socket, on, e);
        throw new RuntimeException(e);
      }
    }
  }

  public static void setTcpNoDelay(final Socket socket, final Boolean on)
  {
    if (on != null) {
      try {
        socket.setTcpNoDelay(on);
      } catch (SocketException e) {
        logger.error("socket {}, tcpNoDelay {}", socket, on, e);
        throw new RuntimeException(e);
      }
    }
  }
}
