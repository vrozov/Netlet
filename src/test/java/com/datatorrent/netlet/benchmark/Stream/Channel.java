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
package com.datatorrent.netlet.benchmark.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;
import com.datatorrent.netlet.benchmark.util.Counter;

import static com.datatorrent.netlet.benchmark.util.DelayedOperation.getDelayedOperation;
import static com.datatorrent.netlet.benchmark.util.DelayedOperation.getDelayedOperations;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setOption;

import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

@SuppressWarnings("Since15")
public class Channel
{
  private static final Logger logger = LoggerFactory.getLogger(Channel.class);

  private static class Input
  {
    private static void benchmark()
    {
      ServerSocketChannel serverChannel = null;
      try {
        serverChannel = ServerSocketChannel.open();
        logger.debug("server channel {}", serverChannel);
        setOption(serverChannel, SO_RCVBUF, BenchmarkConfiguration.SO_RCVBUF);
        logger.debug("SO_RCVBUF {}", getDelayedOperation(serverChannel, SO_RCVBUF));
        serverChannel.bind(new InetSocketAddress(BenchmarkConfiguration.port), 128);

        while (true) {
          final SocketChannel channel = serverChannel.accept();
          logger.debug("channel {}", channel);
          new Thread()
          {
            @Override
            public void run()
            {
              try {
                channel.shutdownOutput();
                setOption(channel, SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
                setOption(channel, TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
                logger.debug("SO_RCVBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
                    getDelayedOperations(channel, SO_RCVBUF, SO_KEEPALIVE, TCP_NODELAY));

                Counter counter = new Counter().init();
                final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BenchmarkConfiguration.receiveBufferSize);
                while (counter.count(channel.read(byteBuffer)) != -1) {
                  byteBuffer.clear();
                }
              } catch (IOException e) {
                logger.error("channel {}", channel, e);
              } finally {
                try {
                  logger.debug("Closing channel {}", channel);
                  channel.close();
                } catch (IOException e) {
                  logger.error("channel {}", channel, e);
                }
              }
            }
          }.start();
        }
      } catch (IOException e) {
        logger.error("server channel {}", serverChannel, e);
      } finally {
        if (serverChannel != null) {
          try {
            logger.debug("Closing server channel {}", serverChannel);
            serverChannel.close();
          } catch (IOException e) {
            logger.error("server channel {}", serverChannel, e);
          }
        }
      }
    }
  }

  private static class Output
  {
    private static ByteBuffer getByteBuffer()
    {
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BenchmarkConfiguration.sendBufferSize);
      if (byteBuffer.hasArray()) {
        Arrays.fill(byteBuffer.array(), (byte)0xFF);
      } else {
        final byte[] bytes = new byte[byteBuffer.capacity()];
        Arrays.fill(bytes, (byte)0xFF);
        byteBuffer.put(bytes);
      }
      byteBuffer.flip();
      byteBuffer.mark();
      return byteBuffer;
    }

    private static void benchmark(final String host)
    {
      SocketChannel channel = null;
      try {
        channel = SocketChannel.open();
        setOption(channel, SO_SNDBUF, BenchmarkConfiguration.SO_SNDBUF);
        setOption(channel, SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
        setOption(channel, TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
        channel.connect(new InetSocketAddress(host, BenchmarkConfiguration.port));
        channel.shutdownInput();
        logger.debug("channel {}", channel);
        logger.debug("SO_SNDBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
            getDelayedOperations(channel, SO_SNDBUF, SO_KEEPALIVE, TCP_NODELAY));

        final ByteBuffer byteBuffer = getByteBuffer();
        Counter counter = new Counter().init();
        for (int i = 0; i < BenchmarkConfiguration.messageCount; i++) {
          if (counter.count(channel.write(byteBuffer)) > 0) {
            byteBuffer.reset();
          }
        }
      } catch (IOException e) {
        logger.error("channel {}", channel, e);
      } finally {
        if (channel != null) {
          try {
            logger.debug("Closing channel {}", channel);
            channel.close();
          } catch (IOException e) {
            logger.error("", e);
          }
        }
      }
    }
  }

  public static void main(String[] args)
  {
    if (args.length == 0 || "-server".equals(args[0])) {
      Input.benchmark();
    } else if ("-client".equals(args[0])) {
      Output.benchmark(args[1]);
    } else {
      throw new IllegalArgumentException("Not a valid option " + args[0]);
    }
  }
}
