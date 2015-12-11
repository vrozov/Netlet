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
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.AbstractServer;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;
import com.datatorrent.netlet.benchmark.util.Counter;

import static com.datatorrent.netlet.benchmark.util.DelayedOperation.getDelayedOperations;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setOption;

import static java.lang.Thread.sleep;
import static java.net.StandardSocketOptions.SO_KEEPALIVE;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

@SuppressWarnings("Since15")
public class Netlet
{
  private static final Logger logger = LoggerFactory.getLogger(Netlet.class);

  private static class Input
  {
    private static void benchmark()
    {
      try {
        final DefaultEventLoop defaultEventLoop = DefaultEventLoop.createEventLoop("eventLoop");
        final Thread eventLoopThread = defaultEventLoop.start();
        defaultEventLoop.start(null, BenchmarkConfiguration.port,
            new AbstractServer()
            {
              @Override
              public ClientListener getClientConnection(final SocketChannel channel,
                  final ServerSocketChannel serverChannel)
              {
                logger.debug("channel {}" + channel);
                try {
                  channel.shutdownOutput();
                } catch (IOException e) {
                  logger.error("channel {}", channel, e);
                  throw new RuntimeException(e);
                }
                setOption(channel, SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
                setOption(channel, TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
                logger.debug("SO_RCVBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
                    getDelayedOperations(channel, SO_RCVBUF, SO_KEEPALIVE, TCP_NODELAY));

                return new AbstractClient()
                {
                  private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BenchmarkConfiguration.receiveBufferSize);
                  private final Counter counter = new Counter();

                  @Override
                  public void read(int read)
                  {
                    if (counter.count(read) > 0) {
                      byteBuffer.clear();
                    }
                  }

                  @Override
                  public void connected()
                  {
                    super.connected();
                    counter.init();
                  }

                  @Override
                  public ByteBuffer buffer()
                  {
                    return byteBuffer;
                  }

                  @Override
                  public void handleException(Exception e, EventLoop eventLoop)
                  {
                    super.handleException(e, eventLoop);
                    eventLoop.disconnect(this);
                  }
                };
              }

              @Override
              public void handleException(Exception e, EventLoop eventLoop)
              {
                super.handleException(e, eventLoop);
                eventLoop.stop(this);
                defaultEventLoop.stop();
              }

              @Override
              public void registered(SelectionKey key)
              {
                setOption((ServerSocketChannel)key.channel(), SO_RCVBUF, BenchmarkConfiguration.SO_RCVBUF);
                super.registered(key);
              }
            }
        );
        eventLoopThread.join();
      } catch (IOException e) {
        logger.error("", e);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
    }
  }

  private static class Output
  {
    private static volatile boolean isAlive = true;

    private static void benchmark(final String host)
    {
      try {
        final DefaultEventLoop defaultEventLoop = DefaultEventLoop.createEventLoop("EventLoop");
        defaultEventLoop.start();
        final Counter counter = new Counter();
        final AbstractClient abstractClient = new AbstractClient(ByteBuffer.allocateDirect(BenchmarkConfiguration.sendBufferSize), 1024)
        {
          private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BenchmarkConfiguration.receiveBufferSize);

          @Override
          public void connected()
          {
            super.connected();
            final SocketChannel channel = (SocketChannel)key.channel();
            logger.debug("channel {}", channel);
            try {
              channel.shutdownInput();
            } catch (IOException e) {
              logger.error("channel {}", channel, e);
              throw new RuntimeException(e);
            }
            setOption(channel, SO_SNDBUF, BenchmarkConfiguration.SO_SNDBUF);
            setOption(channel, SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
            setOption(channel, TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
            logger.debug("SO_SNDBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
                getDelayedOperations(channel, SO_SNDBUF, SO_KEEPALIVE, TCP_NODELAY));
            counter.init();
          }

          @Override
          public void disconnected()
          {
            logger.debug("Closed channel {}", key.channel());
            super.disconnected();
            isAlive = false;
          }

          @Override
          public ByteBuffer buffer()
          {
            return byteBuffer;
          }

          @Override
          public void read(int len)
          {
            throw new RuntimeException();
          }

          @Override
          public void handleException(Exception e, EventLoop eventLoop)
          {
            isAlive = false;
            super.handleException(e, eventLoop);
          }
        };
        defaultEventLoop.connect(new InetSocketAddress(host, BenchmarkConfiguration.port), abstractClient);
        int i = 0;
        long sleepMillis = 0;
        byte[] bytes = null;
        while (i < BenchmarkConfiguration.messageCount && isAlive) {
          if (bytes == null) {
            bytes = new byte[BenchmarkConfiguration.messageSize];
          }
          Arrays.fill(bytes, (byte)0xFF);
          if (abstractClient.send(bytes)) {
            i++;
            counter.count(bytes.length);
            bytes = null;
            sleepMillis = 0;
          } else {
            try {
              sleep(sleepMillis);
              sleepMillis = Math.max(BenchmarkConfiguration.sleep, sleepMillis + 1);
            } catch (InterruptedException e) {
              logger.error("", e);
              throw new RuntimeException(e);
            }
          }
        }
      } catch (IOException e) {
        logger.error("", e);
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
