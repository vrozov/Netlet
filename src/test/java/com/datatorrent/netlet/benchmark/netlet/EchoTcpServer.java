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
package com.datatorrent.netlet.benchmark.netlet;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.AbstractClient;
import com.datatorrent.netlet.AbstractServer;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;
import com.datatorrent.netlet.benchmark.util.BenchmarkResults;

import static java.lang.Thread.sleep;

/**
 * <p>Netlet Coral Block based Benchmark Echo Test Server</p>
 * see: <a href="http://www.coralblocks.com/index.php/2014/04/coralreactor-vs-netty-performance-comparison">http://www.coralblocks.com/index.php/2014/04/coralreactor-vs-netty-performance-comparison</a>,
 * <a href="http://stackoverflow.com/questions/23839437/what-are-the-netty-alternatives-for-high-performance-networking">http://stackoverflow.com/questions/23839437/what-are-the-netty-alternatives-for-high-performance-networking</a>,
 * <a href="http://www.coralblocks.com/NettyBench.zip">http://www.coralblocks.com/NettyBench.zip</a> and
 * <a href="https://groups.google.com/forum/#!topic/mechanical-sympathy/fhbyMnnxmaA">https://groups.google.com/forum/#!topic/mechanical-sympathy/fhbyMnnxmaA</a>
 * <p>run: <code>mvn test -Dbenchmark=netlet.server</code></p>
 * <p>results=Iterations: 1000000 | Avg Time: 14.274 micros | Min Time: 3.0 micros | Max Time: 125.0 micros | 75% Time: 14.0 micros | 90% Time: 17.0 micros | 99% Time: 24.0 micros | 99.9% Time: 40.0 micros | 99.99% Time: 71.0 micros | 99.999% Time: 81.0 micros</p>
 */
public class EchoTcpServer extends AbstractServer
{
  private static final Logger logger = LoggerFactory.getLogger(EchoTcpServer.class);

  private EchoTcpServer(final String host, final int port) throws IOException, InterruptedException
  {
    super();
    final DefaultEventLoop defaultEventLoop = DefaultEventLoop.createEventLoop("eventLoop");
    final Thread eventLoopThread = defaultEventLoop.start();
    defaultEventLoop.start(host, port, this);
    eventLoopThread.join();
  }

  @Override
  public void handleException(Exception e, EventLoop eventLoop)
  {
    logger.error("", e);
    eventLoop.stop(this);
    ((DefaultEventLoop)eventLoop).stop();
  }

  @Override
  public ClientListener getClientConnection(SocketChannel client, final ServerSocketChannel server)
  {
    logger.info("{} connected.", client);
    return new AbstractClient(BenchmarkConfiguration.sendBufferSize, 1)
    {
      private final ByteBuffer byteBuffer = ByteBuffer.allocate(BenchmarkConfiguration.messageSize);
      private final BenchmarkResults benchmarkResults = new BenchmarkResults(BenchmarkConfiguration.messageCount);
      private long start;

      @Override
      public ByteBuffer buffer()
      {
        return byteBuffer;
      }

      @Override
      public void handleException(Exception e, EventLoop eventLoop)
      {
        logger.error("", e);
        eventLoop.stop(EchoTcpServer.this);
      }

      @Override
      public void unregistered(SelectionKey key)
      {
        super.unregistered(key);
        benchmarkResults.printResults(System.out);
      }

      @Override
      public void read(int len)
      {
        if (byteBuffer.position() < byteBuffer.capacity()) {
          return;
        }

        byteBuffer.flip();
        long timestamp = byteBuffer.getLong();

        if (timestamp > 0) {
          benchmarkResults.addResult(System.nanoTime() - timestamp);
        } else if (timestamp == -1) {
          start = System.currentTimeMillis();
          logger.info("Received the first message.");
        } else if (timestamp == -2) {
          logger.info("Finished receiving messages! Overall test time: {} millis", System.currentTimeMillis() - start);
          return;
        } else if (timestamp < 0) {
          logger.error("Received bad timestamp {}", timestamp);
          return;
        }

        try {
          while (!send(byteBuffer.array())) {
            sleep(5);
          }
          write();
          byteBuffer.clear();
        } catch (Exception ie) {
          throw new RuntimeException(ie);
        }
      }
    };
  }

  public static void main(String[] args)
  {
    try {
      new EchoTcpServer(null, BenchmarkConfiguration.port);
    } catch (IOException e) {
      logger.error("", e);
    } catch (InterruptedException e) {
      logger.error("", e);
    }
  }
}
