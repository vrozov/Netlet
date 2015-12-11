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
package com.datatorrent.netlet.benchmark.netty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;
import com.datatorrent.netlet.benchmark.util.BenchmarkResults;

/**
 * <p>Netty Coral Block based Benchmark Test Client</p>
 * see: <a href="http://www.coralblocks.com/index.php/2014/04/coralreactor-vs-netty-performance-comparison">http://www.coralblocks.com/index.php/2014/04/coralreactor-vs-netty-performance-comparison</a>,
 * <a href="http://stackoverflow.com/questions/23839437/what-are-the-netty-alternatives-for-high-performance-networking">http://stackoverflow.com/questions/23839437/what-are-the-netty-alternatives-for-high-performance-networking</a>,
 * <a href="http://www.coralblocks.com/NettyBench.zip">http://www.coralblocks.com/NettyBench.zip</a> and
 * <a href="https://groups.google.com/forum/#!topic/mechanical-sympathy/fhbyMnnxmaA">https://groups.google.com/forum/#!topic/mechanical-sympathy/fhbyMnnxmaA</a>
 * <p>run: <code>mvn test -Dbenchmark=netty.client</code></p>
 * <p>results=Iterations: 1000000 | Avg Time: 28.155 micros | Min Time: 10.0 micros | Max Time: 163.0 micros | 75% Time: 29.0 micros | 90% Time: 31.0 micros | 99% Time: 43.0 micros | 99.9% Time: 67.0 micros | 99.99% Time: 88.0 micros | 99.999% Time: 116.0 micros</p>
 */
public class BenchmarkTcpClient extends ChannelInboundHandlerAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(BenchmarkTcpClient.class);

  private int count = 0;
  private long start;
  private boolean warmingUp = false;
  private boolean benchmarking = false;
  private long timestamp;
  private ByteBuf sendByteBuf;
  private final BenchmarkResults benchmarkResults = new BenchmarkResults(BenchmarkConfiguration.messageCount);

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
  {
    logger.error("", cause);
    ctx.close();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx)
  {
    logger.info("Connected. Sending the first message.");
    start = System.currentTimeMillis();
    warmingUp = true;
    benchmarking = false;
    count = 0;
    sendByteBuf = ctx.alloc().ioBuffer(BenchmarkConfiguration.messageSize);
    send(-1, ctx);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx)
  {
    logger.info("Disconnected. Overall test time: {} millis", System.currentTimeMillis() - start);
    benchmarkResults.printResults(System.out);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
  {
    ByteBuf byteBuf = (ByteBuf)msg;
    long timestamp = byteBuf.getLong(0);
    byteBuf.clear().release();

    if (timestamp < -2) {
      logger.error("Received bad timestamp {}", timestamp);
      ctx.close();
      return;
    } else if (timestamp != this.timestamp) {
      logger.error("Received bad timestamp {}. Sent timestamp {}", timestamp, this.timestamp);
      ctx.close();
      return;
    } else if (timestamp > 0) {
      benchmarkResults.addResult(System.nanoTime() - timestamp);
    }
    send(ctx);
  }

  private void send(final long timestamp, final ChannelHandlerContext ctx)
  {
    this.timestamp = timestamp; // save to check echo msg...
    sendByteBuf.writeLong(timestamp);
    for (int i = 0; i < BenchmarkConfiguration.messageSize - 8; i++) {
      sendByteBuf.writeByte((byte)'x');
    }
    sendByteBuf.retain();
    try {
      ctx.writeAndFlush(sendByteBuf, ctx.voidPromise()).await();
    } catch (InterruptedException e) {
      logger.error("", e);
      ctx.close();
    }
    sendByteBuf.clear();
  }

  private void send(ChannelHandlerContext ctx)
  {
    if (warmingUp) {
      if (++count == BenchmarkConfiguration.messageCount) { // done warming up...
        logger.info("Finished warming up! Sent {} messages in {} millis", count, System.currentTimeMillis() - start);
        warmingUp = false;
        benchmarking = true;
        count = 0;
        send(System.nanoTime(), ctx); // first testing message
      } else {
        send(0, ctx);
      }
    } else if (benchmarking) {
      if (++count == BenchmarkConfiguration.messageCount) {
        logger.info("Finished sending messages! Sent {} messages.", count);
        // send the last message to tell the client we are done...
        send(-2, ctx);
        ctx.close();
      } else {
        send(System.nanoTime(), ctx);
      }
    }
  }
	
  public static void main(String[] args)
  {
    EventLoopGroup workerGroup = new NioEventLoopGroup(1);

    try {
      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.ALLOCATOR, new SingletonByteBufAllocator());
      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.handler(
          new ChannelInitializer<SocketChannel>()
          {
            @Override
            public void initChannel(SocketChannel ch) throws Exception
            {
              ch.pipeline().addLast(new BenchmarkTcpClient());
            }
          }
      );

      b.connect(args[0], BenchmarkConfiguration.port).sync().channel().closeFuture().sync();
    } catch (Exception e) {
      logger.error("", e);
    } finally {
      workerGroup.shutdownGracefully();
    }
  }
}
