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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;

public class Netty
{
  private static final Logger logger = LoggerFactory.getLogger(Netty.class);

  public static class Input extends ChannelInboundHandlerAdapter
  {

    private int count = 0;
    private long start;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
      logger.error("{}", ctx, cause);
      ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
      final SocketChannel channel = (SocketChannel)ctx.channel();
      channel.shutdownOutput();
      if (logger.isDebugEnabled()) {
        final SocketChannelConfig config = channel.config();
        final int soRcvBuf = config.getReceiveBufferSize();
        final boolean soKeepAlive = config.isKeepAlive();
        final boolean tcpNoDelay = config.isTcpNoDelay();
        logger.debug("SO_RCVBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}", soRcvBuf, soKeepAlive, tcpNoDelay);
      }
      start = System.currentTimeMillis();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
      ByteBuf byteBuf = (ByteBuf)msg;
      int read = byteBuf.readableBytes();
      byteBuf.release();

      if (read > 0) {
        if (count + read > BenchmarkConfiguration.bytesCount) {
          long temp = System.currentTimeMillis();
          logger.debug("{} {} {}", count, read, temp - start);
          start = temp;
          count += (read - BenchmarkConfiguration.bytesCount);
        } else {
          count += read;
        }
      }
    }

    private static void benchmark(final int port, Class<? extends ServerChannel> serverSocketChannelClass)
    {
      final EventLoopGroup parentGroup;
      final EventLoopGroup childGroup;
      if (serverSocketChannelClass == OioServerSocketChannel.class) {
        parentGroup = new OioEventLoopGroup();
        childGroup = new OioEventLoopGroup();
      } else if (serverSocketChannelClass == NioServerSocketChannel.class) {
        parentGroup = new NioEventLoopGroup();
        childGroup = new NioEventLoopGroup();
      } else if (serverSocketChannelClass == EpollServerSocketChannel.class) {
        parentGroup = new EpollEventLoopGroup();
        childGroup = new EpollEventLoopGroup();
      } else {
        throw new IllegalArgumentException("Not a valid serverSocketChannelClass " + serverSocketChannelClass.getName());
      }

      try {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(parentGroup, childGroup);
        bootstrap.channel(serverSocketChannelClass);
        if (BenchmarkConfiguration.SO_RCVBUF != null) {
          bootstrap.childOption(ChannelOption.SO_RCVBUF, BenchmarkConfiguration.SO_RCVBUF);
        }
        if (BenchmarkConfiguration.SO_KEEPALIVE != null) {
          bootstrap.childOption(ChannelOption.SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
        }
        if (BenchmarkConfiguration.TCP_NODELAY != null) {
          bootstrap.childOption(ChannelOption.TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
        }
        bootstrap.childHandler(
            new ChannelInitializer<SocketChannel>()
            {
              @Override
              protected void initChannel(final SocketChannel ch) throws Exception
              {
                if (LoggerFactory.getLogger(LoggingHandler.class).isDebugEnabled()) {
                  ch.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
                }
                ch.pipeline().addLast(new Input());
              }
            }
        );
        bootstrap.bind(port).sync().channel().closeFuture().sync();
      } catch (InterruptedException e) {
        logger.debug("", e);
      } finally {
        try {
          childGroup.shutdownGracefully().sync();
          parentGroup.shutdownGracefully().sync();
        } catch (InterruptedException e) {
          logger.debug("", e);
        }
      }
    }

    public static void main(String[] args)
    {
      benchmark(args.length > 0 ? Integer.parseInt(args[0]) : 8080, NioServerSocketChannel.class);
    }
  }

  public static class Output extends ChannelInboundHandlerAdapter
  {
    private final byte[] bytes;

    private Output()
    {
      bytes = new byte[0x1FFFF];
      Arrays.fill(bytes, (byte)0xFF);
    }

    private void benchmark(final String host, final int port, Class<? extends Channel> channelClass)
    {
      final EventLoopGroup eventLoopGroup;
      if (channelClass == OioSocketChannel.class) {
        eventLoopGroup = new OioEventLoopGroup();
      } else if (channelClass == NioSocketChannel.class) {
        eventLoopGroup = new NioEventLoopGroup();
      } else if (channelClass == EpollSocketChannel.class) {
        eventLoopGroup = new EpollEventLoopGroup();
      } else {
        throw new IllegalArgumentException("Invalid channel class " + channelClass.getName());
      }

      try {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(channelClass);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, BenchmarkConfiguration.SO_KEEPALIVE);
        bootstrap.option(ChannelOption.TCP_NODELAY, BenchmarkConfiguration.TCP_NODELAY);
        bootstrap.option(ChannelOption.SO_SNDBUF, BenchmarkConfiguration.SO_SNDBUF);
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 1024 * 1024);
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 512 * 1024);
        bootstrap.handler(
            new ChannelInitializer<SocketChannel>()
            {
              @Override
              public void initChannel(SocketChannel ch) throws Exception
              {
                if (LoggerFactory.getLogger(LoggingHandler.class).isDebugEnabled()) {
                  ch.pipeline().addLast(new LoggingHandler(LogLevel.DEBUG));
                }
                ch.pipeline().addLast(Output.this);
              }
            }
        );

        Channel channel = bootstrap.connect(host, port).sync().channel();
        channel.closeFuture().sync();

      } catch (InterruptedException e) {
        logger.debug("", e);
        throw new RuntimeException(e);
      } finally {
        eventLoopGroup.shutdownGracefully();
      }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
      logger.debug("{}", ctx, cause);
      ctx.close();
      synchronized (this) {
        notify();
      }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
    {
      final Channel channel = ctx.channel();
      final ByteBufAllocator allocator = channel.alloc();
      while (channel.isWritable()) {
        ByteBuf buffer = allocator.ioBuffer(bytes.length);
        buffer.writeBytes(bytes);
        ctx.writeAndFlush(buffer, ctx.voidPromise());
      }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
      super.channelActive(ctx);

      final SocketChannel channel = (SocketChannel)ctx.channel();
      final SocketChannelConfig config = channel.config();
      logger.debug("TCP_NODELAY {}, SO_RCVBUF {}", config.isTcpNoDelay(), config.getReceiveBufferSize());
      final ByteBufAllocator allocator = channel.alloc();

      while (channel.isWritable()) {
        ByteBuf buffer = allocator.ioBuffer(bytes.length);
        buffer.writeBytes(bytes);
        ctx.writeAndFlush(buffer, ctx.voidPromise());
      }
    }

    public static void main(String[] args)
    {
      String host;
      int port;

      if (args.length > 0) {
        host = args[0];
        port = Integer.parseInt(args[1]);
      } else {
        host = "localhost";
        port = 8080;
      }
      new Output().benchmark(host, port, NioSocketChannel.class);
    }
  }
}
