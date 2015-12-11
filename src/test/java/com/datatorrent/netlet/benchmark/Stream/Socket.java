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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;
import com.datatorrent.netlet.benchmark.util.Counter;

import static com.datatorrent.netlet.benchmark.util.DelayedOperation.getDelayedOperation;
import static com.datatorrent.netlet.benchmark.util.DelayedOperation.getDelayedOperations;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setKeepAlive;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setReceiveBufferSize;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setSendBufferSize;
import static com.datatorrent.netlet.benchmark.util.SocketOptionsHelper.setTcpNoDelay;
import static java.net.SocketOptions.SO_KEEPALIVE;
import static java.net.SocketOptions.SO_RCVBUF;
import static java.net.SocketOptions.SO_SNDBUF;
import static java.net.SocketOptions.TCP_NODELAY;

public class Socket
{
  public static class Input
  {
    private static final Logger logger = LoggerFactory.getLogger(Input.class);

    private static void benchmark()
    {
      ServerSocket serverSocket = null;
      try {
        serverSocket = new ServerSocket();
        logger.debug("server socket {}", serverSocket);
        setReceiveBufferSize(serverSocket, BenchmarkConfiguration.SO_RCVBUF);
        logger.debug("SO_RCVBUF {}", getDelayedOperation(serverSocket, SO_RCVBUF));
        serverSocket.bind(new InetSocketAddress(BenchmarkConfiguration.port), 128);

        while (true) {
          final java.net.Socket socket = serverSocket.accept();
          logger.debug("socket {}", socket);
          new Thread()
          {
            @Override
            public void run()
            {
              final byte[] byteBuffer = new byte[BenchmarkConfiguration.receiveBufferSize];
              try {
                socket.shutdownOutput();
                setKeepAlive(socket, BenchmarkConfiguration.SO_KEEPALIVE);
                setTcpNoDelay(socket, BenchmarkConfiguration.TCP_NODELAY);
                logger.debug("SO_RCVBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
                    getDelayedOperations(socket, SO_RCVBUF, SO_KEEPALIVE, TCP_NODELAY));
                final InputStream in = socket.getInputStream();
                Counter counter = new Counter().init();
                while (counter.count(in.read(byteBuffer)) != -1) {
                }
              } catch (IOException e) {
                logger.error("socket {}", socket, e);
              } finally {
                try {
                  logger.debug("closing socket {}", socket);
                  socket.close();
                } catch (IOException e) {
                  logger.error("socket {}", socket, e);
                }
              }
            }
          }.start();
        }
      } catch (IOException e) {
        logger.error("server socket {}", serverSocket, e);
      } finally {
        if (serverSocket != null) {
          try {
            serverSocket.close();
          } catch (IOException e) {
            logger.error("server socket {}", serverSocket, e);
          }
        }
      }
    }
  }

  public static class Output
  {
    private static final Logger logger = LoggerFactory.getLogger(Output.class);

    private static void benchmark(final String host)
    {
      java.net.Socket socket = null;
      try {
        socket = new java.net.Socket();
        logger.debug("socket {}", socket);
        setSendBufferSize(socket, BenchmarkConfiguration.SO_SNDBUF);
        setKeepAlive(socket, BenchmarkConfiguration.SO_KEEPALIVE);
        setTcpNoDelay(socket, BenchmarkConfiguration.TCP_NODELAY);
        socket.connect(new InetSocketAddress(host, BenchmarkConfiguration.port));
        logger.debug("SO_SNDBUF {}, SO_KEEPALIVE {}, TCP_NODELAY {}",
            getDelayedOperations(socket, SO_SNDBUF, SO_KEEPALIVE, TCP_NODELAY));
        socket.shutdownInput();
        OutputStream out = socket.getOutputStream();
        byte[] byteBuffer = new byte[BenchmarkConfiguration.sendBufferSize];
        Arrays.fill(byteBuffer, (byte)0xFF);
        Counter counter = new Counter().init();
        for (int i = 0; i < BenchmarkConfiguration.messageCount; i++) {
          out.write(byteBuffer);
          counter.count(byteBuffer.length);
        }
      } catch (IOException e) {
        logger.error("socket {}", socket, e);
      } finally {
        if (socket != null) {
          try {
            logger.debug("closing socket {}", socket);
            socket.close();
          } catch (IOException e) {
            logger.error("socket {}", socket, e);
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
