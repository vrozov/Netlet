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
package com.datatorrent.netlet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jctools.queues.SpscArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

public class AbstractWriteOnlyClient extends AbstractClientListener
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractWriteOnlyClient.class);

  protected final ByteBuffer writeBuffer;
  protected final SpscArrayQueue<Slice> sendQueue;
  protected final SpscArrayQueue<Slice> freeQueue;
  protected boolean isWriteEnabled = true;
  protected Lock lock = new ReentrantLock();

  public AbstractWriteOnlyClient()
  {
    this(64 * 1024, 1024);
  }

  public AbstractWriteOnlyClient(final int writeBufferCapacity, final int sendQueueCapacity)
  {
    this(ByteBuffer.allocateDirect(writeBufferCapacity), sendQueueCapacity);
  }

  public AbstractWriteOnlyClient(final ByteBuffer writeBuffer, final int sendQueueCapacity)
  {
    this.writeBuffer = writeBuffer;
    sendQueue = new SpscArrayQueue<Slice>(sendQueueCapacity);
    freeQueue = new SpscArrayQueue<Slice>(sendQueueCapacity);
  }

  @Override
  public void connected()
  {
    super.connected();
    shutdownIO(true);
    suspendReadIfResumed();
  }

  @Override
  public final void read() throws IOException
  {
    if (suspendReadIfResumed()) {
      logger.warn("{} OP_READ should be disabled", this);
    } else {
      logger.error("{} read is not expected", this);
    }
  }

  @Override
  public void write() throws IOException
  {
    /*
     * at first when we enter this function, our buffer is in fill mode.
     */
    int remaining = writeBuffer.remaining();
    Slice slice;
    while ((slice = sendQueue.peek()) != null) {
      if (remaining < slice.length) {
        if (remaining > 0) {
          writeBuffer.put(slice.buffer, slice.offset, remaining);
          slice.offset += remaining;
          slice.length -= remaining;
        }
        if (channelWrite() == 0) {
          return;
        }
        remaining = writeBuffer.remaining();
      } else {
        writeBuffer.put(slice.buffer, slice.offset, slice.length);
        slice.buffer = null;
        remaining -= slice.length;
        freeQueue.offer(sendQueue.poll());
      }
    }
    channelWrite();
  }

  protected int channelWrite() throws IOException
  {
    if (writeBuffer.flip().remaining() > 0) {
      final SocketChannel channel = (SocketChannel)key.channel();
      final int write = channel.write(writeBuffer);
      writeBuffer.compact();
      return write;
    } else {
      writeBuffer.clear();
      try {
        lock.lock();
        suspendWriteIfResumed();
        isWriteEnabled = false;
      } finally {
        lock.unlock();
      }
      return 0;
    }
  }

  public boolean send(byte[] array)
  {
    return send(array, 0, array.length);
  }

  public boolean send(byte[] array, int offset, int len)
  {
    /*
    if (!throwables.isEmpty()) {
      NetletThrowable.Util.throwRuntime(throwables.pollUnsafe());
    }
    */

    Slice f = freeQueue.poll();
    if (f == null) {
      f = new Slice(array, offset, len);
    } else {
      if (f.buffer != null) {
        throw new RuntimeException("Unexpected slice " + f.toString());
      }
      f.buffer = array;
      f.offset = offset;
      f.length = len;
    }
    return send(f);
  }

  public boolean send(Slice f)
  {
    if (sendQueue.offer(f)) {
      try {
        lock.lock();
        if (!isWriteEnabled) {
          resumeWriteIfSuspended();
          isWriteEnabled = true;
        }
      } finally {
        lock.unlock();
      }
      return true;
    }
    return false;
  }
}
