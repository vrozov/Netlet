/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.jctools.queues.SpscArrayQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.netlet.Listener.ClientListener;
import com.datatorrent.netlet.NetletThrowable.NetletRuntimeException;
import com.datatorrent.netlet.util.CircularBuffer;

/**
 * <p>
 * Abstract AbstractClient class.</p>
 *
 * @since 1.0.0
 */
public abstract class AbstractClient implements ClientListener
{
  private static final int THROWABLES_COLLECTION_SIZE = 4;
  public static final int MAX_SENDBUFFER_SIZE;
  private static final SelectionKey invalidSelectionKey = new SelectionKey()
  {
    @Override
    public SelectableChannel channel()
    {
      return null;
    }

    @Override
    public Selector selector()
    {
      return null;
    }

    @Override
    public boolean isValid()
    {
      return false;
    }

    @Override
    public void cancel()
    {

    }

    @Override
    public int interestOps()
    {
      return 0;
    }

    @Override
    public SelectionKey interestOps(int ops)
    {
      return this;
    }

    @Override
    public int readyOps()
    {
      return 0;
    }
  };

  protected final CircularBuffer<NetletThrowable> throwables;
  protected final CircularBuffer<CircularBuffer<Slice>> bufferOfBuffers;
  protected final CircularBuffer<Slice> freeBuffer;
  protected CircularBuffer<Slice> sendBuffer4Offers, sendBuffer4Polls;
  protected SpscArrayQueue<Slice> sendBuffer;
  private final SpscArrayQueue<Slice> sliceSpscArrayQueue;
  protected final ByteBuffer writeBuffer;
  protected boolean write = true;
  /*
   * access to the key is not thread safe. It is read/write on the default event loop and read only on other threads,
   * so other threads may get stale value.
   */
  protected SelectionKey key = invalidSelectionKey;

  public boolean isConnected()
  {
    return key.isValid() && ((SocketChannel)key.channel()).isConnected();
  }

  public AbstractClient(int writeBufferSize, int sendBufferSize)
  {
    this(ByteBuffer.allocateDirect(writeBufferSize), sendBufferSize);
  }

  public AbstractClient(int sendBufferSize)
  {
    this(64 * 1024, sendBufferSize);
  }

  public AbstractClient()
  {
    this(64 * 1024, 1024);
  }

  public AbstractClient(ByteBuffer writeBuffer, int sendBufferSize)
  {
    int i = 1;
    int n = 1;
    do {
      n *= 2;
      i++;
    }
    while (n != MAX_SENDBUFFER_SIZE);
    bufferOfBuffers = new CircularBuffer<CircularBuffer<Slice>>(i);

    this.throwables = new CircularBuffer<NetletThrowable>(THROWABLES_COLLECTION_SIZE);
    this.writeBuffer = writeBuffer;
    if (sendBufferSize == 0) {
      sendBufferSize = 1024;
    }
    else if (sendBufferSize % 1024 > 0) {
      sendBufferSize += 1024 - (sendBufferSize % 1024);
    }
    sendBuffer4Polls = sendBuffer4Offers = new CircularBuffer<Slice>(sendBufferSize, 10);
    freeBuffer = new CircularBuffer<Slice>(sendBufferSize, 10);
    sendBuffer = new SpscArrayQueue<Slice>(sendBufferSize);
    sliceSpscArrayQueue = new SpscArrayQueue<Slice>(sendBufferSize);
  }

  @Override
  public void registered(SelectionKey key)
  {
    this.key = key;
  }

  @Override
  public void connected()
  {
    write = false;
  }

  @Override
  public void disconnected()
  {
    write = true;
  }

  @Override
  public final void read() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int read;
    if ((read = channel.read(buffer())) > 0) {
      this.read(read);
    }
    else if (read == -1) {
      try {
        channel.close();
      }
      finally {
        disconnected();
        unregistered(key);
        key.attach(Listener.NOOP_CLIENT_LISTENER);
      }
    }
    else {
      logger.debug("{} read 0 bytes", this);
    }
  }

  /**
   * @since 1.2.0
   */
  public boolean isReadSuspended()
  {
    return (key.interestOps() & SelectionKey.OP_READ) == 0;
  }

  /**
   * @since 1.2.0
   */
  public boolean suspendReadIfResumed()
  {
    final int interestOps = key.interestOps();
    if ((interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
      logger.debug("Suspending read on key {} with attachment {}", key, key.attachment());
      key.interestOps(interestOps & ~SelectionKey.OP_READ);
      return true;
    } else {
      return false;
    }
  }

  /**
   * @since 1.2.0
   */
  public boolean resumeReadIfSuspended()
  {
    final int interestOps = key.interestOps();
    if ((interestOps & SelectionKey.OP_READ) == 0) {
      logger.debug("Resuming read on key {} with attachment {}", key, key.attachment());
      key.interestOps(interestOps | SelectionKey.OP_READ);
      key.selector().wakeup();
      return true;
    } else {
      return false;
    }
  }

  /**
   * @deprecated As of release 1.2.0, replaced by {@link #suspendReadIfResumed()}
   */
  @Deprecated
  public void suspendRead()
  {
    key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
  }

  /**
   * @deprecated As of release 1.2.0, replaced by {@link #resumeReadIfSuspended()}
   */
  @Deprecated
  public void resumeRead()
  {
    key.interestOps(key.interestOps() | SelectionKey.OP_READ);
    key.selector().wakeup();
  }

  @Override
  public final void write() throws IOException
  {
    /*
     * at first when we enter this function, our buffer is in fill mode.
     */
    int remaining = writeBuffer.remaining();
    if (remaining == 0) {
      channelWrite();
      return;
    }
    Slice f = sendBuffer.peek();
    if (f == null) {
      synchronized (sendBuffer) {
        f = sendBuffer.peek();
        if (f == null) {
          key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
          write = false;
          return;
        }
      }
    }
    do {
      if (remaining < f.length) {
        writeBuffer.put(f.buffer, f.offset, remaining);
        f.offset += remaining;
        f.length -= remaining;
        channelWrite();
        return;
      } else {
        writeBuffer.put(f.buffer, f.offset, f.length);
        remaining -= f.length;
        sliceSpscArrayQueue.offer(sendBuffer.poll());
      }
    } while ((f = sendBuffer.peek()) != null);
    channelWrite();
  }

  private void channelWrite() throws IOException
  {
    writeBuffer.flip();
    final SocketChannel channel = (SocketChannel)key.channel();
    channel.write(writeBuffer);
    writeBuffer.compact();
  }

  public boolean send(byte[] array)
  {
    return send(array, 0, array.length);
  }

  public boolean send(byte[] array, int offset, int len)
  {
    if (!throwables.isEmpty()) {
      NetletThrowable.Util.throwRuntime(throwables.pollUnsafe());
    }

    Slice f = sliceSpscArrayQueue.poll();
    if (f == null) {
      f = new Slice(array, offset, len);
    }
    else {
      f.buffer = array;
      f.offset = offset;
      f.length = len;
    }

    if (sendBuffer.offer(f)) {
      synchronized (sendBuffer) {
        if (!write) {
          key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
          write = true;
          key.selector().wakeup();
        }
      }

      return true;
    }

    return false;
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    logger.error("Exception in event loop {}", el, cce);
    throwables.offer(NetletThrowable.Util.rewrap(cce, el));
  }

  public abstract ByteBuffer buffer();

  public abstract void read(int len);

  @Override
  public void unregistered(SelectionKey key)
  {
      final SpscArrayQueue<Slice> SEND_BUFFER = sendBuffer;
      sendBuffer = new SpscArrayQueue<Slice>(0)
      {
        @Override
        public boolean isEmpty()
        {
          return SEND_BUFFER.peek() == null;
        }

        @Override
        public boolean offer(Slice e)
        {
          throw new NetletRuntimeException(new UnsupportedOperationException("Client does not own the socket any longer!"), null);
        }

        @Override
        public int size()
        {
          return SEND_BUFFER.size();
        }

        @Override
        public Slice poll()
        {
          return SEND_BUFFER.poll();
        }

        @Override
        public Slice peek()
        {
          return SEND_BUFFER.peek();
        }

      };
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractClient.class);

  /* implemented here since it requires access to logger. */
  static {
    int size = 32 * 1024;
    final String key = "NETLET.MAX_SENDBUFFER_SIZE";
    String property = System.getProperty(key);
    if (property != null) {
      try {
        size = Integer.parseInt(property);
        if (size <= 0) {
          throw new IllegalArgumentException(key + " needs to be a positive integer which is also power of 2.");
        }

        if ((size & (size - 1)) != 0) {
          size--;
          size |= size >> 1;
          size |= size >> 2;
          size |= size >> 4;
          size |= size >> 8;
          size |= size >> 16;
          size++;
          logger.warn("{} set to {} since {} is not power of 2.", key, size, property);
        }
      }
      catch (Exception exception) {
        logger.warn("{} set to {} since {} could not be parsed as an integer.", key, size, property, exception);
      }
    }
    MAX_SENDBUFFER_SIZE = size;
  }
}
