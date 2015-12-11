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
package com.datatorrent.netlet.benchmark.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

import com.datatorrent.netlet.benchmark.util.BenchmarkConfiguration;

class SingletonByteBufAllocator implements ByteBufAllocator
{
  ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(BenchmarkConfiguration.messageSize, Integer.MAX_VALUE);

  @Override
  public ByteBuf buffer()
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf buffer(int initialCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf buffer(int initialCapacity, int maxCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf ioBuffer()
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf ioBuffer(int initialCapacity, int maxCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf heapBuffer()
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf heapBuffer(int initialCapacity, int maxCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf directBuffer()
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public ByteBuf directBuffer(int initialCapacity, int maxCapacity)
  {
    return byteBuf.retain();
  }

  @Override
  public CompositeByteBuf compositeBuffer()
  {
    return ByteBufAllocator.DEFAULT.compositeBuffer();
  }

  @Override
  public CompositeByteBuf compositeBuffer(int maxNumComponents)
  {
    return ByteBufAllocator.DEFAULT.compositeBuffer(maxNumComponents);
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer()
  {
    return ByteBufAllocator.DEFAULT.compositeHeapBuffer();
  }

  @Override
  public CompositeByteBuf compositeHeapBuffer(int maxNumComponents)
  {
    return ByteBufAllocator.DEFAULT.compositeHeapBuffer(maxNumComponents);
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer()
  {
    return ByteBufAllocator.DEFAULT.compositeDirectBuffer();
  }

  @Override
  public CompositeByteBuf compositeDirectBuffer(int maxNumComponents)
  {
    return ByteBufAllocator.DEFAULT.compositeDirectBuffer(maxNumComponents);
  }

  @Override
  public boolean isDirectBufferPooled()
  {
    return false;
  }
}
