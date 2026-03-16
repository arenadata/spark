/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.util.AbstractFileRegion;
import org.apache.spark.network.util.NettyUtils;

/**
 * A {@link io.netty.channel.FileRegion} implementation that reads file data ahead in a
 * separate I/O thread pool, so that the Netty event loop is never blocked on disk reads.
 *
 * <p>Background I/O threads read chunks from the file into a bounded queue of ByteBuffers.
 * When Netty calls {@link #transferTo}, it takes a pre-read buffer from the queue and writes
 * it to the socket — a fast, non-blocking operation from the event loop's perspective.</p>
 *
 * <p>If the queue is empty (disk hasn't caught up), transferTo returns 0, causing Netty to
 * yield the event loop and retry later. This keeps heartbeats and other connections responsive
 * even when disk I/O is slow.</p>
 */
public class ReadAheadFileRegion extends AbstractFileRegion {

  private static final Logger logger = LoggerFactory.getLogger(ReadAheadFileRegion.class);

  private static final ByteBuffer EOF_MARKER = ByteBuffer.allocate(0);

  private static volatile ExecutorService ioPool;

  private final FileChannel fileChannel;
  private final long fileOffset;
  private final long regionCount;
  private final int chunkSize;
  private final ArrayBlockingQueue<ByteBuffer> readAheadQueue;
  private long transferred;

  private ByteBuffer currentBuf;

  private volatile Throwable readError;
  private final AtomicBoolean deallocated = new AtomicBoolean(false);
  private boolean eofSeen;

  public ReadAheadFileRegion(
      FileChannel fileChannel,
      long offset,
      long count,
      int chunkSize,
      int queueDepth,
      int poolSize) {
    this.fileChannel = fileChannel;
    this.fileOffset = offset;
    this.regionCount = count;
    this.chunkSize = chunkSize;
    this.readAheadQueue = new ArrayBlockingQueue<>(queueDepth + 1); // +1 for EOF marker
    this.transferred = 0;
    this.eofSeen = false;

    getOrCreatePool(poolSize).submit(this::readAhead);
  }

  private static ExecutorService getOrCreatePool(int poolSize) {
    if (ioPool == null) {
      synchronized (ReadAheadFileRegion.class) {
        if (ioPool == null) {
          ioPool = Executors.newFixedThreadPool(poolSize,
              NettyUtils.createThreadFactory("shuffle-readahead"));
        }
      }
    }
    return ioPool;
  }

  private void readAhead() {
    try {
      long readOffset = 0;
      while (readOffset < regionCount && !deallocated.get()) {
        int toRead = (int) Math.min(chunkSize, regionCount - readOffset);
        ByteBuffer buf = ByteBuffer.allocate(toRead);
        int bytesRead = 0;
        while (bytesRead < toRead) {
          int n = fileChannel.read(buf, fileOffset + readOffset + bytesRead);
          if (n < 0) {
            break;
          }
          bytesRead += n;
        }
        buf.flip();
        if (deallocated.get()) {
          break;
        }
        readAheadQueue.put(buf); // blocks if queue full — backpressure
        readOffset += bytesRead;
        if (bytesRead < toRead) {
          // Unexpected EOF from file
          break;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      readError = t;
      logger.error("ReadAhead I/O error for file region [offset={}, count={}]",
          fileOffset, regionCount, t);
    } finally {
      // Signal completion to the consumer
      try {
        if (!deallocated.get()) {
          readAheadQueue.put(EOF_MARKER);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public long count() {
    return regionCount;
  }

  @Override
  public long position() {
    return 0;
  }

  @Override
  public long transferred() {
    return transferred;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    if (readError != null) {
      throw new IOException("ReadAhead I/O failed", readError);
    }

    // Get a buffer: either a partially written one from last call, or a new one from queue
    if (currentBuf == null || !currentBuf.hasRemaining()) {
      currentBuf = readAheadQueue.poll(); // non-blocking
      if (currentBuf == null) {
        // No data ready yet. Return 0 so Netty yields the event loop.
        return 0;
      }
      if (currentBuf == EOF_MARKER) {
        eofSeen = true;
        currentBuf = null;
        if (transferred < regionCount) {
          throw new IOException(String.format(
              "ReadAhead completed prematurely: transferred %d of %d bytes", transferred,
              regionCount));
        }
        return 0;
      }
    }

    int written = target.write(currentBuf);
    if (written > 0) {
      transferred += written;
    }
    return written;
  }

  @Override
  protected void deallocate() {
    if (deallocated.compareAndSet(false, true)) {
      readAheadQueue.clear();
      try {
        fileChannel.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }
}
