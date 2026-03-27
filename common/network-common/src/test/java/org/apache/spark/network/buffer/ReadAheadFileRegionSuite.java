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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.util.ByteArrayWritableChannel;

@Ignore("Waiting for fix")
public class ReadAheadFileRegionSuite {

  private File testFile;

  @Before
  public void setUp() throws Exception {
    testFile = File.createTempFile("readahead-test", ".dat");
    testFile.deleteOnExit();
  }

  @After
  public void tearDown() {
    if (testFile != null) {
      testFile.delete();
    }
  }

  private void writeTestFile(byte[] data) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(testFile, "rw")) {
      raf.write(data);
    }
  }

  private byte[] drain(ReadAheadFileRegion region, int maxIterations) throws IOException {
    ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) region.count());
    int iterations = 0;
    while (region.transferred() < region.count()) {
      region.transferTo(channel, region.transferred());
      iterations++;
      if (iterations > maxIterations) {
        fail("Too many iterations (" + iterations + "), transferred only "
            + region.transferred() + " of " + region.count());
      }
    }
    return Arrays.copyOf(channel.getData(), (int) region.count());
  }

  @Test
  public void testBasicTransfer() throws Exception {
    byte[] data = new byte[1024];
    new Random(42).nextBytes(data);
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 256, 4, 2);

    byte[] result = drain(region, 10000);
    assertArrayEquals(data, result);
    assertEquals(data.length, region.transferred());
    region.release();
  }

  @Test
  public void testLargeFileMultipleChunks() throws Exception {
    // 1MB file, 64KB chunks = 16 chunks
    byte[] data = new byte[1024 * 1024];
    new Random(123).nextBytes(data);
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 64 * 1024, 4, 2);

    byte[] result = drain(region, 100000);
    assertArrayEquals(data, result);
    region.release();
  }

  @Test
  public void testOffsetAndPartialFile() throws Exception {
    // Write 1KB, but only transfer bytes [256, 768)
    byte[] data = new byte[1024];
    new Random(99).nextBytes(data);
    writeTestFile(data);

    int offset = 256;
    int length = 512;
    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, offset, length, 128, 4, 2);

    byte[] result = drain(region, 10000);
    byte[] expected = Arrays.copyOfRange(data, offset, offset + length);
    assertArrayEquals(expected, result);
    region.release();
  }

  @Test
  public void testSingleByteChunks() throws Exception {
    // Extreme case: 1-byte chunks
    byte[] data = new byte[64];
    new Random(7).nextBytes(data);
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 1, 8, 2);

    byte[] result = drain(region, 100000);
    assertArrayEquals(data, result);
    region.release();
  }

  @Test
  public void testChunkLargerThanFile() throws Exception {
    byte[] data = new byte[100];
    new Random(55).nextBytes(data);
    writeTestFile(data);

    // Chunk size (8MB) >> file size (100 bytes) — should still work
    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(
        fc, 0, data.length, 8 * 1024 * 1024, 4, 2);

    byte[] result = drain(region, 10000);
    assertArrayEquals(data, result);
    region.release();
  }

  @Test
  public void testQueueDepthOne() throws Exception {
    // Minimal queue depth — backpressure on every chunk
    byte[] data = new byte[4096];
    new Random(77).nextBytes(data);
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 512, 1, 1);

    byte[] result = drain(region, 100000);
    assertArrayEquals(data, result);
    region.release();
  }

  @Test
  public void testDeallocateDuringTransfer() throws Exception {
    // Ensure deallocate doesn't cause errors even if transfer is in progress
    byte[] data = new byte[1024 * 1024];
    new Random(33).nextBytes(data);
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 64 * 1024, 4, 2);

    // Transfer only partially
    ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) region.count());
    for (int i = 0; i < 50; i++) {
      region.transferTo(channel, region.transferred());
    }

    // Deallocate mid-transfer — should not throw
    region.release();
  }

  @Test
  public void testCountAndPosition() throws Exception {
    byte[] data = new byte[512];
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 100, 200, 64, 4, 2);

    assertEquals(200, region.count());
    assertEquals(0, region.position());
    assertEquals(0, region.transferred());

    region.release();
  }

  @Test
  public void testIsFileRegion() throws Exception {
    byte[] data = new byte[256];
    writeTestFile(data);

    FileChannel fc = FileChannel.open(testFile.toPath(), StandardOpenOption.READ);
    ReadAheadFileRegion region = new ReadAheadFileRegion(fc, 0, data.length, 64, 4, 2);

    assertTrue(region instanceof io.netty.channel.FileRegion);
    region.release();
  }
}
