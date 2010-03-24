/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.hadoop.mapreduce.lib;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads Avro data from an Avro file.
 * Records are returned in the key portion.
 * The value represents the byte offset of the current object in the file.
 */
public class AvroRecordReader<K> extends RecordReader<K, LongWritable> {
  
  private static final Logger log = LoggerFactory.getLogger(AvroRecordReader.class);
  
  public static final String COUNTER_GROUP = "Counter-Group";
  public static final String BYTES_READ = "Bytes-Read";
  
  private static final Log LOG = LogFactory.getLog(AvroRecordReader.class);

  // Split information
  private long start; // Where the user instructed us to start
  private long end; // Where the user instructed us to end

  // The last position we reported reading to.
  // Avro's file reader will do some readahead, so we need to
  // record this to track our progress for counters
  private long lastCheckedPos;
  private long lastSync = 0;
  
  // Stream / reader layers
  private FSDataInputStream fileIn;
  private SeekableInput avroSeekable;
  private DataFileReader<K> reader;

  // Values to present to the user
  private K key = null;
  private LongWritable value = null;
  private Counter inputByteCounter;

  /**
   * Class that wraps FSDataInputStream in Avro's SeekableInput
   * interface.
   */
  public static class AvroSeekableStream implements SeekableInput {
    private FSDataInputStream stream;
    private long len;

    public AvroSeekableStream(final FSDataInputStream in, final long len) {
      this.stream = in;
      this.len = len;
    }

    public long length() {
      return len;
    }

    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    public void seek(long p) throws IOException {
      stream.seek(p);
    }

    public long tell() throws IOException {
      return stream.getPos();
    }
  }

  /**
   * Interpret the user-specified metadata to determine the type of DatumReader
   * to generate.
   * @param context the context containing the job configuration
   * @return a DatumReader
   */
  private DatumReader<K> getDatumReader(TaskAttemptContext context) {
    Map<String, String> metadata = AvroInputFormat.getAvroInputMetadata(
        context);
    String className = metadata.get(AvroSerialization.CLASS_KEY);
    Class<?> clazz = null;
    try {
      clazz = context.getConfiguration().getClassByName(className);
    }
    catch (ClassNotFoundException ex) {
      return new GenericDatumReader<K>();
    }
      
    SerializationFactory factory = new SerializationFactory(context.getConfiguration());
    SerializationBase<K> serialization = (SerializationBase<K>) factory.getSerialization(clazz);
    if (null == serialization) {
      // metadata is unset or corrupt. Use the generic reader.
      LOG.warn("Could not find appropriate serialization for AvroInputFormat;"
          + " trying GenericDatumReader");
      return new GenericDatumReader<K>();
    } else if (! (serialization instanceof AvroSerialization)) {
      // metadata is not avro metadata?
      LOG.warn("Metadata in " + AvroInputFormat.AVRO_INPUT_METADATA_KEY
          + " is not avro-serializable. Using GenericDatumReader.");
      return new GenericDatumReader<K>();
    }

    LOG.info("Got serialization: " + serialization.getClass().getName());

    return ((AvroSerialization<K>) serialization).getReader(metadata);
  }

  public synchronized void initialize(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {

    FileSplit split = (FileSplit) genericSplit;
    inputByteCounter = ((MapContext) context).getCounter(COUNTER_GROUP, BYTES_READ);
    Configuration conf = context.getConfiguration();

    this.start = split.getStart();
    this.end = start + split.getLength();
    final Path file = split.getPath();

    // Open the file
    final FileSystem fs = file.getFileSystem(conf);
    FileStatus [] status = fs.listStatus(file);
    if (status.length != 1) {
      throw new IOException("Could not get status for file : " + file);
    }

    long fileLength = status[0].getLen();

    fileIn = fs.open(file);
    avroSeekable = new AvroSeekableStream(fileIn, fileLength);
    DatumReader<K> datumReader = getDatumReader(context);
    reader = new DataFileReader<K>(avroSeekable, datumReader);

    // Now seek to the beginning of our extent.
    reader.sync(start);

    this.lastCheckedPos = start;

    LOG.debug("start: " + this.start);
    LOG.debug("end: " + this.end);
    LOG.debug("fileLen: " + fileLength);
    LOG.debug("Synced beginning: " + getFilePosition());
  }

  public synchronized boolean nextKeyValue() throws IOException {
    long position = avroSeekable.tell();
    
    if (position >= avroSeekable.length()) {
      log.info("Reached end of file");
      return false;
    }
    
    if ((lastSync == 0) && (position >= this.end)) {
      // find the position of the start of the next sync and
      // set that as the true end.
      reader.sync(position);
      lastSync = avroSeekable.tell();
      avroSeekable.seek(position);
    }
    
    // the true end is set and we're past it.
    if ((lastSync > 0) && (lastSync <= position)) {
      log.info("Reached first sync past end of split");
      return false;
    }

    if (value == null) {
      value = new LongWritable();
    }

    this.value.set(position);
    this.key = reader.next(key);

    inputByteCounter.increment(position - this.lastCheckedPos);
    this.lastCheckedPos = position;
    return this.key != null;
  }

  @Override
  public synchronized K getCurrentKey() {
    return key;
  }

  @Override
  public synchronized LongWritable getCurrentValue() {
    return value;
  }

  private synchronized long getFilePosition() throws IOException {
    return avroSeekable.tell();
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
          (getFilePosition() - start) / (float)(end - start));
    }
  }

  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}

