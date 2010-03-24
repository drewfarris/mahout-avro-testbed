package org.apache.mahout.hadoop.mapred;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;


public class AvroRecordReader<K> implements RecordReader<K, LongWritable> {

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
  private final Map<String,String> metadata;
  private final Class<K> keyClass;
  private K key = null;
  private LongWritable value = null;
  private Counter inputByteCounter;
  
  public static enum CounterGroup {
    INPUT_BYTE_COUNTER
  };
  
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
  
  public AvroRecordReader(InputSplit genericSplit, JobConf conf, Reporter reporter) throws IOException {
    metadata = AvroInputFormat.getAvroInputMetadata(conf);
   
    String className = metadata.get(AvroSerialization.CLASS_KEY);
    if (className != null) {
      try {
        keyClass = (Class<K>) conf.getClassByName(className);
      }
      catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    } 
    else {
      keyClass = null;
    }
    
    FileSplit split = (FileSplit) genericSplit;
    
    inputByteCounter = reporter.getCounter(CounterGroup.INPUT_BYTE_COUNTER);

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
    DatumReader<K> datumReader = getDatumReader(conf);
    reader = new DataFileReader<K>(avroSeekable, datumReader);

    // Now seek to the beginning of our extent.
    reader.sync(start);

    this.lastCheckedPos = start;

    LOG.debug("start: " + this.start);
    LOG.debug("end: " + this.end);
    LOG.debug("fileLen: " + fileLength);
    LOG.debug("Synced beginning: " + getFilePosition());
  }
  
  /**
   * Interpret the user-specified metadata to determine the type of DatumReader
   * to generate.
   * @param context the context containing the job configuration
   * @return a DatumReader
   */
  private DatumReader<K> getDatumReader(JobConf conf) {
    SerializationFactory factory = new SerializationFactory(conf);
    SerializationBase<K> serialization = (SerializationBase<K>) factory.getSerialization(keyClass);
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

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public K createKey() {
    //TODO: do this right, if the keyClass is null, and a schema has been 
    // provided, need to pull the type of the top level record from the 
    // schema and return that. If no schems ia provided, need to return
    // some form of Generic Record or something.
    try {
      return (K) keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LongWritable createValue() {
    return new LongWritable();
  }

  @Override
  public long getPos() throws IOException {
    return this.getFilePosition();
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
          (getFilePosition() - start) / (float)(end - start));
    }
  }

  @Override
  public boolean next(K key, LongWritable value) throws IOException {
    long position = avroSeekable.tell();
    
    if (position >= avroSeekable.length()) {
      LOG.info("Reached end of file");
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
      LOG.info("Reached first sync past end of split");
      return false;
    }

    if (value == null) {
      value = new LongWritable();
    }

    
    value.set(position);
    this.value = value;
    
    this.key = reader.next(key);

    inputByteCounter.increment(position - this.lastCheckedPos);
    this.lastCheckedPos = position;
    return this.key != null;
  }

  private synchronized long getFilePosition() throws IOException {
    return avroSeekable.tell();
  }
}
