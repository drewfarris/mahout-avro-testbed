package org.apache.mahout.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;

public class AvroOutputFormat<K> extends FileOutputFormat<K, Object> {

  public static final String AVRO_OUTPUT_METADATA_KEY =
    "mapreduce.output.avro.serialization.metadata";
    
  public final static String EXT = ".avro";

  private static final Log LOG = LogFactory.getLog(AvroOutputFormat.class);

  // frequency with which sync tokens are introduced into the output file.
  private final static int SYNC_DISTANCE = 32;
  
 

  /**
   * Set the output metadata controlling how AvroInputFormat deserializes
   * data from the input files.
   * @param job the job being configured
   * @param metadata the metadata specifying the input schema, classes, etc.
   */
  public static void setAvroOutputMetadata(JobConf conf, Map<String, String> metadata) {
    SerializationBase.setConfigurationMap(conf, AVRO_OUTPUT_METADATA_KEY, metadata);
  }
  
  /**
   * Retrieve the metadata controlling how AvroInputFormat deserializes
   * data from the input files
   * @param job the job being executed
   * @return metadata metadata that specifies the input schema or class.
   */
  public static Map<String, String> getAvroOutputMetadata(JobConf conf) {
    return SerializationBase.getConfigurationMap(conf, AVRO_OUTPUT_METADATA_KEY);
  }

  /**
   * Set the output metadata based on a schema (for the generic interface).
   * @param job the job being configured.
   * @param schema the schema to use to interpret the input.
   */
  public static void setAvroOutputSchema(JobConf conf, Schema schema) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, schema.toString());
    setAvroOutputMetadata(conf, metadata);
  }
  
  /**
   * Set the output metadata based on a class name (for specific/reflect
   * interfaces).
   * @param job the job being configured
   * @param theClass the class to use to hold a record from the input
   */
  public static void setAvroOutputClass(JobConf conf, Class<?> theClass) {
    Map<String, String> metadata = 
      SerializationBase.getMetadataFromClass(conf, theClass);
    setAvroOutputMetadata(conf, metadata);
  }

  @Override
  public RecordWriter<K, Object> getRecordWriter(FileSystem ignored,
      JobConf conf, String name, Progressable progress) throws IOException {
    SerializationFactory factory = new SerializationFactory(conf);
    Map<String, String> metadata = getAvroOutputMetadata(conf);
    
    String className = metadata.get(AvroSerialization.CLASS_KEY);
    Class<?> clazz = null;
    try {
      clazz = conf.getClassByName(className);
    }
    catch (ClassNotFoundException ex) {
      throw new IOException("Could not get class for output key metadata.");
    }
    
    SerializationBase<K> serialization = (SerializationBase<K>)  factory.getSerialization(clazz);
    // Check to make sure we have an Avro-based serializer.
    if (null == serialization) {
      throw new IOException("Could not get serializer for output key metadata.");
    } else if (! (serialization instanceof AvroSerialization)) {
      throw new IOException("Output key is not Avro-serializable.");
    }

    // Use this to instantiate the appropriate type of DatumWriter.
    AvroSerialization<K> avroSerialization =
        (AvroSerialization<K>) serialization;

    final Schema schema = avroSerialization.getSchema(metadata);
    final DatumWriter<K> datumWriter = avroSerialization.getWriter(metadata);
    final OutputStream ostream = createStream(conf);
    final DataFileWriter<K> fileWriter = new DataFileWriter<K>(schema, ostream, datumWriter);

    return new RecordWriter<K, Object>() {
      private boolean warnedVal = false;
      private int recordsFromSync = 0;

      public void write(K key, Object value)
          throws IOException {
        if (!warnedVal && null != value) {
          LOG.warn("Writing non-null value to AvroOutputFormat: "
              + "this value will be ignored.");
          warnedVal = true;
        }

        fileWriter.append(key);
        recordsFromSync++;
        if (recordsFromSync >= SYNC_DISTANCE) {
          fileWriter.sync();
          recordsFromSync = 0;
        }
      }

      public void close(Reporter reporter) throws IOException {
        if (null != fileWriter) {
          fileWriter.close();
        }
      }
    };
  }
  
  private OutputStream createStream(JobConf conf)
      throws IOException {
    Path workFile = getPathForCustomFile(conf, EXT);
    FileSystem fs = workFile.getFileSystem(conf);
    return fs.create(workFile, false);
  }

}
