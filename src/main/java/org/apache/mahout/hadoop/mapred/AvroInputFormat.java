package org.apache.mahout.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;

public class AvroInputFormat<K> extends FileInputFormat<K, LongWritable> {


  /**
   * By default, AvroInputFormat reads the schema from the file and
   * uses GenericDatumReader. If you want to specify serialization
   * metadata to more precisely control the DatumReader, the map
   * should be embedded below this configuration key. (i.e., with
   * setAvroInputMetadata().)
   */

  public static final String AVRO_INPUT_METADATA_KEY =
      "mapreduce.input.avro.serialization.metadata";

  /**
   * Set the input metadata controlling how AvroInputFormat deserializes
   * data from the input files.
   * @param job the job being configured
   * @param metadata the metadata specifying the input schema, classes, etc.
   */
  public static void setAvroInputMetadata(JobConf conf, Map<String, String> metadata) {
    SerializationBase.setConfigurationMap(conf, AVRO_INPUT_METADATA_KEY, metadata);
  }

  /**
   * Retrieve the metadata controlling how AvroInputFormat deserializes
   * data from the input files
   * @param job the job being executed
   * @return metadata metadata that specifies the input schema or class.
   */
  public static Map<String, String> getAvroInputMetadata(JobConf conf) {
    return SerializationBase.getConfigurationMap(conf, AVRO_INPUT_METADATA_KEY);
  }

  /**
   * Set the input metadata based on a schema (for the generic interface).
   * @param job the job being configured.
   * @param schema the schema to use to interpret the input.
   */
  public static void setAvroInputSchema(JobConf conf, Schema schema) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, schema.toString());
    setAvroInputMetadata(conf, metadata);
  }

  /**
   * Set the input metadata based on a class name (for specific/reflect
   * interfaces).
   * @param job the job being configured
   * @param theClass the class to use to hold a record from the input
   */
  public static void setAvroInputClass(JobConf conf, Class<?> theClass) {
    Map<String, String> metadata = 
      SerializationBase.getMetadataFromClass(conf, theClass);
    setAvroInputMetadata(conf, metadata);
  }


  @Override
  public RecordReader<K, LongWritable> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new AvroRecordReader<K>(split, job, reporter);
  }

}
