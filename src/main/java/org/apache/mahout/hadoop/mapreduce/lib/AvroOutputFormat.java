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
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;

/** An {@link OutputFormat} that writes Avro files. Only the key is
 * used in the output file. The value must be null. Non-null values trigger
 * a warning and are not written to the output file.
 *
 * The output key metadata must be set (e.g., by
 * SchemaBasedJobData.setOutputKeySchema()) to metadata accepted by an
 * Avro-based serialization.
 */
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
  public static void setAvroOutputMetadata(JobContext job, Map<String, String> metadata) {
    SerializationBase.setConfigurationMap(job.getConfiguration(), AVRO_OUTPUT_METADATA_KEY, metadata);
  }
  
  /**
   * Retrieve the metadata controlling how AvroInputFormat deserializes
   * data from the input files
   * @param job the job being executed
   * @return metadata metadata that specifies the input schema or class.
   */
  public static Map<String, String> getAvroOutputMetadata(JobContext job) {
    return SerializationBase.getConfigurationMap(job.getConfiguration(), AVRO_OUTPUT_METADATA_KEY);
  }

  /**
   * Set the output metadata based on a schema (for the generic interface).
   * @param job the job being configured.
   * @param schema the schema to use to interpret the input.
   */
  public static void setAvroOutputSchema(JobContext job, Schema schema) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, schema.toString());
    setAvroOutputMetadata(job, metadata);
  }
  
  /**
   * Set the output metadata based on a class name (for specific/reflect
   * interfaces).
   * @param job the job being configured
   * @param theClass the class to use to hold a record from the input
   */
  public static void setAvroOutputClass(JobContext job, Class<?> theClass) {
    Map<String, String> metadata = 
      SerializationBase.getMetadataFromClass(job.getConfiguration(), theClass);
    setAvroOutputMetadata(job, metadata);
  }

  private OutputStream createStream(TaskAttemptContext context)
      throws IOException {
    Configuration conf = context.getConfiguration();
    Path workFile = getDefaultWorkFile(context, EXT);
    FileSystem fs = workFile.getFileSystem(conf);
    return fs.create(workFile, false);
  }

  public RecordWriter<K, Object> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {

    Configuration conf = context.getConfiguration();
    SerializationFactory factory = new SerializationFactory(conf);
    Map<String, String> metadata = getAvroOutputMetadata(context);
    
    String className = metadata.get(AvroSerialization.CLASS_KEY);
    Class<?> clazz = null;
    try {
      clazz = context.getConfiguration().getClassByName(className);
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
    final OutputStream ostream = createStream(context);
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

      public void close(TaskAttemptContext context) throws IOException {
        if (null != fileWriter) {
          fileWriter.close();
        }
      }
    };
  }
}

