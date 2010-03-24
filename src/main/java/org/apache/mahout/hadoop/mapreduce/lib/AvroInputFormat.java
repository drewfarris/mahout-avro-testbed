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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.mahout.hadoop.io.serializer.SerializationBase;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSerialization;

/** An {@link InputFormat} for Avro files */
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
  public static void setAvroInputMetadata(JobContext job, Map<String, String> metadata) {
    SerializationBase.setConfigurationMap(job.getConfiguration(), AVRO_INPUT_METADATA_KEY, metadata);
  }

  /**
   * Retrieve the metadata controlling how AvroInputFormat deserializes
   * data from the input files
   * @param job the job being executed
   * @return metadata metadata that specifies the input schema or class.
   */
  public static Map<String, String> getAvroInputMetadata(JobContext job) {
    return SerializationBase.getConfigurationMap(job.getConfiguration(), AVRO_INPUT_METADATA_KEY);
  }

  /**
   * Set the input metadata based on a schema (for the generic interface).
   * @param job the job being configured.
   * @param schema the schema to use to interpret the input.
   */
  public static void setAvroInputSchema(JobContext job, Schema schema) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(AvroSerialization.AVRO_SCHEMA_KEY, schema.toString());
    setAvroInputMetadata(job, metadata);
  }

  /**
   * Set the input metadata based on a class name (for specific/reflect
   * interfaces).
   * @param job the job being configured
   * @param theClass the class to use to hold a record from the input
   */
  public static void setAvroInputClass(JobContext job, Class<?> theClass) {
    Map<String, String> metadata = 
      SerializationBase.getMetadataFromClass(job.getConfiguration(), theClass);
    setAvroInputMetadata(job, metadata);
  }

  @Override
  public RecordReader<K, LongWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new AvroRecordReader<K>();
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // seems kinda silly to require this.
    final String ext = AvroOutputFormat.EXT;
    List<FileStatus> inFiles = super.listStatus(job);
    List<FileStatus> outFiles = new ArrayList<FileStatus>();
    for (FileStatus file : inFiles) {
      outFiles.add(file);
      //if (file.getPath().getName().endsWith(ext)) {
      //  outFiles.add(file);
      //}
    }
    return outFiles;
  }
}

