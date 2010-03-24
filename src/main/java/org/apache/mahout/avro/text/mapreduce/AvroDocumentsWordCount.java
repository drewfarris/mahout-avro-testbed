/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.avro.text.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentReader;
import org.apache.mahout.avro.document.AvroFieldReader;
import org.apache.mahout.hadoop.io.serializer.avro.AvroGenericSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.mahout.hadoop.mapreduce.lib.AvroInputFormat;

/** Generates word counts for documents stored in avro document format */
public class AvroDocumentsWordCount extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(AvroDocumentsWordCount.class);
  
  public static class TokenizerMapper 
       extends Mapper<AvroDocument, LongWritable, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    private final AvroDocumentReader dr = new AvroDocumentReader();
    private final AvroFieldReader fr = new AvroFieldReader();
    
    public void map(AvroDocument key, LongWritable value, Context context
                    ) throws IOException, InterruptedException {
      dr.wrap(key);
      fr.wrap(dr.getField("content"));
      
      StringTokenizer itr = new StringTokenizer(fr.getOriginalText());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new AvroDocumentsWordCount(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      return 0;
    }
    
    conf.setStrings("io.serializations",
        new String[] {
          WritableSerialization.class.getName(), 
          AvroSpecificSerialization.class.getName(), 
          AvroReflectSerialization.class.getName(),
          AvroGenericSerialization.class.getName()
        });
    
    Job job = new Job(conf, "word count");
    job.setJarByClass(AvroDocumentsWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setInputFormatClass(AvroInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    
    FileSystem fs = FileSystem.get(conf);
    fs.delete(output, true);
    
    AvroInputFormat.setAvroInputClass(job, AvroDocument.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
    // TODO Auto-generated method stub
    
  }
}
