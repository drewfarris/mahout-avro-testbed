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
package org.apache.mahout.avro.text.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentReader;
import org.apache.mahout.avro.document.AvroFieldReader;
import org.apache.mahout.hadoop.io.serializer.avro.AvroGenericSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.mahout.hadoop.mapred.AvroInputFormat;

/** Generates word counts for documents stored in avro document format */
public class AvroDocumentsWordCount extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(AvroDocumentsWordCount.class);
  
  public static class TokenizerMapper extends MapReduceBase
       implements Mapper<AvroDocument, LongWritable, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    private final AvroDocumentReader dr = new AvroDocumentReader();
    private final AvroFieldReader fr = new AvroFieldReader();
    
    public void map(AvroDocument key, LongWritable value, 
        OutputCollector<Text, IntWritable> collector, Reporter reporter
                    ) throws IOException {
      dr.wrap(key);
      fr.wrap(dr.getField("content"));
      
      StringTokenizer itr = new StringTokenizer(fr.getOriginalText());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        collector.collect(word, one);
      }
    }
  }
  
  public static class IntSumReducer extends MapReduceBase
       implements Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterator<IntWritable> values,  
          OutputCollector<Text, IntWritable> collector, Reporter reporter

                       ) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      result.set(sum);
      collector.collect(key, result);
    }
  }


  @Override
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf();
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
    
    conf.setJarByClass(AvroDocumentsWordCount.class);
    conf.setMapperClass(TokenizerMapper.class);
    conf.setCombinerClass(IntSumReducer.class);
    conf.setReducerClass(IntSumReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    
    FileSystem fs = FileSystem.get(conf);
    fs.delete(output, true);
    
    AvroInputFormat.setAvroInputClass(conf, AvroDocument.class);
    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    
    RunningJob job = JobClient.runJob(conf);
    job.waitForCompletion();
    
    return job.isSuccessful() ? 1 : 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new AvroDocumentsWordCount(), args);
  }

}
