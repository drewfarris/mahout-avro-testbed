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
import java.io.StringReader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentReader;
import org.apache.mahout.avro.document.AvroField;
import org.apache.mahout.avro.document.AvroFieldReader;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.hadoop.io.serializer.avro.AvroComparator;
import org.apache.mahout.hadoop.io.serializer.avro.AvroGenericSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.mahout.hadoop.io.serializer.avro.AvroSpecificSerialization;
import org.apache.mahout.hadoop.mapred.AvroInputFormat;
import org.apache.mahout.hadoop.mapred.AvroOutputFormat;


/** Tokenize text found in document fields and write the tokens back to the
 *  documents.
 */
public class AvroDocumentProcessor extends Configured implements Tool {
 
  private static enum CounterGroup {
    TOKEN_COUNT, DOCUMENT_COUNT
  };

  public static class ProcessorMapper extends MapReduceBase
    implements Mapper<AvroDocument, LongWritable, AvroDocument, NullWritable> {

    private final AvroDocumentReader dr = new AvroDocumentReader();
    private final AvroFieldReader    fr = new AvroFieldReader();
    private final Analyzer           a  = new StandardAnalyzer();
    private final NullWritable       nw = NullWritable.get();
    
    @Override
    public void map(AvroDocument key, LongWritable value, OutputCollector<AvroDocument, NullWritable> collector, Reporter reporter)
        throws IOException {

      AvroField content = dr.wrap(key).getField("content");
      String text = fr.wrap(content).getOriginalText();

      //TODO: configurable analyzer.
      TokenStream stream = a.tokenStream("content", new StringReader(text));
      TermAttribute termAtt = (TermAttribute) stream.addAttribute(TermAttribute.class);
      StringTuple document = new StringTuple();
      while (stream.incrementToken()) {
        if (termAtt.termLength() > 0) {
          document.add(new String(termAtt.termBuffer(), 0, termAtt.termLength()));
        }
      }
      
      //TODO: there has to be a nicer way to do this.
      GenericArray<Utf8> tokens = new GenericData.Array<Utf8>(document.getEntries().size(), Schema.create(Schema.Type.STRING));
      for (String token: document.getEntries()) {
        tokens.add(new Utf8(token));
      }
      
      reporter.getCounter(CounterGroup.TOKEN_COUNT).increment(document.getEntries().size());
      reporter.getCounter(CounterGroup.DOCUMENT_COUNT).increment(1);

      //TODO: field writer? drop original text?
      content.set(2, tokens);
      collector.collect(key, nw);
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
    
    AvroComparator.setSchema(AvroDocument._SCHEMA); //TODO: must be done in mapper, reducer configure method.
    
    conf.setClass("mapred.output.key.comparator.class", AvroComparator.class, RawComparator.class);
    
    conf.setJarByClass(AvroDocumentProcessor.class);
    conf.setMapperClass(ProcessorMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(AvroDocument.class);
    conf.setOutputValueClass(NullWritable.class);
    
    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
    
    AvroInputFormat.setAvroInputClass(conf, AvroDocument.class);
    AvroOutputFormat.setAvroOutputClass(conf, AvroDocument.class);
    
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    
    FileSystem fs = FileSystem.get(conf);
    fs.delete(output, true);
    
    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    
    RunningJob job = JobClient.runJob(conf);
    job.waitForCompletion();
    
    return job.isComplete() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new AvroDocumentProcessor(), args);
  }
}
