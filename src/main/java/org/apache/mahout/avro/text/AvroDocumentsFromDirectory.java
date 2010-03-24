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

package org.apache.mahout.avro.text;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentBuilder;
import org.apache.mahout.avro.document.AvroFieldBuilder;
import org.apache.mahout.common.FileLineIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a directory of text documents into a avro files of Specified
 * chunkSize. This class takes in a parent directory containing sub folders of
 * text documents and recursively reads the files and creates the
 * avro files. The docid is set as the relative path of the document from 
 * the parent directory prepended with a specified prefix. You can also
 * specify the input encoding of the text files. The content of the output 
 * documents are encoded as UTF-8 text.
 * 
 * 
 */
public final class AvroDocumentsFromDirectory {

  private static final Logger log = LoggerFactory.getLogger(AvroDocumentsFromDirectory.class);
  public static final int DEFAULT_CHUNK_SIZE = 5000;
  
  private static ChunkedWriter createNewChunkedWriter(Configuration conf, int documentsPerChunk,
      String outputDir) throws IOException {
    return new ChunkedWriter(conf, documentsPerChunk, outputDir);
  }
  
  public void createAvroDocuments(Configuration conf,
                                  File parentDir,
                                  String outputDir,
                                  String prefix,
                                  int documentsPerChunk,
                                  Charset charset) throws IOException {
    
    if (!parentDir.isDirectory()) {
      throw new IllegalArgumentException("Parent directory " + parentDir.getAbsolutePath() + " does not exist.");
    }
    
    log.info("Reading files with prefix '{}' from '{}', writing to '{}'", new Object[]{ prefix, parentDir, outputDir });
    
    ChunkedWriter writer = createNewChunkedWriter(conf, documentsPerChunk, outputDir);
    parentDir.listFiles(new PrefixAdditionFilter(prefix, writer, charset));
    writer.close();
  }
  
  public static class ChunkedWriter implements Closeable {

    private final int documentsPerChunk;
    private final Path output;
    private final DatumWriter<Object> datumWriter;
    private DataFileWriter<Object> dfw;
    private OutputStream os;
    private int currentChunkID;
    private int currentDocuments;
    private final Configuration conf;
    private final FileSystem fs;
    private long lastOpenTime;
    
    public ChunkedWriter(Configuration conf, int documentsPerChunk, String outputDir) throws IOException {
      this.conf = conf;
      this.documentsPerChunk = documentsPerChunk;
      output = new Path(outputDir);
      fs = FileSystem.get(conf);
      fs.mkdirs(output);
      
      if (!fs.getFileStatus(output).isDir()) {
          throw new IOException("Unable to create or open output directory");
      }
      lastOpenTime = System.currentTimeMillis();
      currentChunkID = 0;
      datumWriter = new SpecificDatumWriter(AvroDocument.class);
      
      openNextChunk();
    }
    
    private void openNextChunk() throws IOException {
      Path p = getPath(currentChunkID++);
      log.info("Opening {}", p);
      
      os = fs.create(p);
      dfw = new DataFileWriter<Object>(AvroDocument._SCHEMA, os, datumWriter);
      currentDocuments = 0;
      lastOpenTime = System.currentTimeMillis();
    }
    
    private Path getPath(int chunkID) {
      return new Path(output, "chunk-" + chunkID);
    }
    
    public void write(AvroDocument value) throws IOException {
      if (currentDocuments >= documentsPerChunk && documentsPerChunk > 0) {
        close();
        openNextChunk();
      }

      dfw.append(value);
      currentDocuments++;
    }
    
    @Override
    public void close() throws IOException {
      long delta = System.currentTimeMillis() - lastOpenTime;
      log.info("close(): Wrote {} documents in {} msec", currentDocuments, delta);
      
      dfw.flush();
      dfw.close();
      os.close();
    }
  }
  
  public class PrefixAdditionFilter implements FileFilter {
    private final String prefix;
    private final ChunkedWriter writer;
    private final Charset charset;

    public PrefixAdditionFilter(String prefix,
                                ChunkedWriter writer,
                                Charset charset) {
      this.prefix = prefix;
      this.writer = writer;
      this.charset = charset;
    }
    
    @Override
    public boolean accept(File current) {
      if (current.isDirectory()) {
        current.listFiles(new PrefixAdditionFilter(prefix
            + File.separator
            + current.getName(), writer, charset));
      } else {
        try {
          if (log.isDebugEnabled()) {
            log.info("accept(): reading {}", current.getAbsolutePath());
          }
          StringBuilder file = new StringBuilder();
          for (String aFit : new FileLineIterable(current, charset, false)) {
            file.append(aFit).append('\n');
          }
          
          AvroDocumentBuilder b = new AvroDocumentBuilder();
          AvroFieldBuilder f = new AvroFieldBuilder();
          
          AvroDocument d = 
            b.withDocId(prefix + File.separator + current.getName())
              .withField(
                  f.withName("content").withOriginalText(file.toString()).create()
               ).create();

          writer.write(d);
        } catch (FileNotFoundException e) {
          // Skip file.
        } catch (IOException e) {
          // TODO: report exceptions and continue;
          throw new IllegalStateException(e);
        }
      }
      return false;
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser p = new GenericOptionsParser(conf, args);
    args = p.getRemainingArgs();
    
    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();
    
    Option parentOpt =
        obuilder.withLongName("parent").withRequired(true).withArgument(
            abuilder.withName("parent").withMinimum(1).withMaximum(1).create())
            .withDescription("Parent dir containing the documents")
            .withShortName("p").create();
    
    Option outputDirOpt =
        obuilder.withLongName("outputDir").withRequired(true).withArgument(
            abuilder.withName("outputDir").withMinimum(1).withMaximum(1)
                .create()).withDescription("The output directory")
            .withShortName("o").create();
    
    Option chunkSizeOpt =
        obuilder.withLongName("chunkSize").withArgument(
            abuilder.withName("chunkSize").withMinimum(1).withMaximum(1)
                .create()).withDescription(
            "The chunkSize in documents. Defaults to " + DEFAULT_CHUNK_SIZE)
            .withShortName("chunk").create();
    
    Option keyPrefixOpt =
        obuilder.withLongName("keyPrefix").withArgument(
            abuilder.withName("keyPrefix").withMinimum(1).withMaximum(1)
                .create()).withDescription(
            "The prefix to be prepended to the key").withShortName("prefix")
            .create();
    
    Option charsetOpt =
        obuilder.withLongName("charset").withRequired(true)
            .withArgument(
                abuilder.withName("charset").withMinimum(1).withMaximum(1)
                    .create()).withDescription(
                "The name of the character encoding of the input files")
            .withShortName("c").create();
    
    Group group =
        gbuilder.withName("Options").withOption(keyPrefixOpt).withOption(
            chunkSizeOpt).withOption(charsetOpt).withOption(outputDirOpt)
            .withOption(parentOpt).create();
    
    Parser parser = new Parser();
    parser.setGroup(group);
    CommandLine cmdLine = parser.parse(args);
    
    File parentDir = new File((String) cmdLine.getValue(parentOpt));
    String outputDir = (String) cmdLine.getValue(outputDirOpt);
    
    int documentsPerChunk = DEFAULT_CHUNK_SIZE;
    if (cmdLine.hasOption(chunkSizeOpt)) {
      documentsPerChunk = Integer.parseInt((String) cmdLine.getValue(chunkSizeOpt));
    }
    
    String prefix = "";
    if (cmdLine.hasOption(keyPrefixOpt)) {
      prefix = (String) cmdLine.getValue(keyPrefixOpt);
    }
    Charset charset = Charset.forName((String) cmdLine.getValue(charsetOpt));
    AvroDocumentsFromDirectory dir = new AvroDocumentsFromDirectory();
    dir.createAvroDocuments(conf, parentDir, outputDir, prefix, documentsPerChunk, charset);
  }
}
