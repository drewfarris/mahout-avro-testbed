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
package org.apache.mahout.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentBuilder;
import org.apache.mahout.avro.document.AvroDocumentReader;
import org.apache.mahout.avro.document.AvroFieldBuilder;
import org.junit.Test;

/** demonstrate writing and reading of specific avro object type generated from
 *  schema */
public class AvroWriterReaderTest {

  public static AvroDocument createTestDocument() {
    AvroDocumentBuilder db = new AvroDocumentBuilder();
    AvroFieldBuilder fb = new AvroFieldBuilder();
    
    return db.withDocId("test").withField(
            // my obsession with palindromes started when I grew up in 07670
          fb.withName("palindrome").withOriginalText("A man, a plan, a canal, Panama!").create()
        ).withField(
            // oops, someone tripped over a frog.
          fb.withName("onomatopoea").withOriginalText("Bash crash bang slam thud rumble ribbit kerplunk").create()
        ).create();
  }
  
  @Test
  public void test() throws IOException {
    File file = new File("target/AvroDocument.avro");
    
    Schema schema = AvroDocument._SCHEMA;
    
    {  
      System.out.println("Writing to: " + file.getAbsolutePath());
      DatumWriter<Object> datumWriter = new SpecificDatumWriter(AvroDocument.class);
      FileOutputStream outputStream = new FileOutputStream(file);
      DataFileWriter<Object> dfw = new DataFileWriter<Object>(schema, outputStream, datumWriter);
      
      AvroDocument d = createTestDocument();
      dfw.append(d);
      dfw.flush();
      dfw.close();
    }

    {
      System.out.println("Reading from: " + file.getAbsolutePath());
      DatumReader<Object> datumReader = new SpecificDatumReader(AvroDocument.class);
      SeekableInput seekableInput = new SeekableFileInput(file);
      DataFileReader<Object> dfr = new DataFileReader<Object>(seekableInput, datumReader);
      AvroDocument d = new AvroDocument();
      dfr.next(d);
      AvroDocumentReader.dumpAvroDocument(d, System.out);
      
    }
  }
}
