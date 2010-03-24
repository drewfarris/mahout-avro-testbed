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
package org.apache.mahout.avro.document;

import java.util.LinkedList;
import java.util.List;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.util.Utf8;

public class AvroDocumentBuilder {

  Object docId;
  List<AvroField> fields;
  
  public void reset() {
    this.docId = null;
    this.fields = null;
  }
  
  public AvroDocumentBuilder withDocId(int docId) {
    this.docId = Integer.valueOf(docId);
    return this;
  }
  
  public AvroDocumentBuilder withDocId(String string) {
    this.docId = new Utf8(string);
    return this;
  }
  
  public AvroDocumentBuilder withField(AvroField field) {
    if (fields == null) {
      fields = new LinkedList<AvroField>();
    }
    fields.add(field);
    return this;
  }
  
  public AvroDocument create() throws IllegalStateException {
    if (docId == null) {
      throw new IllegalStateException("No docId was specified");
    }
    
    final AvroDocument d = new AvroDocument();
    d.set(0, docId);
    
    if (fields != null && !fields.isEmpty()) {
      GenericArray<AvroField> ga = new Array<AvroField>(fields.size(), AvroDocument._SCHEMA);
      for (AvroField f: fields) {
        ga.add(f);
      }
      d.set(1, ga);
    }

    reset();
    
    return d;
  }
}
