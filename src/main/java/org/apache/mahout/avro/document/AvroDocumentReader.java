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

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.generic.GenericArray;

public class AvroDocumentReader {
  
  AvroDocument document;
  GenericArray<AvroField> fields;
  final Map<String, AvroField> fieldMap;
  
  public AvroDocumentReader() {
    fieldMap = new HashMap<String, AvroField>();
  }
  
  public AvroDocumentReader(AvroDocument d) {
    this();
    wrap(d);
  }

  public AvroDocument getDocument() {
    return document;
  }

  public AvroDocumentReader wrap(AvroDocument document) {
    this.document = document;
    fieldMap.clear();
    fields = (GenericArray<AvroField>) document.get(1);
    for (AvroField f: fields) {
      fieldMap.put(f.name.toString(), f);
    }
    return this;
  }
  
  public Object getDocId() {
    return document.get(0);
  }
  
  public GenericArray<AvroField> getFields() {
    return fields;
  }
  
  public Iterator<AvroField> fieldIterator() {
    return fields.iterator();
  }
  
  public AvroField getField(String name) {
    return fieldMap.get(name);
  }
  
  public static void dumpAvroDocument(AvroDocument d, PrintStream out) {
    out.println("docId: " + d.get(0));
    GenericArray<AvroField> fa = (GenericArray<AvroField>) d.get(1);
    if (fa != null) {
      int pos = 0;
      for (AvroField f: fa) {
        AvroFieldReader.dumpAvroField(pos++, f, out);
      }
    } 
  }
}
