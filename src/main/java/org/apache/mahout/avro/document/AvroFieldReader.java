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

public class AvroFieldReader {
  
  AvroField field;
  
  public AvroFieldReader() {
    
  }
  
  public AvroFieldReader(AvroField f) {
    wrap(f);
  }
  
  public AvroFieldReader wrap(AvroField f) {
    field = f;
    return this;
  }
  
  public String getName() {
    return field.get(0).toString();
  }
  
  public String getOriginalText() {
    return field.get(1).toString();
  }
  
  public Object getValue() {
    return field.get(2);
  }
  
  public static void dumpAvroField(int pos, AvroField d, PrintStream out) {
    String name = d.get(0).toString();
  
    Object value = d.get(2);
    out.println("\t" + pos + ". " + name);
    
    if (d.get(1) != null) {
      String originalText = d.get(1).toString();
      out.println("\t\torigText: " + originalText);
    }
    else {
      out.println("\t\tvalue: null-value");
    }
    
    if (d.get(2) != null) {
      out.println("\t\tvalue: " + value.toString());
    }
    else {
      out.println("\t\tvalue: null-value");
    }
  }
}
