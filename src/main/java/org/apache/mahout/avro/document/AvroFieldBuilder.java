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

import org.apache.avro.util.Utf8;

public class AvroFieldBuilder {
  String name;
  String originalText;
  Object value;
  
  public void reset() {
    name = null;
    originalText = null;
    value = null;
  }
  
  public AvroField create() throws IllegalStateException {
    if (name == null) 
      throw new IllegalStateException("Field name must not be null");
    
    final AvroField f = new AvroField();
    f.set(0, new Utf8(name));
    
    if (originalText != null)
      f.set(1, new Utf8(originalText));
    
    if (value != null) 
      f.set(2, value);
    
    reset();
    
    return f;
  }
  
  public AvroFieldBuilder withName(String name) {
    this.name = name;
    
    return this;
  }
  
  public AvroFieldBuilder withOriginalText(String text) {
    this.originalText = text;
    
    return this;
  }
  
  public AvroFieldBuilder withValue(Object value) {
    this.value = value;
    
    return this;
  }
}
