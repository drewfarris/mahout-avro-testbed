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
package org.apache.mahout.avro.util;

import java.io.File;
import java.io.IOException;

import org.apache.avro.specific.SpecificCompiler;

/** Compiles the avro schema to java classes 
 *  TODO: replace with a maven plugin, possibly:
 *  http://github.com/phunt/avro-maven-plugin
 *
 */
public class AvroDocumentCompiler {
  public static void main(String[] args) throws IOException {
    File src = new File("src/main/schemata/org/apache/mahout/avro/AvroDocument.avsc");
    File dest = new File("src/main/java");
    dest.mkdirs();
    SpecificCompiler.compileSchema(src, dest);
  }
}
