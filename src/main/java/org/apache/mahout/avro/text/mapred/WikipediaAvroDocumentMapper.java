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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;
import org.apache.mahout.avro.document.AvroDocument;
import org.apache.mahout.avro.document.AvroDocumentBuilder;
import org.apache.mahout.avro.document.AvroFieldBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps over Wikipedia xml format and output all document having the category
 * listed in the input category file
 * 
 */
public class WikipediaAvroDocumentMapper extends MapReduceBase implements
    Mapper<LongWritable,Text,Text,AvroDocument> {
  
  private static final Logger log =
      LoggerFactory.getLogger(WikipediaAvroDocumentMapper.class);
  
  private static final Pattern SPACE_NON_ALPHA_PATTERN = Pattern.compile("[\\s]");
  private static final String  START_DOC = "<text xml:space=\"preserve\">";
  private static final String  END_DOC = "</text>";
  private static final Pattern TITLE = Pattern.compile("<title>(.*)<\\/title>");
  
  private static final String REDIRECT = "<redirect />";
  private Set<String> inputCategories;
  private boolean exactMatchOnly;
  private boolean all;
  
  @Override
  public void map(LongWritable key,
                  Text value,
                  OutputCollector<Text,AvroDocument> output,
                  Reporter reporter) throws IOException {
    
    String content = value.toString();
    if (content.contains(REDIRECT)) return;
    String document = "";
    String title = "";
    try {
      document = getDocument(content);
      title = getTitle(content);
    } catch (Exception e) {
      reporter.getCounter("Wikipedia", "Parse errors").increment(1);
      return;
    }
    
    if (!all) {
      String catMatch = findMatchingCategory(document);
      if (catMatch.equals("Unknown")) return;
    }
    document = StringEscapeUtils.unescapeHtml(document);
    title = SPACE_NON_ALPHA_PATTERN.matcher(title).replaceAll("_");
    
    AvroDocumentBuilder db = new AvroDocumentBuilder();
    AvroFieldBuilder fb = new AvroFieldBuilder();
    
    AvroDocument ad = db.withDocId(title).withField(
        fb.withName("title").withOriginalText(title).create()
      ).withField(
        fb.withName("body").withOriginalText(document).create()
      ).create();

    output.collect(new Text(title), ad);
  }
  
  private static String getDocument(String xml) {
    int start = xml.indexOf(START_DOC) + START_DOC.length();
    int end = xml.indexOf(END_DOC, start);
    return xml.substring(start, end);
  }
  
  private static String getTitle(String xml) {
    Matcher m = TITLE.matcher(xml);
    String ret = "";
    if (m.find()) {
      ret = m.group(1);
    }
    return ret;
  }
  
  private String findMatchingCategory(String document) {
    int startIndex = 0;
    int categoryIndex;
    while ((categoryIndex = document.indexOf("[[Category:", startIndex)) != -1) {
      categoryIndex += 11;
      int endIndex = document.indexOf("]]", categoryIndex);
      if (endIndex >= document.length() || endIndex < 0) {
        break;
      }
      String category =
          document.substring(categoryIndex, endIndex).toLowerCase().trim();
      // categories.add(category.toLowerCase());
      if (exactMatchOnly && inputCategories.contains(category)) {
        return category;
      } else if (exactMatchOnly == false) {
        for (String inputCategory : inputCategories) {
          if (category.contains(inputCategory)) { // we have an inexact match
            return inputCategory;
          }
        }
      }
      startIndex = endIndex;
    }
    return "Unknown";
  }
  
  @Override
  public void configure(JobConf job) {
    try {
      if (inputCategories == null) {
        Set<String> newCategories = new HashSet<String>();
        
        DefaultStringifier<Set<String>> setStringifier =
            new DefaultStringifier<Set<String>>(job, GenericsUtil
                .getClass(newCategories));
        
        String categoriesStr = setStringifier.toString(newCategories);
        categoriesStr = job.get("wikipedia.categories", categoriesStr);
        inputCategories = setStringifier.fromString(categoriesStr);
        
      }
      exactMatchOnly = job.getBoolean("exact.match.only", false);
      all = job.getBoolean("all.files", true);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
    log.info("Configure: Input Categories size: "
        + inputCategories.size()
        + " All: "
        + all
        + " Exact Match: "
        + exactMatchOnly);
  }
}
