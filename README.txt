Provided are two different versions of AvroInputFormat/AvroOutputFormat that 
are compatible with the mapred (pre 0.21) and mapreduce (0.21+) apis. They are 
based on, code provided as a part of  MAPREDUCE-815 and other patches. 
Also provided are backports of the SerializationBase/AvroSerialization classes
from the current hadoop-core trunk.

AvroInputFormat/AvroOutputFormat usage:

mapred, pre 0.21 api:

Add serializations:

{code}
conf.setStrings("io.serializations",
        new String[] {
          WritableSerialization.class.getName(), 
          AvroSpecificSerialization.class.getName(), 
          AvroReflectSerialization.class.getName(),
          AvroGenericSerialization.class.getName()
        });
{code}

Setup input and output formats:

{code}
    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
    
    AvroInputFormat.setAvroInputClass(conf, AvroDocument.class);
    AvroOutputFormat.setAvroOutputClass(conf, AvroDocument.class);
{code}

AvroInputFormat provides the specified class as the key and a LongWritable file
offset as the value.

AvroOutputFormat expects the specified class as the key and expects a 
NullWritable as a value.

If an avro serializable class is passed between the map and reduce phases it is
necessary to set the following:

{code}
    AvroComparator.setSchema(AvroDocument._SCHEMA);
    conf.setClass("mapred.output.key.comparator.class", 
      AvroComparator.class, RawComparator.class);
{code}

mapreduce, post 0.21 api:

io.serializations and comparator, set as above, input and output formats are 
reasonably identical (e.g: job.setInputFormatClass(..) instead of 
conf.setInputFormat(...))

What's serialized?!

So far I've been using avro 'specific' serialization, which compiles an avro 
schema into a Java class. see 
src/main/schemata/org/apache/mahout/avro/AvroDocument.avsc. This is currently
compiled into classes o.a.m.avro.document.(AvroDocument|AvroField) using 
o.a.m.avro.util.AvroDocumentCompiler (eventually to be replaced by a maven 
plugin, Generated sources are currently checked in.).

Helper classes for AvroDocument and AvroField include 
o.a.m.avro.document.Avro(Document|Field)Builder, 
o.a.m.avro(Document|Field)Reader. This seems to work ok here, but I'm not 
certain that this is be best pattern to use, especially when there are many
pre-existing classes (such as there are in the case of vector. 

Avro also provides reflection-based serialization and schema-based 
serialization, both should be supported by the infrastructure that has been 
backported here, but that's something else to explore.
 
Examples:

These are quick and dirty and need much cleanup work before they can be taken 
out to the dance.

see o.a.m.avro.text, o.a.m.avro.text.mapred and o.a.m.avro.text.mapreduce:

* AvroDocumentsFromDirectory: quick and dirty port of SequenceFilesFromDirectory to use AvroDocuments. Writes a file containing documents in avro format; file contents is stored in a single field named 'content', contents are stored in the originalText portion of this field.
* AvroDocumentsDumper: dump an avro documents file to a standard output
* AvroDocumentsWordCount: perform a wordcount on an avro document input file.
* AvroDocumentProcessor: tokenizes the text found in the input document file, reads from the originalText of the field named content and writes original document+tokens to output file.

Running the examples:

Here's some example command-lines:

{code}
mvn exec:java -Dexec.mainClass=org.apache.mahout.avro.text.AvroDocumentsFromDirectory -Dexec.args='--parent /home/drew/mahout/20news-18828 --outputDir /home/drew/mahout/20news-18828-example --charset UTF-8'
mvn exec:java -Dexec.mainClass=org.apache.mahout.avro.text.mapred.AvroDocumentProcessor -Dexec.args='/home/drew/mahout/20news-18828-example /home/drew/mahout/20news-18828-processed'
mvn exec:java -Dexec.mainClass=org.apache.mahout.avro.text.AvroDocumentsDumper -Dexec.args='/home/drew/mahout/20news-18828-processed/.avro-r-00000' > foobar.txt
{code}

The Wikipedia stuff is in there, but isn't working yet. Many thanks (apologies)
 to Robin for the starting point for much of this code and hacking it to pieces
  so badly. 

Unit tests:

You can hardly call them that, but I did backport the serialization tests 
from hadoop-trunk.

Known bugs:

* AvroInputFormat isn't getting all of the documents it should be for the 18828 news example. Same problem exist for both mapred and mapreduce versions.
* mapred.AvroOutputFormat can't properly handle cases where a schema is used instead of a class.
* mapred.AvroDocumentProcessor produces some strangely named output files, e.g: .avro-r-00000 instead of part-00000

Notes regarding how SerializationBase/AvroSerialization and subclasses are plumbed into the hadoop 0.20 Serialization object.

* Hadoop 0.20 uses Serialization.accept(Class<?>)  to determine if a particular Serialization will serialize a class
* SerializationBase and subclasses expect SerializationBase.accept(Map<String,String> metadata) to determine if they will serialize a class (see HADOOP-6165)
* Values from the configuration are read to determine the values to pass into SerializationBase.accept (Map<String, String) based on the classname passed to Serialization.accept(Class<?> clazz). Implemented in loadClassMetadataFromConf:
   * The value of className + "." + SERIALIZATION_METADATA_KEYS is read from the configuration. This is used to determine the keys that should be put into the map.
   * For each key, the value of className + "." + keyname is read from the configuration. This is used to determine the value corresponding to each key.

* Keys used by the SerializationBase/AvroSerialization include:
   * SerializationBase.SERIALIZATION_CLASS: used to specify a specific serialization class for another class. SerializationBase.accept(Map<String,String>) will return true if the value of this key is non-null and the name of the class calling this method is equal to the value.
   * SerializationBase.CLASS_KEY: always inserted into the metadata Map<String,String> by loadClassMetadataFromConf: the class that is being serialized. 
   * AvroSerialization.SCHEMA_KEY: used to specify a schema (in string form), used by AvroGenericSerialization to determine the structure of the expected data.
   


