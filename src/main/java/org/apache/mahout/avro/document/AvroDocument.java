package org.apache.mahout.avro.document;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificExceptionBase;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.reflect.FixedSize;

@SuppressWarnings("all")
public class AvroDocument extends SpecificRecordBase implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"AvroDocument\",\"namespace\":\"org.apache.mahout.avro.document\",\"fields\":[{\"name\":\"docid\",\"type\":[\"null\",\"int\",\"string\"]},{\"name\":\"fields\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"AvroField\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"originalText\",\"type\":[\"null\",\"string\"]},{\"name\":\"value\",\"type\":[\"null\",\"int\",\"long\",\"double\",\"string\",{\"type\":\"array\",\"items\":[\"int\",\"long\",\"double\",\"string\"]}]}]}}}]}");
  public Object docid;
  public GenericArray<AvroField> fields;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return docid;
    case 1: return fields;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    switch (_field) {
    case 0: docid = (Object)_value; break;
    case 1: fields = (GenericArray<AvroField>)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
}
