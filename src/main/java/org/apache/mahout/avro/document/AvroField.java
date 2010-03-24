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
public class AvroField extends SpecificRecordBase implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"AvroField\",\"namespace\":\"org.apache.mahout.avro.document\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"originalText\",\"type\":[\"null\",\"string\"]},{\"name\":\"value\",\"type\":[\"null\",\"int\",\"long\",\"double\",\"string\",{\"type\":\"array\",\"items\":[\"int\",\"long\",\"double\",\"string\"]}]}]}");
  public Utf8 name;
  public Utf8 originalText;
  public Object value;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return name;
    case 1: return originalText;
    case 2: return value;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    switch (_field) {
    case 0: name = (Utf8)_value; break;
    case 1: originalText = (Utf8)_value; break;
    case 2: value = (Object)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
}
