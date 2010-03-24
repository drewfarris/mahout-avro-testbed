package org.apache.mahout.hadoop.io.serializer.avro;

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
public class AvroRecord extends SpecificRecordBase implements SpecificRecord {
  public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"AvroRecord\",\"namespace\":\"org.apache.mahout.hadoop.io.serializer.avro\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\"}]}");
  public int intField;
  public Schema getSchema() { return _SCHEMA; }
  public Object get(int _field) {
    switch (_field) {
    case 0: return intField;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void set(int _field, Object _value) {
    switch (_field) {
    case 0: intField = (Integer)_value; break;
    default: throw new AvroRuntimeException("Bad index");
    }
  }
}
