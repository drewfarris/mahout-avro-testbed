/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.hadoop.io.serializer;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * <p>
 * Encapsulates a {@link SerializerBase}/{@link DeserializerBase} pair.
 * </p>
 * 
 * @param <T>
 */
public abstract class SerializationBase<T> extends Configured
  implements Serialization<T> {
    
  public static final String SERIALIZATION_KEY = "Serialization-Class";
  public static final String SERIALIZATION_METADATA_KEYS = "Serialization-Metadata-Keys";
  
  public static final String CLASS_KEY = "Serialized-Class";
  
  public static Map<String, String> getMetadataFromClass(Configuration conf, Class<?> c) {
    Map<String, String> metadata = new HashMap<String, String>();
    metadata.put(CLASS_KEY, c.getName());
    
    loadClassMetadataFromConf(conf, c.getName(), metadata);
    
    return metadata;
  }
  
  /** Workaround for the fact the {@link SerializationFactory} in hadoop 0.20 will
   *  only call {@link #accept(Class)} for each serialization. The avro serializers
   *  will use this to load metadata from the configuration.
   * @param metadata
   */
  protected static void loadClassMetadataFromConf(Configuration conf, String className, Map<String, String> metadata) {
    String prefix = className + ".";
    String classKeys = conf.get(prefix + SERIALIZATION_METADATA_KEYS);
    if (classKeys != null) {
      for (String key: classKeys.split(",\\s*")) {
        String value = conf.get(prefix + key);
        
        // copy the value from conf to metadata if the key isn't already present.
        if (value != null && !metadata.containsKey(key)) {
          metadata.put(key, value);
        }
      }
    }
  }
  
  /**
   * Instantiates a map view over a subset of the entries in
   * the Configuration. This is instantiated by getMap(), which
   * binds a prefix of the namespace to the ConfigItemMap. This
   * mapping reflects changes to the underlying Configuration.
   *
   * This map does not support iteration.
   */
  protected static class ConfigItemMap extends AbstractMap<String, String>
      implements Map<String, String> {

    private final String prefix;
    private final Configuration conf;
    
    public ConfigItemMap(String prefix, Configuration conf) {
      this.prefix = prefix;
      this.conf = conf;
    }

    @Override
    public boolean containsKey(Object key) {
      return lookup(key.toString()) != null;
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet() {
      throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean equals(Object o) {
      return o != null && o instanceof ConfigItemMap
          && prefix.equals(((ConfigItemMap) o).prefix)
          && this.conf == ((ConfigItemMap) o).getConfiguration();
    }

    private Configuration getConfiguration() {
      return this.conf;
    }

    @Override
    public String get(Object key) {
      if (null == key) {
        return null;
      }

      return lookup(key.toString());
    }

    @Override
    public int hashCode() {
      return prefix.hashCode();
    }

    @Override
    public String put(String key, String val) {
      if (null == key) {
        return null;
      }

      String ret = get(key);
      conf.set(prefix + key, val);
      return ret;
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
      for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    private String lookup(String subKey) {
      String configKey = prefix + subKey;
      return conf.get(configKey);
    }
  }

  /**
   * Given a string -&gt; string map as a value, embed this in the
   * Configuration by prepending 'name' to all the keys in the valueMap,
   * and storing it inside the current Configuration.
   *
   * e.g., setMap("foo", { "bar" -&gt; "a", "baz" -&gt; "b" }) would
   * insert "foo.bar" -&gt; "a" and "foo.baz" -&gt; "b" in this
   * Configuration.
   *
   * @param name the prefix to attach to all keys in the valueMap. This
   * should not have a trailing "." character.
   * @param valueMap the map to embed in the Configuration.
   */
  public static void setConfigurationMap(Configuration conf, String name, Map<String, String> valueMap) {
    // Store all elements of the map proper.
    for (Map.Entry<String, String> entry : valueMap.entrySet()) {
      conf.set(name + "." + entry.getKey(), entry.getValue());
    }
  }

  /**
   * Returns a map containing a view of all configuration properties
   * whose names begin with "name.*", with the "name." prefix  removed.
   * e.g., if "foo.bar" -&gt; "a" and "foo.baz" -&gt; "b" are in the
   * Configuration, getMap("foo") would return { "bar" -&gt; "a",
   * "baz" -&gt; "b" }.
   *
   * Map name deprecation is handled via "prefix deprecation"; the individual
   * keys created in a configuration by inserting a map do not need to be
   * individually deprecated -- it is sufficient to deprecate the 'name'
   * associated with the map and bind that to a new name. e.g., if "foo"
   * is deprecated for "newfoo," and the configuration contains entries for
   * "newfoo.a" and "newfoo.b", getMap("foo") will return a map containing
   * the keys "a" and "b".
   *
   * The returned map does not support iteration; it is a lazy view over
   * the slice of the configuration whose keys begin with 'name'. Updates
   * to the underlying configuration are reflected in the returned map,
   * and updates to the map will modify the underlying configuration.
   *
   * @param name The prefix of the key names to extract into the output map.
   * @return a String-&gt;String map that contains all (k, v) pairs
   * where 'k' begins with 'name.'; the 'name.' prefix is removed in the output.
   */
  public static Map<String, String> getConfigurationMap(Configuration conf, String name) {
    String prefix = name + ".";
    return new ConfigItemMap(prefix, conf);
  }
  
  @Deprecated
  @Override
  public boolean accept(Class<?> c) {
    return accept(getMetadataFromClass(getConf(), c));
  }

  @Deprecated
  @Override
  public Deserializer<T> getDeserializer(Class<T> c) {
    return getDeserializer(getMetadataFromClass(getConf(), c));
  }

  @Deprecated
  @Override
  public Serializer<T> getSerializer(Class<T> c) {
    return getSerializer(getMetadataFromClass(getConf(), c));
  }

  /**
   * Allows clients to test whether this {@link SerializationBase} supports the
   * given metadata.
   */
  public abstract boolean accept(Map<String, String> metadata);

  /**
   * @return a {@link SerializerBase} for the given metadata.
   */
  public abstract SerializerBase<T> getSerializer(Map<String, String> metadata);

  /**
   * @return a {@link DeserializerBase} for the given metadata.
   */
  public abstract DeserializerBase<T> getDeserializer(
      Map<String, String> metadata);

  public Class<?> getClassFromMetadata(Map<String, String> metadata) {
    String classname = metadata.get(CLASS_KEY);
    if (classname == null) {
      return null;
    }
    try {
      return getConf().getClassByName(classname);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Provide a raw comparator for the specified serializable class.
   * Requires a serialization-specific metadata entry to name the class
   * to compare (e.g., "Serialized-Class" for JavaSerialization and
   * WritableSerialization).
   * @param metadata a set of string mappings providing serialization-specific
   * arguments that parameterize the data being serialized/compared.
   * @return a {@link RawComparator} for the given metadata.
   * @throws UnsupportedOperationException if it cannot instantiate a RawComparator
   * for this given metadata.
   */
  public abstract RawComparator<T> getRawComparator(Map<String,String> metadata);

  /**
   * Check that the SERIALIZATION_KEY, if set, matches the current class.
   * @param metadata the serialization metadata to check.
   * @return true if SERIALIZATION_KEY is unset, or if it matches the current class
   * (meaning that accept() should continue processing), or false if it is a mismatch,
   * meaning that accept() should return false.
   */
  protected boolean checkSerializationKey(Map<String, String> metadata) {
    String intendedSerializer = metadata.get(SERIALIZATION_KEY);
    return intendedSerializer == null ||
        getClass().getName().equals(intendedSerializer);
  }
}
