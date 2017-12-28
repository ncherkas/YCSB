package com.yahoo.ycsb.db;

import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Filter;
import com.tangosol.util.QueryHelper;
import com.tangosol.util.filter.LimitFilter;
import com.yahoo.ycsb.*;

import java.util.*;

/**
 * YCSB Oracle Coherence client.
 */
public class CoherenceClient extends DB {

  private Map<String, NamedCache<String, Map<String, String>>> cacheRegistry = new HashMap<>();
  private boolean indexCreated = false;

  public void init() throws DBException {

  }

  private void createIndex(String name) {
    if (!indexCreated) {
      getNamedCache(name).addIndex(QueryHelper.createExtractor("key()"), true, null);
      indexCreated = true;
    }
  }

  private NamedCache<String, Map<String, String>> getNamedCache(String name) {
    NamedCache<String, Map<String, String>> namedCache = cacheRegistry.get(name);
    if (namedCache == null) {
      namedCache = CacheFactory.getCache(name);
      cacheRegistry.put(name, namedCache);
    }
    return namedCache;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, String> data = getNamedCache(table).get(key);
    if (data != null) {
      if (fields == null || fields.isEmpty()) {
        StringByteIterator.putAllAsByteIterators(result, data);
      } else {
        for (String field : fields) {
          result.put(field, new StringByteIterator(data.get(field)));
        }
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    createIndex(table);

    Filter queryFilter = QueryHelper.createFilter("key() >= ?1", new Object[]{startkey});
    @SuppressWarnings("unchecked")
    Filter limitFilter = new LimitFilter(queryFilter, recordcount); // TODO: check behaviour, not clear
    Collection<Map<String, String>> values = getNamedCache(table).values(limitFilter);
    for (Map<String, String> value : values) {
      result.add((HashMap<String, ByteIterator>) StringByteIterator.getByteIteratorMap(value));
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    createIndex(table);

    NamedCache<String, Map<String, String>> namedCache = getNamedCache(table);
    Map<String, String> oldValue = namedCache.get(key);
    Map<String, String> newValue = StringByteIterator.getStringMap(values);
    oldValue.putAll(newValue);

    namedCache.put(key, oldValue);

//    Let's use simpler method for now

//    ValueUpdater<Map<String, String>, Map<String, String>> valueUpdater = new MapValueUpdater();
//    getNamedCache(table).invoke(key, new UpdaterProcessor<String, Map<String, String>, Map<String, String>>(valueUpdater, StringByteIterator.getStringMap(values)));

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    createIndex(table);

    getNamedCache(table).put(key, StringByteIterator.getStringMap(values)); // TODO: not set() ?

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    getNamedCache(table).remove(key);
    return Status.OK;
  }

  public void cleanup() {
    CacheFactory.shutdown();
    cacheRegistry.clear();
  }

// Disabled due to an issue with serialization

//  @Portable
//  private static class MapValueUpdater implements ValueUpdater<Map<String, String>, Map<String, String>> {
//
//    @Override
//    public void update(Map<String, String> target, Map<String, String> value) {
//      for (Map.Entry<String, String> entry : value.entrySet()) {
//        target.put(entry.getKey(), entry.getValue());
//      }
//    }
//  }
}
