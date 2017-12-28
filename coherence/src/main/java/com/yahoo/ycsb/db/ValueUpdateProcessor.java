package com.yahoo.ycsb.db;

import com.tangosol.util.InvocableMap;
import com.tangosol.util.ValueUpdater;
import com.tangosol.util.processor.AbstractProcessor;

import java.util.Map;
import java.util.Objects;

/**
 * Not used as it's not clear how to upload it into a running cluster.
 */
public class ValueUpdateProcessor extends AbstractProcessor<String, Map<String, String>, Void> {

  public static final ValueUpdater<Map<String, String>, Map<String, String>> VALUE_UPDATER = new ValueUpdaterImpl();

  private final Map<String, String> fields;

  public ValueUpdateProcessor(Map<String, String> fields) {
    this.fields = Objects.requireNonNull(fields);
  }

  @Override
  public Void process(InvocableMap.Entry<String, Map<String, String>> entry) {
    entry.update(VALUE_UPDATER, fields);
    return null;
  }

  private static class ValueUpdaterImpl implements ValueUpdater<Map<String, String>, Map<String, String>> {

    @Override
    public void update(Map<String, String> target, Map<String, String> value) {
      for (Map.Entry<String, String> entry : value.entrySet()) {
        target.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
