package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(enabled = true)
public class CoherenceClientIntegrationTest {

  private static final String TABLE_NAME = "YcsbTestCache";

  private CoherenceClient coherenceClient;

  @BeforeClass
  public void setup() {
    System.setProperty("tangosol.coherence.cacheconfig", "tangosol-java-client-config.xml"); // TODO: check if it's needed

    this.coherenceClient = new CoherenceClient();
  }

  @Test
  public void testCrud() {
    // Insertion
    coherenceClient.insert(TABLE_NAME, "k1", Collections.<String, ByteIterator>singletonMap("f1", new StringByteIterator("v1")));

    // Reading
    Map<String, ByteIterator> readResult = new HashMap<>();
    Status status = coherenceClient.read(TABLE_NAME, "k1", singleton("f1"), readResult);
    assertTrue(status.isOk());
    assertEquals(readResult.size(), 1);
    assertNotNull(readResult.get("f1"));
    assertEquals(readResult.get("f1").toString(), "v1");
    readResult.clear();

    // Updating
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("f1", new StringByteIterator("v2"));
    values.put("f2", new StringByteIterator("v2"));
    status = coherenceClient.update(TABLE_NAME, "k1", values);
    assertTrue(status.isOk());

    // Reading again
    status = coherenceClient.read(TABLE_NAME, "k1", new HashSet<>(asList("f1", "f2")), readResult);
    assertTrue(status.isOk());
    assertEquals(readResult.size(), 2);
    assertNotNull(readResult.get("f1"));
    assertEquals(readResult.get("f1").toString(), "v2");
    assertNotNull(readResult.get("f2"));
    assertEquals(readResult.get("f2").toString(), "v2");
    readResult.clear();

    // Deleting
    status = coherenceClient.delete(TABLE_NAME, "k1");
    assertTrue(status.isOk());

    // Checking
    status = coherenceClient.read(TABLE_NAME, "k1", singleton("f1"), readResult);
    assertTrue(status.isOk());
    assertTrue(readResult.isEmpty());
  }

  @Test
  public void testScan() {
    int rowCount = 5;

    // Insertion
    for (int i = 0; i < rowCount; i++) {
      coherenceClient.insert(TABLE_NAME, "k" + i, Collections.<String, ByteIterator>singletonMap("f1", new StringByteIterator("v" + i)));
    }

    Vector<HashMap<String, ByteIterator>> scanResult = new Vector<>();
    try {
      // Scan
      Status scanStatus = coherenceClient.scan(TABLE_NAME, "k1", rowCount, singleton("f1"), scanResult);
      assertTrue(scanStatus.isOk());
      assertEquals(scanResult.size(), rowCount);

      assertEquals(scanResult.get(0).get("f1").toString(), "v5");
      assertEquals(scanResult.get(1).get("f1").toString(), "v4");
      // TODO: fix it
      assertEquals(scanResult.get(2).get("f1").toString(), "v3");
      assertEquals(scanResult.get(3).get("f1").toString(), "v2");
      assertEquals(scanResult.get(4).get("f1").toString(), "v1");
    } finally {
      // Deleting
      for (int i = 0; i < rowCount; i++) {
        coherenceClient.delete(TABLE_NAME, "k" + i);
      }
    }
  }

  @AfterClass
  public void tearDown() {
    try {
      this.coherenceClient.cleanup();
    } finally {
      System.clearProperty("tangosol.coherence.cacheconfig"); // TODO: check if it's needed
    }
  }
}
