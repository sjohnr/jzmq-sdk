package org.zeromq.support;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public class ZmqUtilsTest {

  @Test
  public void t0() {
    byte[] bytes = mergeBytes(Arrays.asList("AAA".getBytes(),
                                            "BBB".getBytes(),
                                            "CCC".getBytes()));
    // AAA
    assertEquals(65, bytes[0]);
    assertEquals(65, bytes[1]);
    assertEquals(65, bytes[2]);
    // BBB
    assertEquals(66, bytes[3]);
    assertEquals(66, bytes[4]);
    assertEquals(66, bytes[5]);
    // CCC
    assertEquals(67, bytes[6]);
    assertEquals(67, bytes[7]);
    assertEquals(67, bytes[8]);
  }
}
