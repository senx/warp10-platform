package io.warp10.continuum.egress;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.store.thrift.data.Metadata;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class EgressFetchHandlerTest {
  private static ClassLoader CL = EgressFetchHandlerTest.class.getClassLoader();

  @Test
  public void testJsonDump() throws Exception {
    GTSEncoder gts = new GTSEncoder();
    gts.getMetadata()
            .setName("my.class")
            .setLabels(singletonMap("foo", "bar"))
            .setAttributes(singletonMap("foo", "baz"))
            .setLastActivity(1234);
    gts.addValue(5678, 0, 0, 3.14);
    Iterator<GTSDecoder> iter = singleton(gts.getDecoder()).iterator();

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (PrintWriter pw = new PrintWriter(buf)) { // auto flush
      EgressFetchHandler.jsonDump(pw, iter, 10000, -1, false, false,
                                    new AtomicReference<Metadata>(null), new AtomicLong(0));
    }

    String json = buf.toString("UTF-8");
    String expect = IOUtils.toString(CL.getResourceAsStream("EgressFetchHandlerTest_1.json"), "UTF-8");

    assertEquals(expect.trim(), json);
  }

}
