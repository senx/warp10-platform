package io.warp10.continuum.gts;

import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class MetadataSelectorMatcherTest {

  @Test
  public void metaDataMatch() throws WarpScriptException {

    Metadata test = new Metadata();
    test.setName("temperature");

    Map labels = new HashMap();
    labels.put("sensor", "23");
    test.setLabels(labels);

    Map attributes = new HashMap();
    attributes.put("room", "A");
    test.setAttributes(attributes);


    MetadataSelectorMatcher x;

    x = new MetadataSelectorMatcher("~.*{toto~tata.*}{attr=yes,attr2~.false.*}");
    Assert.assertTrue(x.MetaDataMatch(test));

    x = new MetadataSelectorMatcher("~temp.*{sensor~(23|22),room=B}{}");
    Assert.assertTrue(x.MetaDataMatch(test));

    x = new MetadataSelectorMatcher("~temp.*{sensor~(23|22),room=B}");
    Assert.assertFalse(x.MetaDataMatch(test));

  }
}