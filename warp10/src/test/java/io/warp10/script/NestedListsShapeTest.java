package io.warp10.script;

import io.warp10.WarpConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class NestedListsShapeTest {

  private static final Path FILE_1 = Paths.get("src", "test", "java", "io", "warp10", "script", "nestedList_0.mc2");
  private static final Path FILE_2 = Paths.get("src", "test", "java", "io", "warp10", "script", "nestedList_1.mc2");
  private static final Path FILE_3 = Paths.get("src", "test", "java", "io", "warp10", "script", "nestedList_2.mc2");

  @BeforeClass
  public static void beforeClass() throws Exception {
    StringBuilder props = new StringBuilder();

    props.append("warp.timeunits=us");
    WarpConfig.safeSetProperties(new StringReader(props.toString()));
  }

  @Test
  public void testGet() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("'a' STORE");
    stack.execMulti("0 4 <% 'i' STORE");
    stack.execMulti("0 3 <% 'j' STORE");
    stack.execMulti("0 3 <% 'k' STORE");
    stack.execMulti("0 2 <% 'l' STORE");
    stack.execMulti("$a [ $i $j $k $l ] GET");
    stack.execMulti("$l $k 3 * + $j 3 * 4 * + $i 3 * 4 * 4 * +");
    stack.execMulti("== ASSERT");
    stack.execMulti("%> FOR %> FOR %> FOR %> FOR");
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testPut() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("4.3 [ 4 3 2 1 ] PUT DUP [ 4 3 2 1 ] GET 4.3 == ASSERT ");
    stack.execMulti("7.5 [ 1 0 2 2 ] PUT [ 1 0 2 2 ] GET 7.5 == ASSERT ");
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testShape() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null,null);
    stack.maxLimits();

    stack.execMulti("[ 1 2 2 3 4 5 * * * * <% %> FOR ] SHAPE");

    Object o = stack.pop();
    Assert.assertTrue(o instanceof List);
    Assert.assertEquals(((List) o).get(0), 2L*2*3*4*5);

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("SHAPE LIST-> " +
      "4 == ASSERT " +
      "3 == ASSERT " +
      "4 == ASSERT " +
      "4 == ASSERT " +
      "5 == ASSERT ");
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testHullShape() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null,null);
    stack.maxLimits();

    stack.execMulti("[ 1 2 2 3 4 5 * * * * <% %> FOR ] SHAPE");

    Object o = stack.pop();
    Assert.assertTrue(o instanceof List);
    Assert.assertEquals(((List) o).get(0), 2L*2*3*4*5);

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("HULLSHAPE LIST-> " +
      "4 == ASSERT " +
      "3 == ASSERT " +
      "4 == ASSERT " +
      "4 == ASSERT " +
      "5 == ASSERT ");
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testReshape() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti("[ 1 2 2 3 4 5 * * * * <% %> FOR ]");
    stack.execMulti("[ 5 4 4 3 ] RESHAPE ->JSON");

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("->JSON");

    Assert.assertArrayEquals(((String) stack.pop()).toCharArray(), ((String) stack.pop()).toCharArray());
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testPermute() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("[ 1 0 3 2 ] PERMUTE ->JSON");

    stack.execMulti(new String(Files.readAllBytes(FILE_2), StandardCharsets.UTF_8));
    stack.execMulti("->JSON");

    Assert.assertArrayEquals(((String) stack.pop()).toCharArray(), ((String) stack.pop()).toCharArray());
    stack.execMulti("DEPTH 0 == ASSERT");
  }

  @Test
  public void testPermuteCycle() throws Exception {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.maxLimits();

    stack.execMulti(new String(Files.readAllBytes(FILE_1), StandardCharsets.UTF_8));
    stack.execMulti("[ 3 0 1 2 ] PERMUTE ->JSON");

    stack.execMulti(new String(Files.readAllBytes(FILE_3), StandardCharsets.UTF_8));
    stack.execMulti("->JSON");

    Assert.assertArrayEquals(((String) stack.pop()).toCharArray(), ((String) stack.pop()).toCharArray());
    stack.execMulti("DEPTH 0 == ASSERT");
  }
}
