package io.warp10.script;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WarpScriptStackRegistry {
  
  private static final Map<String,WeakReference<WarpScriptStack>> stacks = new HashMap<String,WeakReference<WarpScriptStack>>();
  
  private static boolean enabled = false;
  
  public static void register(WarpScriptStack stack) {
    if (!enabled || null == stack) {
      return;
    }
    stacks.put(stack.getUUID(), new WeakReference<WarpScriptStack>(stack));
  }
  
  public static boolean unregister(WarpScriptStack stack) {
    if (!enabled || null == stack) {
      return false;
    }
    return null != stacks.remove(stack.getUUID());
  }
  
  public static boolean unregister(String uuid) {
    if (!enabled) {
      return false;
    }
    return null != stacks.remove(uuid);
  }
  
  public static boolean abort(String uuid) {
    if (!enabled) {
      return false;
    }
    
    WeakReference<WarpScriptStack> stackref = stacks.get(uuid);
    
    if (null == stackref) {
      return false;
    }
    
    stackref.get().abort();
    return true;
  }
  
  public static void enable() {
    enabled = true;
  }
  
  public static void disable() {
    // Clear the registered stacks
    stacks.clear();
    enabled = false;
  }
  
  public static List<WarpScriptStack> stacks() {
    List<WarpScriptStack> stacks = new ArrayList<WarpScriptStack>(WarpScriptStackRegistry.stacks.size());
    
    for (WeakReference<WarpScriptStack> ref: WarpScriptStackRegistry.stacks.values()) {
      stacks.add(ref.get());
    }
    
    return stacks;
  }
}
