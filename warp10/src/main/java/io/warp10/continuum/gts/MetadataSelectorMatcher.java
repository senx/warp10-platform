package io.warp10.continuum.gts;


import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.WarpScriptException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataSelectorMatcher {

  private final String selector;

  private final Matcher classnamePattern;
  private final boolean withClassnameSelector;
  private final Map<String, Matcher> labelsPatterns;
  private final boolean withLabelSelector;
  private final Map<String, Matcher> attributesPatterns;
  private final boolean withAttributeSelector;

  private static final Pattern EXPR_RE = Pattern.compile("^(?<classname>[^{]+)\\{(?<labels>[^}]*)\\}(\\{(?<attributes>[^}]*)\\})?$");

  public MetadataSelectorMatcher(String selector) throws WarpScriptException {
    this.selector = selector;

    Matcher m = EXPR_RE.matcher(selector);
    if (!m.matches()) {
      throw new WarpScriptException(" invalid syntax for selector.");
    }

    // read class selector
    String classSelector = null;
    try {
      classSelector = URLDecoder.decode(m.group("classname"), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException uee) {
      // Can't happen, we're using UTF-8
    }

    // build class selector pattern
    if (classSelector.equals("~.*") || classSelector.equals("=") || classSelector.equals("~")) {
      this.withClassnameSelector = false;
      this.classnamePattern = null;
    } else if (classSelector.startsWith("~")) {
      this.withClassnameSelector = true;
      this.classnamePattern = Pattern.compile(classSelector.substring(1)).matcher("");
    } else if (classSelector.startsWith("=")) {
      this.withClassnameSelector = true;
      this.classnamePattern = Pattern.compile(Pattern.quote(classSelector.substring(1))).matcher("");
    } else {
      this.withClassnameSelector = true;
      this.classnamePattern = Pattern.compile(Pattern.quote(classSelector)).matcher("");
    }

    System.out.println(this.withClassnameSelector);
    System.out.println(this.classnamePattern);

    // read label selector
    String labelsSelection = m.group("labels");
    Map<String, String> labelsSelectors = null;
    try {
      labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
    } catch (ParseException pe) {
      throw new WarpScriptException(pe);
    }

    if (0 == labelsSelectors.size()) {
      this.withLabelSelector = false;
      this.labelsPatterns = null;
    } else {
      this.withLabelSelector = true;
      this.labelsPatterns = new HashMap<>(labelsSelectors.size());
      // build label patterns map
      for (Entry<String, String> l: labelsSelectors.entrySet()) {
        if (l.getValue().startsWith("=")) {
          this.labelsPatterns.put(l.getKey(), Pattern.compile(Pattern.quote(l.getValue().substring(1))).matcher(""));
        } else { //starts with ~ , otherwise Parse Exception in GTSHelper.parseLabelsSelectors
          this.labelsPatterns.put(l.getKey(), Pattern.compile(l.getValue().substring(1)).matcher(""));
        }
      }
    }

    System.out.println(this.withLabelSelector);
    System.out.println(this.labelsPatterns);


    // read attribute selector, if any
    Map<String, String> attributesSelectors = null;
    String attributesSelection = m.group("attributes");
    if (attributesSelection != null) {
      try {
        attributesSelectors = GTSHelper.parseLabelsSelectors(attributesSelection);
      } catch (ParseException pe) {
        throw new WarpScriptException(pe);
      }
      this.withAttributeSelector = true;
      this.attributesPatterns = new HashMap<>(attributesSelectors.size());
      // build label patterns map
      for (Entry<String, String> l: attributesSelectors.entrySet()) {
        if (l.getValue().startsWith("=")) {
          this.attributesPatterns.put(l.getKey(), Pattern.compile(Pattern.quote(l.getValue().substring(1))).matcher(""));
        } else { //starts with ~ , otherwise Parse Exception in GTSHelper.parseLabelsSelectors
          this.attributesPatterns.put(l.getKey(), Pattern.compile(l.getValue().substring(1)).matcher(""));
        }
      }
    } else {
      this.withAttributeSelector = false;
      this.attributesPatterns = null;
    }

    System.out.println(this.withAttributeSelector);
    System.out.println(this.attributesPatterns);

    System.out.println("---------------");
  }

  public boolean MetaDataMatch(Metadata metadata) {

    // first, check classname
    boolean classnameMatch = !this.withClassnameSelector
        || (this.withClassnameSelector && this.classnamePattern.reset(metadata.getName()).matches());

    // then, check labels.
    // standard selector : class{labelOrAttribute=xxx}
    // if there is no label with this name, look at the attributes.
    // extended selector : class{label=xxx}{attribute=yyy}
    // check separately labels and attributes, both must match.

    Map inputLabels = metadata.getLabels();
    Map inputAttributes = metadata.getAttributes();
    boolean labelAndAttributeMatch = true;

    if (this.withAttributeSelector) {
      // extended selector
      if (this.withLabelSelector) {
        for (Entry<String, Matcher> ls: this.labelsPatterns.entrySet()) {
          if (inputLabels.containsKey(ls.getKey())) {
            labelAndAttributeMatch &= ls.getValue().reset((String) inputLabels.get(ls.getKey())).matches();
          }
          if (!labelAndAttributeMatch) {
            break;
          }
        }
      }
      for (Entry<String, Matcher> ls: this.attributesPatterns.entrySet()) {
        if (!labelAndAttributeMatch) {
          break;
        }
        if (inputAttributes.containsKey(ls.getKey())) {
          labelAndAttributeMatch &= ls.getValue().reset((String) inputAttributes.get(ls.getKey())).matches();
        }
      }
    } else {
      // standard selector
      if (this.withLabelSelector) {
        for (Entry<String, Matcher> ls: this.labelsPatterns.entrySet()) {
          if (inputLabels.containsKey(ls.getKey())) {
            labelAndAttributeMatch &= ls.getValue().reset((String) inputLabels.get(ls.getKey())).matches();
          } else if (inputAttributes.containsKey(ls.getKey())) {
            labelAndAttributeMatch &= ls.getValue().reset((String) inputAttributes.get(ls.getKey())).matches();
          }
          if (!labelAndAttributeMatch) {
            break;
          }
        }
      }
    }

    return classnameMatch && labelAndAttributeMatch;
  }

}
