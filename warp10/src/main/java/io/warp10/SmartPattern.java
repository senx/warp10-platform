package io.warp10;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implements a smart pattern matching engine
 * which will attempt to compare strings before
 * applying an actual regular expression.
 */
public class SmartPattern {
  
  private String string = null;
  private boolean matchall = false;
  private String fragment = null;
  private Matcher matcher = null;
  
  public SmartPattern(String string) {
    this.string = string;
  }
  
  public SmartPattern(Pattern p) {
    this.matcher = p.matcher("");
    
    //
    // Treat some patterns in a special way
    //
    
    String regexp = p.pattern();
    
    if (regexp.contains("|")) {
      return;
    }
    if (".*".equals(regexp) || "^.*".equals(regexp) || "^.*$".equals(regexp)) {
      this.matchall = true;
    }
    
    //
    // Extract the longest fragment from the regexp
    //
    
    //
    // Replace anything between parenthesis, brackets (curly ad square) by whitespace
    //
    
    regexp = regexp.replaceAll("\\(.*\\)", " ");
    regexp = regexp.replaceAll("\\[.*\\]", " ");
    regexp = regexp.replaceAll("\\{.*\\}", " ");
    
    // Replace anything after '\'
    
    regexp = regexp.replaceAll("\\\\.*", "");
    
    // Replace any character not a=z A=Z 0-9 by a space
    
    regexp = regexp.replaceAll("[^a-zA-Z0-9]", " ");
    
    String[] tokens = regexp.split(" ");
    
    for (String token: tokens) {
      if (0 == token.length()) {
        continue;
      }
      if (null == this.fragment) {
        this.fragment = token;
      } else {
        if (this.fragment.length() < token.length()) {
          this.fragment = token;
        }
      }
    }
  }
  
  public boolean matches(String str) {
    if (this.matchall) {
      return true;
    }
    
    if (null != string) {
      return string.equals(str);
    }
    
    //
    // Check if the identified fragment is present
    //
    
    if (null != fragment && str.indexOf(fragment) < 0) {
      return false;
    }
    
    this.matcher.reset(str);
    return this.matcher.matches();
  }
}
