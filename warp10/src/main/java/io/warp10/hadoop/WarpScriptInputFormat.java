package io.warp10.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.base.Charsets;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptExecutor.StackSemantics;

/**
 * This InputFormat wraps another InputFormat and creates a RecordReader
 * which returns K/V processed by some WarpScript™ code.
 */
public class WarpScriptInputFormat extends InputFormat<Writable, Writable> {
  
  /**
   * Suffix to use for the configuration
   */
  public static final String WARPSCRIPT_INPUTFORMAT_SUFFIX = "warpscript.inputformat.suffix";
  
  /**
   * Class of the wrapped InputFormat
   */
  public static final String WARPSCRIPT_INPUTFORMAT_CLASS = "warpscript.inputformat.class";
  
  /**
   * WarpScript™ code fragment to apply
   */
  public static final String WARPSCRIPT_INPUTFORMAT_SCRIPT = "warpscript.inputformat.script";

  private InputFormat wrappedInputFormat;
  private RecordReader wrappedRecordReader;
    
  private String suffix = "";
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,InterruptedException {
    String sfx = Warp10InputFormat.getProperty(context.getConfiguration(), this.suffix, WARPSCRIPT_INPUTFORMAT_SUFFIX, "");
    if (null != sfx) {
      if (!"".equals(sfx)) {
        this.suffix = "." + sfx;
      } else {
        this.suffix = "";
      }
    }

    ensureInnerFormat(context.getConfiguration());
    
    return this.wrappedInputFormat.getSplits(context); 
  }
  
  private void ensureInnerFormat(Configuration conf) throws IOException {
    if (null == this.wrappedInputFormat) {
      try {
        Class innerClass = Class.forName(conf.get(WARPSCRIPT_INPUTFORMAT_CLASS));
        this.wrappedInputFormat = (InputFormat) innerClass.newInstance();        
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }
  }
  
  @Override
  public RecordReader<Writable, Writable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    if (null == this.wrappedRecordReader) {
      ensureInnerFormat(context.getConfiguration());
      this.wrappedRecordReader = this.wrappedInputFormat.createRecordReader(split, context);
    }

    return new WarpScriptRecordReader(this.suffix, this.wrappedRecordReader);
  }
  
  /**
   * Return the actual WarpScript code executor given the script
   * which was passed as parameter.
   * 
   * This method can be overriden if custom loading is needed. In Spark for
   * example SparkFiles#get could be called.
   */
  public static WarpScriptExecutor getWarpScriptExecutor(String code) throws IOException,WarpScriptException {
    if (code.startsWith("@") || code.startsWith("%")) {

      //
      // delete the @/% character
      //

      String originalfilePath = code.substring(1);

      String filepath = Paths.get(originalfilePath).toString();

      String mc2 = parseWarpScript(filepath);
      
      WarpScriptExecutor executor = new WarpScriptExecutor(StackSemantics.PERTHREAD, mc2, null, null, code.startsWith("@"));
      return executor;
    } else {

      //
      // String with Warpscript commands
      //

      //
      // Compute the hash against String content to identify this run
      //

      WarpScriptExecutor executor = new WarpScriptExecutor(StackSemantics.PERTHREAD, code, null, null);
      return executor;
    }

  }
  
  public static String parseWarpScript(String filepath) throws IOException {
    //
    // Load the WarpsScript file
    // Warning: provide target directory when file has been copied on each node
    //
    StringBuffer scriptSB = new StringBuffer();
    InputStream fis = null;
    BufferedReader br = null;
    try {      
      fis = WarpScriptInputFormat.class.getClassLoader().getResourceAsStream(filepath);

      if (null == fis) {
        fis = new FileInputStream(filepath);
      }
      
      if (null == fis) {
        throw new IOException("WarpScript file '" + filepath + "' could not be found.");
      }
      
      br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        scriptSB.append(line).append("\n");
      }
    } catch (IOException ioe) {
      throw new IOException("WarpScript file could not be loaded", ioe);
    } finally {
      if (null == br) { try { br.close(); } catch (Exception e) {} }
      if (null == fis) { try { fis.close(); } catch (Exception e) {} }
    }

    return scriptSB.toString();
  }
}
