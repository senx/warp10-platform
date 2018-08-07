package io.warp10.hadoop;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptExecutor.StackSemantics;

public class WarpScriptRecordReader extends RecordReader<Writable, Writable> {

  private final RecordReader reader;
  
  private Writable key = null;
  private Writable value = null;
  
  /**
   * List of pending records not yet returned
   */
  private List<List<Writable>> records = new ArrayList<List<Writable>>();
  
  private int recordidx = 0;
  
  private final String suffix;
  
  private WarpScriptExecutor executor;
  
  private boolean done;
  
  private final WarpScriptInputFormat inputFormat;

  public WarpScriptRecordReader(WarpScriptInputFormat inputFormat) {
    this.inputFormat = inputFormat;
    this.suffix = inputFormat.getSuffix();
    this.reader = inputFormat.getWrappedRecordReader();
  }
  
  @Override
  public void close() throws IOException {
    this.reader.close();
  }
  
  @Override
  public Writable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }
  @Override
  public Writable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return reader.getProgress();
  }
  
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // Initialize wrapped reader
    reader.initialize(split, context);
    
    String code = Warp10InputFormat.getProperty(context.getConfiguration(), this.suffix, WarpScriptInputFormat.WARPSCRIPT_INPUTFORMAT_SCRIPT, null);
    // Initialize WarpScriptExecutor
    
    try {
      this.executor = inputFormat.getWarpScriptExecutor(context.getConfiguration(), code);
    } catch (WarpScriptException wse) {
      throw new IOException("Error while instatiating WarpScript executor", wse);
    }
    
    done = false;
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // If we have pending records, get the next one and return true
    if (!records.isEmpty()) {
      List<Writable> kv = records.get(recordidx++);
      if (records.size() == recordidx) {
        records.clear();
        recordidx = 0;
      }

      key = kv.get(0);
      value = kv.get(1);
            
      return true;
    }
    
    if (done) {
      return false;
    }
    
    //
    // Request the next K/V from the wrapped reader
    // and pass them to the WarpScript code until
    // the code actually returns records.
    //
    
    while(true) {
      boolean nkv = this.reader.nextKeyValue();
      
      if (nkv) {
        Writable k = (Writable) this.reader.getCurrentKey();
        Writable v = (Writable) this.reader.getCurrentValue();
        
        List<Object> input = new ArrayList<Object>();
        
        // This is not the last K/V we feed to the executor
        input.add(done);
        input.add(WritableUtils.fromWritable(v));
        input.add(WritableUtils.fromWritable(k));
        
        try {
          List<Object> results = this.executor.exec(input);
          
          // If there are no results on the stack, continue
          // calling the wrapped reader
          if (results.isEmpty()) {
            continue;
          }
          
          // push the records onto 'records', the deepest first,
          // ensuring each is a [ key value ] pair
          for (int i = results.size() - 1; i >= 0; i--) {
            Object result = results.get(i);
            if (!(result instanceof List) || 2 != ((List) result).size()) {
              throw new IOException("Invalid WarpScript™ output, expected a [ key value ] pair, got a " + result.getClass());
            }
            List<Writable> record = new ArrayList<Writable>();
            record.add(WritableUtils.toWritable(((List) result).get(0)));
            record.add(WritableUtils.toWritable(((List) result).get(1)));
            records.add(record);
          }
          
          return nextKeyValue();
        } catch (WarpScriptException wse) {
          throw new IOException(wse);
        }
      } else {
        done = true;
        // Call the WarpScript with true on top of the stack, meaning
        // we reached the end of the wrapped reader
        
        List<Object> input = new ArrayList<Object>();
        
        // This is the last K/V we feed to the executor
        input.add(done);
        
        try {
          List<Object> results = this.executor.exec(input);
         
          // If there are no results on the stack, return false,
          // because there are not pending records to consume
          // and we did not return anything
          if (results.isEmpty()) {
            return false;
          }
          
          // push the records onto 'records', the deepest first,
          // ensuring each is a [ key value ] pair
          for (int i = results.size() - 1; i >= 0; i--) {
            Object result = results.get(i);
            if (!(result instanceof List) && 2 != ((List) result).size()) {
              throw new IOException("Invalid WarpScript™ output, expected [ key value ] pairs, got a " + result.getClass());
            }
            List<Writable> record = new ArrayList<Writable>();
            record.add(WritableUtils.toWritable(((List) result).get(0)));
            record.add(WritableUtils.toWritable(((List) result).get(1)));
            records.add(record);
          }
          
          return nextKeyValue();
        } catch (WarpScriptException wse) {
          throw new IOException(wse);
        }      
      }
    }    
  }
}
