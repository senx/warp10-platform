package io.warp10.script.ext.tensorflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.util.PureJavaCrc32C;
import org.tensorflow.example.Example;

import com.google.protobuf.InvalidProtocolBufferException;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TFRECORDTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private final boolean docrc;
  
  public TFRECORDTO(String name) {
    super(name);
    this.docrc = false;
  }

  public TFRECORDTO(String name, boolean docrc) {
    super(name);
    this.docrc = docrc;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof byte[])) {
      throw new WarpScriptException(getName() + " expects a TFRecord on top of the stack.");
    }

    byte[] bytes = null;
    
    if (docrc) {
      ByteBuffer tfrecordbb = ByteBuffer.wrap((byte[]) top);
      tfrecordbb.order(ByteOrder.LITTLE_ENDIAN);
      
      long len = tfrecordbb.getLong();
      
      int goldcrc = tfrecordbb.getInt();
      
      PureJavaCrc32C crc32c = new PureJavaCrc32C();
      crc32c.update((byte[]) top, 0, 8);
      int crc = ((int) crc32c.getValue());
      //  Rotate right by 15 bits and add a constant.
      crc = ((crc >>> 15) | (crc << 17)) + TOTFRECORD.MASK_DELTA;
      
      if (crc != goldcrc) {
        throw new WarpScriptException(getName() + " encountered a corrupted TFRecord length.");
      }
      
      bytes = new byte[(int) len];
            
      tfrecordbb.get(bytes);
      
      goldcrc = tfrecordbb.getInt();
      
      crc32c.reset();
      crc32c.update(bytes, 0, bytes.length);
      crc = ((int) crc32c.getValue());
      //  Rotate right by 15 bits and add a constant.
      crc = ((crc >>> 15) | (crc << 17)) + TOTFRECORD.MASK_DELTA;

      if (crc != goldcrc) {
        throw new WarpScriptException(getName() + " encountered corrupted TFRecord data.");
      }
    } else {
      bytes = (byte[]) top;
    }

    try {
      stack.push(Example.parseFrom(bytes));
    } catch (InvalidProtocolBufferException ipbe) {
      throw new WarpScriptException(getName() + " encountered and error when parsing TFRecord.");
    }      

    return stack;
  }  
}
