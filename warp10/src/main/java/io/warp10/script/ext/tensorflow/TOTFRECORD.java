package io.warp10.script.ext.tensorflow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.util.PureJavaCrc32C;
import org.tensorflow.example.Example;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOTFRECORD extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  static final int MASK_DELTA = 0xa282ead8;
  
  private final boolean docrc;
  
  public TOTFRECORD(String name) {
    super(name);
    docrc = false;
  }

  public TOTFRECORD(String name, boolean docrc) {
    super(name);
    this.docrc = docrc;
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof Example)) {
      throw new WarpScriptException(getName() + " expects a TensorFlow Example on top of the stack.");
    }
    
    Example example = (Example) top;

    if (docrc) {
      stack.push(toTFRecord(example.toByteArray()));
    } else {
      stack.push(example.toByteArray());
    }

    return stack;
  }
  
  public static final byte[] toTFRecord(byte[] bytes) {
    //
    // TFRecord format:
    // uint64 length
    // uint32 masked_crc32_of_length
    // byte   data[length]
    // uint32 masked_crc32_of_data
    //
    
    byte[] tfrecord = new byte[bytes.length + 8 + 8];    
    ByteBuffer tfrecordbb = ByteBuffer.wrap(tfrecord);
    tfrecordbb.order(ByteOrder.LITTLE_ENDIAN);
    
    byte[] len = new byte[8];
    ByteBuffer bb = ByteBuffer.wrap(len);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(bytes.length);
    
    PureJavaCrc32C crc32c = new PureJavaCrc32C();    
    crc32c.update(len, 0, 8);
    int crc = ((int) crc32c.getValue());
    //  Rotate right by 15 bits and add a constant.
    crc = ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    
    tfrecordbb.put(len);
    tfrecordbb.putInt(crc);

    crc32c = new PureJavaCrc32C();    
    crc32c.update(bytes, 0, bytes.length);
    crc = ((int) crc32c.getValue());
    //  Rotate right by 15 bits and add a constant.
    crc = ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    
    tfrecordbb.put(bytes);
    tfrecordbb.putInt(crc);

    return tfrecord;
  }
}
