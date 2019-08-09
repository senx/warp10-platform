package io.warp10.script.binary;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSOpsHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.util.Map;

public class MACROBINARYOP extends FormattedWarpScriptFunction {

  private final Arguments args;
  private final Arguments output;

  private static final String GTS_1 = "gtsa";
  private static final String GTS_2 = "gtsb";
  private static final String MACRO = "macro";
  private static final String OUTPUT = "output";

  @Override
  public Arguments getArguments() {
    return args;
  }

  public Arguments getOutput() {
    return output;
  }

  public MACROBINARYOP(String name) {
    super(name);

    getDocstring().append("Apply a macro as a binary operator on two input GTS.");

    args = new ArgumentsBuilder()
      .addArgument(GeoTimeSerie.class, GTS_1, "First operand.")
      .addArgument(GeoTimeSerie.class, GTS_2, "Second operand.")
      .addArgument(WarpScriptStack.Macro.class, MACRO, "The binary operator macro.")
      .build();

    output = new ArgumentsBuilder()
      .addArgument(GeoTimeSerie.class, OUTPUT, "The resulting GTS.")
      .build();

    //
    // Equality assert unit test
    //

    StringBuilder equalTest = new StringBuilder();
    equalTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'a' STORE" + System.lineSeparator());
    equalTest.append("$a $a <% == ASSERT 1.0 %> " + getName() + " DROP");
    getUnitTests().add(equalTest.toString());

    //
    // Simple unit test (check that results is the same than already defined binary ops)
    //

    StringBuilder addTest = new StringBuilder();
    addTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'a' STORE" + System.lineSeparator());
    addTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'b' STORE" + System.lineSeparator());
    addTest.append("$a $b <% + %> " + getName() + System.lineSeparator());
    addTest.append("$a $b + " + System.lineSeparator());
    addTest.append("<% == ASSERT NULL %> " + getName() + " DROP");
    getUnitTests().add(addTest.toString());

    StringBuilder minusTest = new StringBuilder();
    minusTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'a' STORE" + System.lineSeparator());
    minusTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'b' STORE" + System.lineSeparator());
    minusTest.append("$a $b <% - %> " + getName() + System.lineSeparator());
    minusTest.append("$a $b - " + System.lineSeparator());
    minusTest.append("<% == ASSERT NULL %> " + getName() + " DROP");
    getUnitTests().add(minusTest.toString());

    StringBuilder mulTest = new StringBuilder();
    mulTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'a' STORE" + System.lineSeparator());
    mulTest.append("[ 1 100 <% %> FOR ] [] [] [] [ 1 100 <% RAND %> FOR ] MAKEGTS 'b' STORE" + System.lineSeparator());
    mulTest.append("$a $b <% + %> " + getName() + System.lineSeparator());
    mulTest.append("$a $b + " + System.lineSeparator());
    mulTest.append("<% == ASSERT NULL %> " + getName() + " DROP");
    getUnitTests().add(mulTest.toString());
  }

  @Override
  protected WarpScriptStack apply(Map<String, Object> formattedArgs, final WarpScriptStack stack) throws WarpScriptException {

    //
    // Retrieve arguments
    //

    GeoTimeSerie gts1 = (GeoTimeSerie) formattedArgs.get(GTS_1);
    GeoTimeSerie gts2 = (GeoTimeSerie) formattedArgs.get(GTS_2);
    final WarpScriptStack.Macro macro = (WarpScriptStack.Macro) formattedArgs.get(MACRO);

    //
    // Build BinaryOp
    //

    final WarpScriptStack stack_ = stack;

    GTSOpsHelper.GTSBinaryOp op = new GTSOpsHelper.GTSBinaryOp() {

      @Override
      public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb) throws WarpScriptException {

        stack_.push(GTSHelper.valueAtIndex(gtsa, idxa));
        stack_.push(GTSHelper.valueAtIndex(gtsb, idxb));
        stack_.exec(macro);
        return stack_.pop();
      }
    };

    //
    // Apply binaryOp
    //

    GeoTimeSerie result = new GeoTimeSerie(Math.max(GTSHelper.nvalues(gts1), GTSHelper.nvalues(gts2)));
    //result.setType(GeoTimeSerie.TYPE.UNDEFINED); // let the first value sets the type

    GTSOpsHelper.applyBinaryOp(result, gts1, gts2, op);
    stack.push(result);

    return stack;
  }
}
