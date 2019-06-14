package io.warp10.script.functions;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.formatted.FormattedWarpScriptFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PERMUTE extends FormattedWarpScriptFunction {

  public static final String TENSOR = "tensor";
  public static final String PATTERN = "pattern";
  public static final String FAST = "fast";

  private final Arguments args;
  private final Arguments output;

  protected Arguments getArguments() {
    return args;
  }

  protected Arguments getOutput() {
    return output;
  }

  public PERMUTE(String name) {
    super(name);

    getDocstring().append("Permute the dimensions of a nested LIST as if it were a tensor or a multidimensional array.");

    args = new ArgumentsBuilder()
      .addArgument(List.class, TENSOR, "The nested LIST for which dimensions will be permuted as if it were a tensor.")
      .addListArgument(Long.class, PATTERN, "The permutation pattern (a LIST of LONG).")
      .addOptionalArgument(Boolean.class, FAST, "If True, it does not check if the sizes of the nested lists are coherent before operating. Default to False.", false)
      .build();

    output = new ArgumentsBuilder()
      .addArgument(List.class, TENSOR, "The resulting nested LIST.")
      .build();
  }

  private List<Long> pattern;
  private List<Long> newShape;

  protected WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException {
    List<Object> tensor = (List) formattedArgs.get(TENSOR);
    pattern = (List) formattedArgs.get(PATTERN);
    boolean fast = ((Boolean) formattedArgs.get(FAST)).booleanValue();

    List<Long> shape = SHAPE.candidate_shape(tensor);

    if (!(fast || CHECKSHAPE.recValidateShape(tensor, shape))) {
      throw new WarpScriptException(getName() + " expects that the sizes of the nested lists are coherent together to form a tensor (or multidimensional array).");
    }

    newShape = new ArrayList<>();
    for (int r = 0; r < pattern.size(); r++) {
      newShape.add(shape.get(pattern.get(r).intValue()));
    }

    List<Object> result = new ArrayList<>();
    recPermute(tensor, result, new ArrayList<Long>(), 0);
    stack.push(result);
    return stack;
  }

  private void recPermute(List<Object> tensor, List<Object> result, List<Long> indices, int dimension) throws WarpScriptException {

    for (int i = 0; i < newShape.get(dimension); i++) {
      indices = new ArrayList(indices);
      indices.add(new Long(i));

      if (newShape.size() - 1 == dimension) {
        List<Number> permutedIndices = new ArrayList<>();

        for (int r = 0; r < pattern.size(); r++) {
          permutedIndices.add(indices.get(pattern.get(r).intValue()));
        }

        result.add(GET.recNestedGet(this, tensor, permutedIndices));

      } else {

        List<Object> nested = new ArrayList<>();
        result.add(nested);
        recPermute(tensor, nested, indices, dimension++);
      }
    }
  }
}
