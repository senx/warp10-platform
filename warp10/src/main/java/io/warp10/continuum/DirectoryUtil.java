//
//   Copyright 2018-2023  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

import io.warp10.SmartPattern;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.Directory;
import io.warp10.continuum.store.thrift.data.DirectoryStatsRequest;
import io.warp10.continuum.store.thrift.data.DirectoryStatsResponse;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.SipHashInline;
import io.warp10.script.HyperLogLogPlus;
import io.warp10.sensision.Sensision;
import io.warp10.standalone.StandaloneDirectoryClient;

public class DirectoryUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryUtil.class);

  public static long computeHash(long k0, long k1, DirectoryStatsRequest request) {
    return computeHash(k0, k1, request.getTimestamp(), request.getClassSelector(), request.getLabelsSelectors());
  }

  private static long computeHash(long k0, long k1, long timestamp, List<String> classSelectors, List<Map<String,String>> labelsSelectors) {
    //
    // Create a ByteArrayOutputStream into which the content will be dumped
    //

    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Add timestamp

    try {
      baos.write(Longs.toByteArray(timestamp));

      if (null != classSelectors) {
        for (String classSelector: classSelectors) {
          baos.write(classSelector.getBytes(StandardCharsets.UTF_8));
        }
      }

      if (null != labelsSelectors) {
        for (Map<String, String> map: labelsSelectors) {
          TreeMap<String,String> tm = new TreeMap<String, String>();
          tm.putAll(map);
          for (Entry<String,String> entry: tm.entrySet()) {
            baos.write(entry.getKey().getBytes(StandardCharsets.UTF_8));
            baos.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    } catch (IOException ioe) {
      return 0L;
    }

    // Compute hash

    byte[] data = baos.toByteArray();
    try { baos.close(); } catch (IOException ioe) {}

    long hash = SipHashInline.hash24(k0, k1, data, 0, data.length);

    return hash;
  }

  /**
   * Process a DirectoryStatsRequest and produce a DirectoryStatsResponse given information from either a distributed
   * or standalone Directory.
   * @param request The request to process
   * @param filter A shard filter. null is there is no shard.
   * @param metadatas The metadata map of the Directory.
   * @param classesPerOwner If available, the Map associating owner to their classes, null otherwise.
   * @param classCardinalityLimit Threshold above which to use a simplified estimator, for classes.
   * @param labelsCardinalityLimit Threshold above which to use a simplified estimator, for labels.
   * @param maxage The maximum age of the request, negative to bypass the test.
   * @param classLongs SipHash key for classes.
   * @param labelsLongs SipHash key for labels.
   * @param pskLongs SipHash key for computing MAC for DirectoryFindRequest instances, null to bypass MAC check.
   * @return The result of the request.
   * @throws TException if any Exception occurs in this method.
   */
  public static DirectoryStatsResponse stats(DirectoryStatsRequest request,
                                             StandaloneDirectoryClient.ShardFilter filter,
                                             Map<String, Map<Long, Metadata>> metadatas,
                                             Map<String, Map<Long, String>> classesPerOwner,
                                             long classCardinalityLimit,
                                             long labelsCardinalityLimit,
                                             long maxage,
                                             long[] classLongs,
                                             long[] labelsLongs,
                                             long[] pskLongs) throws TException {

    try {
      DirectoryStatsResponse response = new DirectoryStatsResponse();

      if (0 <= maxage) {
        //
        // Check request age
        //

        long now = System.currentTimeMillis();

        if (now - request.getTimestamp() > maxage) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_EXPIRED, Sensision.EMPTY_LABELS, 1);
          response.setError("Request has expired.");
          return response;
        }
      }

      if (null != pskLongs) {
        //
        // Check request hash
        //

        long hash = DirectoryUtil.computeHash(pskLongs[0], pskLongs[1], request);

        // Check hash against value in the request

        if (hash != request.getHash()) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_INVALID, Sensision.EMPTY_LABELS, 1);
          response.setError("Invalid request.");
          return response;
        }
      }


      //
      // Build patterns from expressions
      //

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_DIRECTORY_STATS_REQUESTS, Sensision.EMPTY_LABELS, 1);

      SmartPattern classSmartPattern;

      HyperLogLogPlus gtsCount = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
      Map<String, HyperLogLogPlus> perClassCardinality = new HashMap<String, HyperLogLogPlus>();
      Map<String, HyperLogLogPlus> perLabelValueCardinality = new HashMap<String, HyperLogLogPlus>();
      HyperLogLogPlus labelNamesCardinality = null;
      HyperLogLogPlus labelValuesCardinality = null;
      HyperLogLogPlus classCardinality = null;

      List<String> missingLabels = Constants.ABSENT_LABEL_SUPPORT ? new ArrayList<String>() : null;

      for (int i = 0; i < request.getClassSelectorSize(); i++) {
        String exactClassName = null;

        if (request.getClassSelector().get(i).startsWith("=") || !request.getClassSelector().get(i).startsWith("~")) {
          exactClassName = request.getClassSelector().get(i).startsWith("=") ? request.getClassSelector().get(i).substring(1) : request.getClassSelector().get(i);
          classSmartPattern = new SmartPattern(exactClassName);
        } else {
          classSmartPattern = new SmartPattern(Pattern.compile(request.getClassSelector().get(i).substring(1)));
        }

        Map<String, SmartPattern> labelPatterns = new LinkedHashMap<String, SmartPattern>();

        if (null != missingLabels) {
          missingLabels.clear();
        }

        if (null != request.getLabelsSelectors()) {
          for (Entry<String, String> entry: request.getLabelsSelectors().get(i).entrySet()) {
            String label = entry.getKey();
            String expr = entry.getValue();
            SmartPattern pattern;

            if (null != missingLabels && ("=".equals(expr) || "".equals(expr))) {
              missingLabels.add(label);
              continue;
            }

            if (expr.startsWith("=") || !expr.startsWith("~")) {
              //pattern = Pattern.compile(Pattern.quote(expr.startsWith("=") ? expr.substring(1) : expr));
              pattern = new SmartPattern(expr.startsWith("=") ? expr.substring(1) : expr);
            } else {
              pattern = new SmartPattern(Pattern.compile(expr.substring(1)));
            }

            //labelPatterns.put(label,  pattern.matcher(""));
            labelPatterns.put(label, pattern);
          }
        }

        //
        // Loop over the class names to find matches
        //

        Collection<String> classNames = new ArrayList<String>();

        if (null != exactClassName) {
          // If the class name is an exact match, check if it is known, if not, skip to the next selector
          if (!metadatas.containsKey(exactClassName)) {
            continue;
          }
          classNames.add(exactClassName);
        } else {
          //
          // Extract per owner classes if owner selector exists and classesPerOwner is defined
          //
          if (null != classesPerOwner) {
            if (request.getLabelsSelectors().get(i).size() > 0) {
              String ownersel = request.getLabelsSelectors().get(i).get(Constants.OWNER_LABEL);

              if (null != ownersel && ownersel.startsWith("=")) {
                classNames = classesPerOwner.get(ownersel.substring(1)).values();
              } else {
                classNames = metadatas.keySet();
              }
            } else {
              classNames = metadatas.keySet();
            }
          } else {
            classNames = metadatas.keySet();
          }
        }

        List<String> labelNames = new ArrayList<String>(labelPatterns.size());
        List<SmartPattern> labelSmartPatterns = new ArrayList<SmartPattern>(labelPatterns.size());
        List<String> labelValues = new ArrayList<String>(labelPatterns.size());

        for (Entry<String, SmartPattern> entry: labelPatterns.entrySet()) {
          labelNames.add(entry.getKey());
          labelSmartPatterns.add(entry.getValue());
          labelValues.add(null);
        }

        for (String className: classNames) {

          //
          // If class matches, check all labels for matches
          //

          if (classSmartPattern.matches(className)) {
            Map<Long, Metadata> classMetadatas = metadatas.get(className);
            if (null == classMetadatas) {
              continue;
            }
            for (Metadata metadata: classMetadatas.values()) {

              boolean exclude = false;

              if (null != missingLabels) {
                for (String missing: missingLabels) {
                  // If the Metadata contain one of the missing labels, exclude the entry
                  if (null != metadata.getLabels().get(missing)) {
                    exclude = true;
                    break;
                  }
                }
                // Check attributes
                if (!exclude && metadata.getAttributesSize() > 0) {
                  for (String missing: missingLabels) {
                    // If the Metadata contain one of the missing labels, exclude the entry
                    if (null != metadata.getAttributes().get(missing)) {
                      exclude = true;
                      break;
                    }
                  }
                }
                if (exclude) {
                  continue;
                }
              }

              int idx = 0;

              for (String labelName: labelNames) {
                //
                // Immediately exclude metadata which do not contain one of the
                // labels for which we have patterns either in labels or in attributes
                //

                String labelValue = metadata.getLabels().get(labelName);

                if (null == labelValue) {
                  labelValue = metadata.getAttributes().get(labelName);
                  if (null == labelValue) {
                    exclude = true;
                    break;
                  }
                }

                labelValues.set(idx++, labelValue);
              }

              // If we did not collect enough label/attribute values, exclude the GTS
              if (idx < labelNames.size()) {
                exclude = true;
              }

              if (exclude) {
                continue;
              }

              //
              // Check if the label value matches, if not, exclude the GTS
              //

              for (int j = 0; j < labelNames.size(); j++) {
                if (!labelSmartPatterns.get(j).matches(labelValues.get(j))) {
                  exclude = true;
                  break;
                }
              }

              if (exclude) {
                continue;
              }

              //
              // We have a match, update estimators
              //

              // Compute classId/labelsId
              long classId = GTSHelper.classId(classLongs, metadata.getName());
              long labelsId = GTSHelper.labelsId(labelsLongs, metadata.getLabels());

              //
              // Apply the shard filter to exclude Metadata which do not belong to the
              // shards we handle
              //

              if (null != filter && filter.exclude(classId, labelsId)) {
                continue;
              }

              // Compute gtsId, we use the GTS Id String from which we extract the 16 bytes
              byte[] data = GTSHelper.gtsIdToString(classId, labelsId).getBytes(StandardCharsets.UTF_16BE);
              long gtsId = SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length);

              gtsCount.aggregate(gtsId);

              if (null != perClassCardinality) {
                HyperLogLogPlus count = perClassCardinality.get(metadata.getName());
                if (null == count) {
                  count = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  perClassCardinality.put(metadata.getName(), count);
                }

                count.aggregate(gtsId);

                // If we reached the limit in detailed number of classes, we fallback to a simple estimator
                if (perClassCardinality.size() >= classCardinalityLimit) {
                  classCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  for (String cls: perClassCardinality.keySet()) {
                    data = cls.getBytes(StandardCharsets.UTF_8);
                    classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));
                    perClassCardinality = null;
                  }
                }
              } else {
                data = metadata.getName().getBytes(StandardCharsets.UTF_8);
                classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));
              }

              if (null != perLabelValueCardinality) {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String, String> entry: metadata.getLabels().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    long siphash = SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false);
                    estimator.aggregate(siphash);
                  }
                }

                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String, String> entry: metadata.getAttributes().entrySet()) {
                    HyperLogLogPlus estimator = perLabelValueCardinality.get(entry.getKey());
                    if (null == estimator) {
                      estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                      perLabelValueCardinality.put(entry.getKey(), estimator);
                    }
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    estimator.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }

                if (perLabelValueCardinality.size() >= labelsCardinalityLimit) {
                  labelNamesCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  labelValuesCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
                  for (Entry<String, HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelNamesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    labelValuesCardinality.fuse(entry.getValue());
                  }
                  perLabelValueCardinality = null;
                }
              } else {
                if (metadata.getLabelsSize() > 0) {
                  for (Entry<String, String> entry: metadata.getLabels().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }
                if (metadata.getAttributesSize() > 0) {
                  for (Entry<String, String> entry: metadata.getAttributes().entrySet()) {
                    data = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                    data = entry.getValue().getBytes(StandardCharsets.UTF_8);
                    labelValuesCardinality.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
                  }
                }
              }
            }
          }
        }
      }

      response.setGtsCount(gtsCount.toBytes());

      if (null != perClassCardinality) {
        classCardinality = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        for (Entry<String, HyperLogLogPlus> entry: perClassCardinality.entrySet()) {
          response.putToPerClassCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          classCardinality.aggregate(SipHashInline.hash24(classLongs[0], classLongs[1], data, 0, data.length, false));
        }
      }

      response.setClassCardinality(classCardinality.toBytes());

      if (null != perLabelValueCardinality) {
        HyperLogLogPlus estimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        HyperLogLogPlus nameEstimator = new HyperLogLogPlus(Directory.ESTIMATOR_P, Directory.ESTIMATOR_PPRIME);
        for (Entry<String, HyperLogLogPlus> entry: perLabelValueCardinality.entrySet()) {
          byte[] data = entry.getKey().getBytes(StandardCharsets.UTF_8);
          nameEstimator.aggregate(SipHashInline.hash24(labelsLongs[0], labelsLongs[1], data, 0, data.length, false));
          estimator.fuse(entry.getValue());
          response.putToPerLabelValueCardinality(entry.getKey(), ByteBuffer.wrap(entry.getValue().toBytes()));
        }
        response.setLabelNamesCardinality(nameEstimator.toBytes());
        response.setLabelValuesCardinality(estimator.toBytes());
      } else {
        response.setLabelNamesCardinality(labelNamesCardinality.toBytes());
        response.setLabelValuesCardinality(labelValuesCardinality.toBytes());
      }

      return response;
    } catch (Exception e) {
      LOG.error("Error getting the stats.", e);
      throw new TException(e);
    }
  }
}
