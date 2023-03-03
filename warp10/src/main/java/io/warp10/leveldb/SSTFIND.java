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

package io.warp10.leveldb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bouncycastle.util.encoders.Hex;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;

import io.warp10.BytesUtils;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.gts.MetadataIdComparator;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.standalone.Warp;
import io.warp10.warp.sdk.Capabilities;

/**
 * Identify Geo Time Series which have data in a given key range, supposedly from an SST file
 */
public class SSTFIND extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private String attrkey = "sstfind.cachekey1." + UUID.randomUUID().toString();
  private String attrkey2 = "sstfind.cachekey2." + UUID.randomUUID().toString();

  public SSTFIND(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {

    if (null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB) && null == Capabilities.get(stack, WarpScriptStack.CAPABILITY_LEVELDB_FIND)) {
      throw new WarpScriptException(getName() + " missing capability '" + WarpScriptStack.CAPABILITY_LEVELDB + "' or '" + WarpScriptStack.CAPABILITY_LEVELDB_FIND + "'.");
    }

    WarpDB db = Warp.getDB();

    if (null == db) {
      throw new WarpScriptException(getName() + " can only be called when using LevelDB.");
    }

    Object top = stack.pop();

    if (!(top instanceof List)) {
      throw new WarpScriptException(getName() + " expects a parameter list.");
    }

    // List is either
    // [ smallestkey largestkey ]
    // or [ token classselector labelselector smallestkey largestkey ]

    List<Object> params = (List<Object>) top;

    if (5 != params.size() && 2 != params.size()) {
      throw new WarpScriptException(getName() + " expects a list of 2 (smallestkey,largestkey) or 5 (token,classselector,labelsseclector,smallestkey,largestkey) parameters.");
    }

    byte[] largest = null == params.get(params.size() - 1) ? null : Hex.decode(params.get(params.size() - 1).toString());
    byte[] smallest = null == params.get(params.size() - 2) ? null : Hex.decode(params.get(params.size() - 2).toString());

    if (null == smallest || null == largest) {
      stack.setAttribute(this.attrkey, null);
      stack.setAttribute(this.attrkey2, null);
      return stack;
    }

    Set<Metadata> selectedmetas = null;

    if (5 == params.size()) {
      String token = params.get(0).toString();

      ReadToken rtoken = Tokens.extractReadToken(token);

      Object oLabelsSelector = params.get(2);

      if (!(oLabelsSelector instanceof Map)) {
        throw new WarpScriptException("Label selectors must be a map.");
      }

      Map<String,String> labelSelectors = new HashMap<String,String>((Map<String,String>) oLabelsSelector);

      //
      // Extract class selector
      //

      Object oClassSelector = params.get(1);

      if (!(oClassSelector instanceof String)) {
        throw new WarpScriptException("Class selector must be a string.");
      }

      String classSelector = (String) oClassSelector;

      labelSelectors.remove(Constants.PRODUCER_LABEL);
      labelSelectors.remove(Constants.OWNER_LABEL);
      labelSelectors.remove(Constants.APPLICATION_LABEL);
      labelSelectors.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

      Metadata metadata = new Metadata();
      metadata.setName(classSelector);
      metadata.setLabels(labelSelectors);

      String selector = GTSHelper.buildSelector(metadata, true);

      //
      // Check if we have a GTS list for the given selector
      //

      if (null != stack.getAttribute(this.attrkey2)) {
        selectedmetas = (Set<Metadata>) stack.getAttribute(this.attrkey2);
      } else {
        List<String> clsSels = new ArrayList<String>();
        List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();
        clsSels.add(classSelector);
        lblsSels.add(labelSelectors);
        selectedmetas = new HashSet<Metadata>();
        try {
          DirectoryRequest dr = new DirectoryRequest();
          dr.setClassSelectors(clsSels);
          dr.setLabelsSelectors(lblsSels);
          selectedmetas.addAll(stack.getDirectoryClient().find(dr));
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " encountered exception while retrieving Geo Time Series.");
        }
        stack.setAttribute(this.attrkey2, selectedmetas);
      }
    }

    byte[] smallestClassId;
    byte[] smallestLabelsId;
    byte[] smallestTimestamp;

    boolean smallestData = false;

    if (0 == Bytes.indexOf(smallest, Constants.FDB_RAW_DATA_KEY_PREFIX)) {
      // 128BITS
      smallestClassId = Arrays.copyOfRange(smallest, Constants.FDB_RAW_DATA_KEY_PREFIX.length, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8);
      smallestLabelsId = Arrays.copyOfRange(smallest, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 16);
      smallestTimestamp = Arrays.copyOfRange(smallest, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 16, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 24);
      smallestData = true;
    } else if (0 == Bytes.indexOf(smallest, Constants.FDB_METADATA_KEY_PREFIX)) {
      smallestClassId = Arrays.copyOfRange(smallest, Constants.FDB_METADATA_KEY_PREFIX.length, Constants.FDB_METADATA_KEY_PREFIX.length + 8);
      smallestLabelsId = Arrays.copyOfRange(smallest, Constants.FDB_METADATA_KEY_PREFIX.length + 8, Constants.FDB_METADATA_KEY_PREFIX.length + 16);
      smallestTimestamp = null;
      smallestData = false;
    } else {
      throw new WarpScriptException(getName() + " invalid key range.");
    }

    byte[] largestClassId;
    byte[] largestLabelsId;
    byte[] largestTimestamp;

    boolean largestData = false;

    if (0 == Bytes.indexOf(largest, Constants.FDB_RAW_DATA_KEY_PREFIX)) {
      // 128BITS
      largestClassId = Arrays.copyOfRange(largest, Constants.FDB_RAW_DATA_KEY_PREFIX.length, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8);
      largestLabelsId = Arrays.copyOfRange(largest, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 16);
      largestTimestamp = Arrays.copyOfRange(largest, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 16, Constants.FDB_RAW_DATA_KEY_PREFIX.length + 24);
      largestData = true;
    } else if (0 == Bytes.indexOf(largest, Constants.FDB_METADATA_KEY_PREFIX)) {
      largestClassId = Arrays.copyOfRange(largest, Constants.FDB_METADATA_KEY_PREFIX.length, Constants.FDB_METADATA_KEY_PREFIX.length + 8);
      largestLabelsId = Arrays.copyOfRange(largest, Constants.FDB_METADATA_KEY_PREFIX.length + 8, Constants.FDB_METADATA_KEY_PREFIX.length + 16);
      largestTimestamp = null;
      largestData = false;
    } else {
      throw new WarpScriptException(getName() + " invalid key range.");
    }

    if (smallestData != largestData) {
      throw new WarpScriptException(getName() + " unable to find GTS for hybrid SST files.");
    }

    //
    // Scan the keys
    //

    // Boolean indicating the SST file contains only one GTS
    boolean singleGTS = 0 == BytesUtils.compareTo(smallestClassId, largestClassId) && 0 == BytesUtils.compareTo(smallestLabelsId, largestLabelsId);

    List<List<Object>> result = new ArrayList<List<Object>>();

    try {

      List<Metadata> metas = null;

      // Attempt to retrieve the GTS from the stack or else
      // from the Directory
      if (null != stack.getAttribute(this.attrkey)) {
        metas = (List<Metadata>) stack.getAttribute(this.attrkey);
      } else {
        //
        // Create a selector for selecting all GTS
        //
        final List<String> clsSel = new ArrayList<String>();
        clsSel.add("~.*");
        List<Map<String,String>> lblsSel = new ArrayList<Map<String,String>>();
        Map<String,String> labels = new HashMap<String,String>();
        labels.put(Constants.PRODUCER_LABEL, "~.*");
        lblsSel.add(labels);

        DirectoryRequest dr = new DirectoryRequest();
        dr.setClassSelectors(clsSel);
        dr.setLabelsSelectors(lblsSel);
        metas = stack.getDirectoryClient().find(dr);
        metas.sort(MetadataIdComparator.COMPARATOR);
        stack.setAttribute(this.attrkey, metas);
      }

      // 128BITS

      // Create a Metadata for the smallest key of the SST file
      Metadata meta = new Metadata();
      meta.setClassId(Longs.fromByteArray(smallestClassId));
      meta.setLabelsId(Longs.fromByteArray(smallestLabelsId));

      int idx = Collections.binarySearch(metas, meta, MetadataIdComparator.COMPARATOR);

      if (idx < 0) {
        idx = - (idx + 1) - 1; // shift the starting index to the left
        if (idx < 0) {
          idx = 0;
        }
      }

      for(int i = idx; i < metas.size(); i++) {

        meta = metas.get(i);

        boolean isSmallest = false;
        boolean isLargest = false;

        byte[] classId = Longs.toByteArray(meta.getClassId());

        int comp = BytesUtils.compareTo(classId, smallestClassId);

        if (comp < 0) {
          continue;
        }

        if (0 == comp) {
          // Identical class id, check labels id
          byte[] labelsId = Longs.toByteArray(meta.getLabelsId());

          comp = BytesUtils.compareTo(labelsId, smallestLabelsId);

          if (comp < 0) {
            continue;
          }

          if (0 == comp) {
            isSmallest = true;
          }
        }

        // current metadata id is > smallest, check largest

        if (comp > 0) {
          comp = BytesUtils.compareTo(classId, largestClassId);

          // ClassId is after the range of the current SST file, exit prematurely
          if (comp > 0) {
            break;
          }

          if (0 == comp) {
            // Identical class id, check labels id
            byte[] labelsId = Longs.toByteArray(meta.getLabelsId());
            comp = BytesUtils.compareTo(labelsId, largestLabelsId);

            // LabelsId is after the range of the current SST file for ClassId, exit
            if (comp > 0) {
              break;
            }

            if (0 == comp) {
              isLargest = true;
            }
          }
        }


        if (smallestData) {
          // The SST file only contains datapoints
          long startTs = Long.MAX_VALUE;
          long endTs = Long.MIN_VALUE;

          if (isSmallest) {
            startTs = Long.MAX_VALUE - Longs.fromByteArray(smallestTimestamp);
          }

          if (isLargest || singleGTS && isSmallest) {
            endTs = Long.MAX_VALUE - Longs.fromByteArray(largestTimestamp);
          }

          List<Object> gts = new ArrayList<Object>();

          if (null != selectedmetas && !selectedmetas.contains(meta)) {
            gts.add(null);
          } else {
            GeoTimeSerie g = new GeoTimeSerie();
            g.setMetadata(meta);
            gts.add(g);
          }
          gts.add(endTs);
          gts.add(startTs);
          result.add(gts);
        } else {
          // The SST file only contains metadatas
          List<Object> gts = new ArrayList<Object>();

          if (null != selectedmetas && !selectedmetas.contains(meta)) {
            gts.add(null);
          } else {
            GeoTimeSerie g = new GeoTimeSerie();
            g.setMetadata(meta);
            gts.add(g);
          }
          gts.add(null);
          gts.add(null);
          result.add(gts);
        }
      }

      stack.push(result);
    } catch (IOException ioe) {
      throw new WarpScriptException("Error while iterating on GTS.", ioe);
    }

    return stack;
  }
/*
SSTREPORT

DUP 'maxlevel' GET 'maxlevel' STORE
'sst' GET
<%
  DROP
  LIST-> DROP
  'largest' STORE
  'smallest' STORE
  'file' STORE
  $maxlevel !=
  <% NULL %>
  <%
    [ 'TOKEN' 'CLASS_SELECTOR' {} $smallest $largest 'SECRET' ]
    <%
      SSTFIND
      DUP SIZE DUP 1 > SWAP 0 == || // If the sst file contains more than 1 GTS or no GTS at all (because it is a file which has data for an already deleted GTS whose data are not yet expunged), discard it
      <% DROP NULL %>
      <% FLATTEN
         // Only keep the SST file if the single GTS it contains is one we selected
         DUP 0 GET ISNULL
         <% DROP NULL %>
         <% $file SWAP 2 ->LIST %>
         IFTE
      %>
      IFTE
    %>
    <% NULL %> <% %> TRY
  %> IFTE
%> LMAP
[] SWAP
<%
  DUP ISNULL <% DROP %> <% +! %> IFTE
%> FOREACH
 */
}
