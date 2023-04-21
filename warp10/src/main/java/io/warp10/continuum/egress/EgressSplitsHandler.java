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

package io.warp10.continuum.egress;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Transaction;

import io.warp10.continuum.Tokens;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.GTSSplit;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.fdb.FDBPool;
import io.warp10.fdb.FDBStoreClient;
import io.warp10.fdb.FDBUtils;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;

/**
 * This handler will generate splits from a selector and a token, those
 * splits will then be used by the InputFormat to retrieve data for MR job
 */
public class EgressSplitsHandler extends AbstractHandler {

  public static final String DUMMY_SERVER = "dummy";

  private final DirectoryClient directoryClient;

  private final FDBStoreClient storeClient;

  private final byte[] fetcherKey;

  public EgressSplitsHandler(KeyStore keystore, DirectoryClient directoryClient, FDBStoreClient storeClient) {
    this.directoryClient = directoryClient;
    this.storeClient = storeClient;

    this.fetcherKey = keystore.getKey(KeyStore.AES_FETCHER);
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (!target.equals(Constants.API_ENDPOINT_SPLITS)) {
      return;
    }

    baseRequest.setHandled(true);

    //
    // Extract parameters
    //

    String token = request.getParameter(Constants.HTTP_PARAM_TOKEN);
    String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);
    String now = request.getParameter(Constants.HTTP_PARAM_NOW);

    Long activeAfter = null == request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
    Long quietAfter = null == request.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(request.getParameter(Constants.HTTP_PARAM_QUIETAFTER));

    long gskip = 0L;
    long gcount = Long.MAX_VALUE;

    if (null != request.getParameter(Constants.HTTP_PARAM_GSKIP)) {
      gskip = Long.parseLong(request.getParameter(Constants.HTTP_PARAM_GSKIP));
    }

    if (null != request.getParameter(Constants.HTTP_PARAM_GCOUNT)) {
      gcount = Long.parseLong(request.getParameter(Constants.HTTP_PARAM_GCOUNT));
    }

    //
    // Validate token
    //

    ReadToken rtoken;

    try {
      rtoken = Tokens.extractReadToken(token);

      if (rtoken.getHooksSize() > 0) {
        throw new IOException("Tokens with hooks cannot be used for generating splits.");
      }

      Map<String, String> rtokenAttributes = rtoken.getAttributes();
      if (null != rtokenAttributes && (rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFETCH) || rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFIND))) {
        throw new IOException("Token cannot be used for fetching data.");
      }
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    //
    // Parse selector
    //

    Object[] elts = null;

    try {
      elts = PARSESELECTOR.parse(selector);
    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    //
    // Force app/owner/producer from token
    //

    String classSelector = elts[0].toString();
    Map<String,String> labelsSelector = (Map<String,String>) elts[1];

    labelsSelector.remove(Constants.PRODUCER_LABEL);
    labelsSelector.remove(Constants.OWNER_LABEL);
    labelsSelector.remove(Constants.APPLICATION_LABEL);

    labelsSelector.putAll(Tokens.labelSelectorsFromReadToken(rtoken));

    List<String> clsSels = new ArrayList<String>();
    List<Map<String,String>> lblsSels = new ArrayList<Map<String,String>>();

    clsSels.add(classSelector);
    lblsSels.add(labelsSelector);

    //
    // Determine the list of fetchers we can use
    //

    FDBPool fdbPool = this.storeClient.getPool();
    Database db = fdbPool.getDatabase();

    Transaction txn = null;

    DirectoryRequest drequest = new DirectoryRequest();
    drequest.setClassSelectors(clsSels);
    drequest.setLabelsSelectors(lblsSels);

    if (null != activeAfter) {
      drequest.setActiveAfter(activeAfter);
    }
    if (null != quietAfter) {
      drequest.setQuietAfter(quietAfter);
    }

    try (MetadataIterator metadatas = directoryClient.iterator(drequest)) {

      //
      // We output a single split per Metadata, split combining is the
      // responsibility of the InputFormat
      // 128bits
      //

      byte[] row = new byte[Constants.FDB_RAW_DATA_KEY_PREFIX.length + 8 + 8 + 8];
      System.arraycopy(Constants.FDB_RAW_DATA_KEY_PREFIX, 0, row, 0, Constants.FDB_RAW_DATA_KEY_PREFIX.length);

      PrintWriter pw = response.getWriter();

      while(metadatas.hasNext()) {
        Metadata metadata = metadatas.next();

        if (gskip > 0) {
          gskip--;
          continue;
        }

        gcount--;

        if (gcount < 0) {
          break;
        }

        //
        // Build row of the GTS
        // 128bits
        //

        long classId = metadata.getClassId();
        long labelsId = metadata.getLabelsId();

        int offset = Constants.FDB_RAW_DATA_KEY_PREFIX.length;

        // Add classId
        for (int i = 7; i >= 0; i--) {
          row[offset+i] = (byte) (classId & 0xFFL);
          classId >>>= 8;
        }

        offset += 8;

        // Add labelsId
        for (int i = 7; i >= 0; i--) {
          row[offset+i] = (byte) (labelsId & 0xFFL);
          labelsId >>>= 8;
        }

        offset += 8;

        // Add timestamp

        if (null != now) {
          long ts = Long.MAX_VALUE - Long.parseLong(now);

          for (int i = 7; i >= 0; i--) {
            row[offset + i] = (byte) (ts & 0xFFL);
            ts >>>= 8;
          }
        } else {
          Arrays.fill(row, offset, row.length, (byte) 0x00);
        }


        boolean retry = true;

        String[] addresses = null;

        while (retry) {
          retry = false;
          try {
            if (null == txn) {
              txn = db.createTransaction();
            }
            addresses = LocalityUtil.getAddressesForKey(txn, row).get();
          } catch (Throwable t) {
            FDBUtils.errorMetrics("egress", t.getCause());
            if (null != txn) {
              try {
                txn.close();
              } catch (Throwable tt) {
              } finally {
                txn = null;
              }
            }
            if (t.getCause() instanceof FDBException) {
              if (1007 == ((FDBException) t.getCause()).getCode()) {
                // Transaction is too old, issue a new one
                retry = true;
                txn = null;
              } else if (1033 == ((FDBException) t.getCause()).getCode()) {
                // locality_information_unavailable
                // Issue a dummy server
                addresses = new String[1];
                addresses[0] = DUMMY_SERVER;
              } else {
                throw new IOException("Error while fetching splits.", t);
              }
            } else {
              throw new IOException("Error while fetching splits", t);
            }
          }
        }

        //
        // Build Split
        //

        GTSSplit split = new GTSSplit();

        split.setTimestamp(System.currentTimeMillis());
        split.setExpiry(rtoken.getExpiryTimestamp());
        split.addToMetadatas(metadata);
        split.setToken(rtoken);

        //
        // Serialize and encrypt Split
        //

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        byte[] data = null;

        try {
          data = serializer.serialize(split);
        } catch (TException te) {
          throw new IOException(te);
        }

        if (null != fetcherKey) {
          data = CryptoUtils.wrap(fetcherKey, data);
        }

        pw.print(addresses[0]); // Server address
        pw.print(" ");
        pw.print(addresses[0]); // Region name, we use the server address
        pw.print(" ");
        pw.println(new String(OrderPreservingBase64.encode(data), StandardCharsets.US_ASCII));
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (null != txn) {
        try {
          txn.close();
        } catch (Throwable t) {
        }
      }
    }
  }
}
