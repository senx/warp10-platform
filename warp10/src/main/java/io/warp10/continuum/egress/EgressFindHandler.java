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
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.ThrowableUtils;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.KeyStore;
import io.warp10.json.JsonUtils;
import io.warp10.quasar.token.thrift.data.ReadToken;
import io.warp10.script.WarpScriptException;
import io.warp10.script.functions.PARSESELECTOR;
import io.warp10.sensision.Sensision;

public class EgressFindHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EgressFindHandler.class);

  private final DirectoryClient directoryClient;

  public EgressFindHandler(KeyStore keystore, DirectoryClient directoryClient) {
    this.directoryClient = directoryClient;
  }

  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

    if (target.equals(Constants.API_ENDPOINT_FIND)) {
      baseRequest.setHandled(true);
    } else {
      return;
    }

    //
    // Add CORS header
    //

    resp.setHeader("Access-Control-Allow-Origin", "*");

    String selector = req.getParameter(Constants.HTTP_PARAM_SELECTOR);

    String token = req.getParameter(Constants.HTTP_PARAM_TOKEN);

    long gskip = 0L;
    long gcount = Long.MAX_VALUE;

    boolean mustSort = false;

    if (null != req.getParameter(Constants.HTTP_PARAM_GSKIP)) {
      gskip = Long.parseLong(req.getParameter(Constants.HTTP_PARAM_GSKIP));
      mustSort = true;
    }

    // 'limit' predates 'gcount', it may be overriden if 'gcount' is set too
    if (null != req.getParameter(Constants.HTTP_PARAM_LIMIT)) {
      gcount = Long.parseLong(req.getParameter(Constants.HTTP_PARAM_LIMIT));
    }

    // 'gcount' overrides 'limit'
    if (null != req.getParameter(Constants.HTTP_PARAM_GCOUNT)) {
      gcount = Long.parseLong(req.getParameter(Constants.HTTP_PARAM_GCOUNT));
      mustSort = true;
    }

    if (null == token) {
      token = req.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
    }

    if (null == token) {
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing token.");
      return;
    }

    try {
      String format = req.getParameter(Constants.HTTP_PARAM_FORMAT);
      boolean json = "json".equals(format);

      boolean showErrors = null != req.getParameter(Constants.HTTP_PARAM_SHOW_ERRORS);

      boolean showUUID = null != req.getParameter(Constants.HTTP_PARAM_SHOWUUID);

      boolean showAttr = null != req.getParameter(Constants.HTTP_PARAM_SHOWATTR);
      boolean sortMeta = null != req.getParameter(Constants.HTTP_PARAM_SORTMETA);

      Long activeAfter = null == req.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER) ? null : Long.parseLong(req.getParameter(Constants.HTTP_PARAM_ACTIVEAFTER));
      Long quietAfter = null == req.getParameter(Constants.HTTP_PARAM_QUIETAFTER) ? null : Long.parseLong(req.getParameter(Constants.HTTP_PARAM_QUIETAFTER));

      ReadToken rtoken;

      try {
        rtoken = Tokens.extractReadToken(token);

        Map<String, String> rtokenAttributes = rtoken.getAttributes();
        if (null != rtokenAttributes && rtokenAttributes.containsKey(Constants.TOKEN_ATTR_NOFIND)) {
          resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Token cannot be used for finding metadata.");
          return;
        }

      } catch (WarpScriptException wse) {
        resp.sendError(HttpServletResponse.SC_FORBIDDEN, "Missing or invalid token.");
        return;
      }

      boolean expose = rtoken.getAttributesSize() > 0 && rtoken.getAttributes().containsKey(Constants.TOKEN_ATTR_EXPOSE);

      String[] selectors = selector.split("\\s+");

      PrintWriter pw = resp.getWriter();

      if (json) {
        pw.println("[");
      }

      StringBuilder sb = new StringBuilder();

      boolean first = true;

      try {
        for (String sel: selectors) {
          if (gcount <= 0) {
            break;
          }

          Object[] elts = null;

          try {
            elts = PARSESELECTOR.parse(sel);
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

          DirectoryRequest request = new DirectoryRequest();
          request.setSorted(mustSort);
          request.setClassSelectors(clsSels);
          request.setLabelsSelectors(lblsSels);

          if (null != activeAfter) {
            request.setActiveAfter(activeAfter);
          }
          if (null != quietAfter) {
            request.setQuietAfter(quietAfter);
          }

          try (MetadataIterator iterator = directoryClient.iterator(request)) {
            while(iterator.hasNext()) {
              if (gcount <= 0) {
                break;
              }

              Metadata metadata = iterator.next();

              if (gskip > 0) {
                gskip--;
                continue;
              }

              gcount--;

              if (showUUID) {
                UUID uuid = new UUID(metadata.getClassId(), metadata.getLabelsId());
                if (null != metadata.getAttributes()) {
                  metadata.setAttributes(new HashMap<String,String>(metadata.getAttributes()));
                }
                metadata.putToAttributes(Constants.UUID_ATTRIBUTE, uuid.toString());
              }

              if (json) {
                // Remove internal labels, need to copy the map as it is Immutable in Metadata
                if (null != metadata.getLabels()) {
                  metadata.setLabels(new HashMap<String,String>(metadata.getLabels()));
                  if (!Constants.EXPOSE_OWNER_PRODUCER && !expose) {
                    metadata.getLabels().remove(Constants.OWNER_LABEL);
                    metadata.getLabels().remove(Constants.PRODUCER_LABEL);
                  }
                }
                if (!first) {
                  pw.println(",");
                } else {
                  first = false;
                }
                JsonUtils.objectToJson(pw, metadata, true);
                continue;
              }

              sb.setLength(0);

              GTSHelper.encodeName(sb, metadata.getName());

              if (metadata.getLabelsSize() > 0) {
                if (sortMeta) {
                  GTSHelper.labelsToString(sb, new TreeMap<String,String>(metadata.getLabels()), expose);
                } else {
                  GTSHelper.labelsToString(sb, metadata.getLabels(), expose);
                }
              }

              if (showAttr) {
                if (metadata.getAttributesSize() > 0) {
                  // For attributes we force 'expose' to be true
                  if (sortMeta) {
                    GTSHelper.labelsToString(sb, new TreeMap<String,String>(metadata.getAttributes()), true);
                  } else {
                    GTSHelper.labelsToString(sb, metadata.getAttributes(), true);
                  }
                } else {
                  sb.append("{}");
                }
              }

              pw.println(sb.toString());
            }
          } catch (Throwable t) {
            throw t;
          }

          if (gcount <= 0) {
            break;
          }
        }
        if (json) {
          pw.println();
          pw.println("]");
        }
      } catch (Throwable t) {
        LOG.error("",t);
        Sensision.update(SensisionConstants.CLASS_WARP_FIND_ERRORS, Sensision.EMPTY_LABELS, 1);
        if (showErrors) {
          pw.println();
          StringWriter sw = new StringWriter();
          PrintWriter pw2 = new PrintWriter(sw);
          t.printStackTrace(pw2);
          pw2.close();
          sw.flush();
          String error = URLEncoder.encode(sw.toString(), StandardCharsets.UTF_8.name());
          pw.println(Constants.EGRESS_FIND_ERROR_PREFIX + error);
        }
        throw new IOException(t);
      }
    } catch (Exception e) {
      if (!resp.isCommitted()) {
        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ThrowableUtils.getErrorMessage(e, Constants.MAX_HTTP_REASON_LENGTH));
        return;
      }
    }
  }
}
