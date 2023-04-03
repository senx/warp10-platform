//
//   Copyright 2023  SenX S.A.S.
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

package io.warp10;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import io.warp10.continuum.store.Constants;

public class Warp extends WarpDist {

  private static final String ENV_KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
  private static final String ENV_KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
  private static final String KERBEROS_PRINCIPAL = "kerberos.principal";
  private static final String KERBEROS_KEYTAB = "kerberos.keytab";
  private static final String ENV_JAAS_CONFIG = "JAAS_CONFIG";
  private static final String ENV_KRB5_CONF = "KRB5_CONF";

  private static AtomicBoolean kerberos = new AtomicBoolean(false);

  public static void main(String[] args) throws Exception {

    System.setProperty("java.awt.headless", "true");

    if (StandardCharsets.UTF_8 != Charset.defaultCharset()) {
      throw new RuntimeException("Default encoding MUST be UTF-8 but it is " + Charset.defaultCharset() + ". Aborting.");
    }

    if (!kerberos.get() && (null != System.getenv(ENV_KERBEROS_PRINCIPAL)
        || null != System.getenv(ENV_KERBEROS_KEYTAB)
        || null != System.getenv(ENV_JAAS_CONFIG)
        || null != System.getenv(ENV_KRB5_CONF)
        || null != System.getProperty(KERBEROS_PRINCIPAL)
        || null != System.getProperty(KERBEROS_KEYTAB))) {
      kerberos.set(true);

      if (null != System.getenv(ENV_KRB5_CONF)) {
        System.setProperty("java.security.krb5.conf", System.getenv(ENV_KRB5_CONF));
      }

      if (null != System.getenv(ENV_JAAS_CONFIG)) {
        System.setProperty("java.security.auth.login.config", System.getenv(ENV_JAAS_CONFIG));
      }

      if (null != System.getenv(ENV_KERBEROS_PRINCIPAL)) {
        System.setProperty(KERBEROS_PRINCIPAL, System.getenv(ENV_KERBEROS_PRINCIPAL));
      }

      if (null != System.getenv(ENV_KERBEROS_KEYTAB)) {
        System.setProperty(KERBEROS_KEYTAB, System.getenv(ENV_KERBEROS_KEYTAB));
      }

      Configuration config = new Configuration();

      //
      // Force authentication method to KERBEROS
      //
      SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, config);

      UserGroupInformation.setConfiguration(config);

      String username = System.getProperty(KERBEROS_PRINCIPAL);
      String path = System.getProperty(KERBEROS_KEYTAB);

      if (null == username) {
        throw new RuntimeException("Kerberos principal MUST be set using the '" + ENV_KERBEROS_PRINCIPAL + "' environment variable.");
      }

      if (null == path) {
        throw new RuntimeException("Kerberos keytab path MUST be set using the '" + ENV_KERBEROS_KEYTAB + "' environment variable.");
      }

      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(username, path);
      UserGroupInformation.setLoginUser(ugi);

      ugi.doAs(new PrivilegedExceptionAction() {
        @Override
        public Object run() throws Exception {
          Warp.main(args);
          return null;
        }
      });
    } else {
      if (args.length > 0) {
        setProperties(args);
      } else if (null != System.getProperty(WarpConfig.WARP10_CONFIG)) {
        setProperties(System.getProperty(WarpConfig.WARP10_CONFIG).split("[, ]+"));
      } else if (null != System.getenv(WarpConfig.WARP10_CONFIG_ENV)) {
        setProperties(System.getenv(WarpConfig.WARP10_CONFIG_ENV).split("[, ]+"));
      }

      System.out.println();
      System.out.println(Constants.WARP10_BANNER);
      System.out.println("  Revision " + Revision.REVISION);

      if (null != WarpConfig.getProperty(io.warp10.continuum.Configuration.BACKEND)) {
        if (Constants.BACKEND_FDB.equals(WarpConfig.getProperty(io.warp10.continuum.Configuration.BACKEND))) {
          System.out.println("      Mode standalone+");
        } else {
          System.out.println("      Mode standalone");
        }
        System.out.println();
        io.warp10.standalone.Warp.main(args);
      } else {
        if (kerberos.get()) {
          System.out.println("      Mode distributed with Kerberos");
        } else {
          System.out.println("      Mode distributed");
        }
        System.out.println();
        WarpDist.main(args);
      }
    }
  }
}
