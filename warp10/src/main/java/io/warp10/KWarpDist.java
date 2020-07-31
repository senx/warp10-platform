//
//   Copyright 2020  SenX S.A.S.
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

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

public class KWarpDist {
  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    
    //
    // Force authentication method to KERBEROS
    //    
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, config);
    
    UserGroupInformation.setConfiguration(config);
    
    String username = System.getProperty("kwarp.user");
    String path = System.getProperty("kwarp.keytab");
    
    UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(username, path);
    UserGroupInformation.setLoginUser(userGroupInformation);

    User user = User.create(userGroupInformation);
    
    user.runAs(new PrivilegedExceptionAction() {
        @Override
        public Object run() throws Exception {
          WarpDist.main(args);
          return null;
        }
    });
  }
}
