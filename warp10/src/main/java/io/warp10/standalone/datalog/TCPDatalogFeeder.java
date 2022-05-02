//
//   Copyright 2020-2022  SenX S.A.S.
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

package io.warp10.standalone.datalog;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.WarpConfig;

public class TCPDatalogFeeder extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TCPDatalogFeeder.class);

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final String DEFAULT_PORT = "3564"; // speeds 'DLNG', DataLog Next Generation
  private static final String DEFAULT_MAXCLIENTS = "2";
  private static final String DEFAULT_BACKLOG = "2";

  private final FileBasedDatalogManager manager;
  private final int port; // = 4321;
  private final InetAddress addr;
  private final int backlog;

  private final int maxclients;

  private final String checkmacro;

  public TCPDatalogFeeder(FileBasedDatalogManager manager) {
    this.manager = manager;

    this.checkmacro = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_CHECKMACRO);

    if (null == checkmacro) {
      throw new RuntimeException("Missing '" + FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_CHECKMACRO + "' configuration.");
    }

    String host = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_HOST, DEFAULT_HOST);

    try {
      this.addr = InetAddress.getByName(host);
    } catch (Exception e) {
      throw new RuntimeException("Error initializing Datalog TCP Feeder on host " + host + ".", e);
    }

    this.port = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_PORT, DEFAULT_PORT));

    this.maxclients = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_MAXCLIENTS, DEFAULT_MAXCLIENTS));
    this.backlog = Integer.parseInt(WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_BACKLOG, DEFAULT_BACKLOG));

    this.setDaemon(true);
    this.setName("[Datalog TCP Feeder on port " + port + "]");
    this.start();
  }

  @Override
  public void run() {
    //
    // Listen on configured port
    //

   ServerSocket server;

   try {
    server = new ServerSocket(this.port, this.backlog, this.addr);
   } catch (Exception e) {
     throw new RuntimeException("Unable to create server socket for Datalog TCP Feeder host " + this.addr + " port " + this.port);
   }

   AtomicInteger clients = new AtomicInteger(0);

   while(true) {
     try {
       Socket socket = server.accept();
       if (clients.incrementAndGet() > maxclients) {
         clients.decrementAndGet();
         socket.close();
       }

       //
       // Allocate a new worker
       //

       TCPDatalogFeederWorker worker = new TCPDatalogFeederWorker(this.manager, socket, clients, this.checkmacro);
     } catch (IOException ioe) {
       LOG.error("Error accepting incoming connection", ioe);
     }
   }
  }
}
