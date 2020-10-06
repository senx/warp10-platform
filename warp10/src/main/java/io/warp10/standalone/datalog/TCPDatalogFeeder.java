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
  
  private final FileBasedDatalogManager manager;
  private final int port = 4321;
  private final InetAddress addr;
  private final int backlog = 2;
  
  private static final int MAX_CLIENTS = 2;
  
  private final String checkmacro;
  
  public TCPDatalogFeeder(FileBasedDatalogManager manager) {
    this.manager = manager;
  
    this.checkmacro = WarpConfig.getProperty(FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_CHECKMACRO);
    
    if (null == checkmacro) {
      throw new RuntimeException("Missing '" + FileBasedDatalogManager.CONFIG_DATALOG_FEEDER_CHECKMACRO + "' configuration.");
    }
    
    try {
      this.addr = InetAddress.getByName("127.0.0.1");
    } catch (Exception e) {
      throw new RuntimeException("Error initializing Datalog TCP Forwarder.", e);
    }
    this.setDaemon(true);
    this.setName("[Datalog TCP Forwarder on port " + port + "]");
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
     throw new RuntimeException("Unable to create server socket for Datalog TCP Forwarder host " + this.addr + " port " + this.port);
   }
   
   AtomicInteger clients = new AtomicInteger(0);
   
   while(true) {
     try {
       Socket socket = server.accept();
       if (clients.incrementAndGet() > MAX_CLIENTS) {
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
