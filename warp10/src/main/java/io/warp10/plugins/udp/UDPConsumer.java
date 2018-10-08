package io.warp10.plugins.udp;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;
import io.warp10.script.WarpScriptStopException;

public class UDPConsumer extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(UDPConsumer.class);

  private static final int DEFAULT_QSIZE = 1024;

  private static final String PARAM_MACRO = "macro";
  private static final String PARAM_PARALLELISM = "parallelism";
  private static final String PARAM_PARTITIONER = "partitioner";
  private static final String PARAM_QSIZE = "qsize";
  private static final String PARAM_UDP_HOST = "host";
  private static final String PARAM_UDP_PORT = "port";
  private static final String PARAM_UDP_TIMEOUT = "timeout";

  private final MemoryWarpScriptStack stack;
  private final Macro macro;
  private final Macro partitioner;
  private final String host;
  private long timeout = 0;

  private final int parallelism;
  private final int port;

  private boolean done;

  private final String warpscript;

  private final LinkedBlockingQueue<DatagramPacket> queue;
  private final LinkedBlockingQueue<DatagramPacket>[] queues;

  private Thread[] executors;

  private DatagramSocket socket;

  public UDPConsumer(Path p) throws Exception {
    //
    // Read content of mc2 file
    //

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    InputStream in = new FileInputStream(p.toFile());
    byte[] buf = new byte[8192];

    while (true) {
      int len = in.read(buf);
      if (len < 0) {
        break;
      }
      baos.write(buf, 0, len);
    }

    in.close();

    this.warpscript = new String(baos.toByteArray(), Charsets.UTF_8);
    this.stack = new MemoryWarpScriptStack(null, null, new Properties());
    stack.maxLimits();

    try {
      stack.execMulti(this.warpscript);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error("Caught exception while loading '" + p.getFileName() + "'.", t);
    }

    Object top = stack.pop();

    if (!(top instanceof Map)) {
      throw new RuntimeException("UDP consumer spec must leave a configuration map on top of the stack.");
    }

    Map<Object, Object> config = (Map<Object, Object>) top;

    //
    // Extract parameters
    //

    this.macro = (Macro) config.get(PARAM_MACRO);
    this.partitioner = (Macro) config.get(PARAM_PARTITIONER);
    this.host = String.valueOf(config.get(PARAM_UDP_HOST));
    this.port = ((Number) config.get(PARAM_UDP_PORT)).intValue();
    this.parallelism = Integer.parseInt(null != config.get(PARAM_PARALLELISM) ? String.valueOf(config.get(PARAM_PARALLELISM)) : "1");

    if (config.containsKey(PARAM_UDP_TIMEOUT)) {
      this.timeout = Long.parseLong(String.valueOf(config.get(PARAM_UDP_TIMEOUT)));
    }

    int qsize = DEFAULT_QSIZE;

    if (null != config.get(PARAM_QSIZE)) {
      qsize = Integer.parseInt(String.valueOf(config.get(PARAM_QSIZE)));
    }

    if (null == this.partitioner) {
      this.queue = new LinkedBlockingQueue<DatagramPacket>(qsize);
      this.queues = null;
    } else {
      this.queue = null;
      this.queues = new LinkedBlockingQueue[this.parallelism];
      for (int i = 0; i < this.parallelism; i++) {
        this.queues[i] = new LinkedBlockingQueue<DatagramPacket>(qsize);
      }
    }

    //
    // Create UDP socket
    //
    this.socket = new DatagramSocket(this.port, InetAddress.getByName(this.host));
    this.socket.setReceiveBufferSize(Integer.MAX_VALUE); // Set SO_RCVBUF to /proc/sys/net/core/rmem_max

    this.setDaemon(true);
    this.setName("[UDP Endpoint port " + this.port + "]");
    this.start();
  }

  @Override
  public void run() {

    this.executors = new Thread[this.parallelism];

    for (int i = 0; i < this.parallelism; i++) {

      final MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
      stack.maxLimits();

      final LinkedBlockingQueue<DatagramPacket> queue = null == this.partitioner ? this.queue : this.queues[i];

      executors[i] = new Thread() {
        @Override
        public void run() {
          while (true) {

            try {
              DatagramPacket msg = null;

              if (timeout > 0) {
                msg = queue.poll(timeout, TimeUnit.MILLISECONDS);
              } else {
                msg = queue.take();
              }

              stack.clear();

              if (null != msg) {
                stack.push(msg.getAddress().getHostAddress());
                stack.push((long) msg.getPort());
                stack.push(Arrays.copyOfRange(msg.getData(), msg.getOffset(), msg.getOffset() + msg.getLength()));
              } else {
                stack.push(null);
              }

              stack.exec(macro);
            } catch (InterruptedException e) {
              return;
            } catch (WarpScriptStopException wsse) {
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      };

      executors[i].setName("[UDP Executor #" + i + "]");
      executors[i].setDaemon(true);
      executors[i].start();
    }

    while (!done) {
      try {

        byte[] buf = new byte[65507];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        this.socket.receive(packet);

        try {
          // Apply the partitioning macro if it is defined
          if (null != this.partitioner) {
            this.stack.clear();
            this.stack.push(packet.getAddress().getHostAddress());
            this.stack.push((long) packet.getPort());
            this.stack.push(Arrays.copyOfRange(packet.getData(), packet.getOffset(), packet.getOffset() + packet.getLength()));
            this.stack.exec(this.partitioner);
            int seq = ((Number) this.stack.pop()).intValue();
            this.queues[seq % this.parallelism].put(packet);
            if (16 > this.queues[seq % this.parallelism].remainingCapacity()) {
              LOG.warn("Packet queue almost full, increase parallelism or make your macro faster.");
            }
          } else {
            this.queue.put(packet);
            if (16 > this.queue.remainingCapacity()) {
              LOG.warn("Packet queue almost full, increase parallelism or make your macro faster.");
            }
          }
        } catch (Exception e) {
          // Ignore exceptions
        }
      } catch (Exception e) {
        LOG.error("Caught exception while receiving message", e);
      }
    }

  }

  public void end() {
    this.done = true;
    try {
      this.socket.close();

      for (Thread t : this.executors) {
        t.interrupt();
      }
    } catch (Exception e) {
    }
  }

  public String getWarpScript() {
    return this.warpscript;
  }
}
