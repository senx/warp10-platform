//
//   Copyright 2016  Cityzen Data
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

package io.warp10.worf;

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import io.warp10.quasar.token.thrift.data.TokenType;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Stack;
import java.util.UUID;

public class WorfInteractive {

  private ConsoleReader reader;
  private PrintWriter out;
  private Properties worfConfig = null;


  public WorfInteractive() throws WorfException {
    try {
      reader = new ConsoleReader();
      out = new PrintWriter(reader.getOutput());
      out.println("Welcome to warp10 token command line");
      out.println("I am Worf, security chief of the USS Enterprise (NCC-1701-D)");

    } catch (IOException e) {
      throw new WorfException("Unexpected Worf error:" + e.getMessage());
    }
  }

  public PrintWriter getPrintWriter() {
    return out;
  }

  public String runTemplate(Properties config, String warp10Configuration) throws WorfException {
    try {
      out.println("The configuration file is a template");
      WorfTemplate template = new WorfTemplate(config, warp10Configuration);

      out.println("Generating crypto keys...");
      for (String cryptoKey : template.getCryptoKeys()) {
        String keySize = template.generateCryptoKey(cryptoKey);
        if (keySize != null) {
          out.println(keySize + " bits secured key for " + cryptoKey + "  generated");
        } else {
          out.println("Unable to generate " + cryptoKey + ", template error");
        }
      }

      out.println("Crypto keys generated");


      Stack<Pair<String, String[]>> fieldsStack = template.getFieldsStack();

      if (fieldsStack.size() > 0) {
        out.println("Update configuration...");
      }

      while (!fieldsStack.isEmpty()) {
        Pair<String, String[]> templateValues = fieldsStack.peek();
        String replaceValue = null;
        // get user input
        switch (templateValues.getValue()[0]) {
          case "path":
            replaceValue = readInputPath(reader, out, templateValues.getValue()[2]);
            break;
          case "host":
            replaceValue = readHost(reader, out, templateValues.getValue()[2]);
            break;
          case "int":
            replaceValue = readInteger(reader, out, templateValues.getValue()[2]);
            break;
        }

        if (replaceValue == null) {
          out.println("Unable to update " + templateValues.getValue()[1] + " key, enter a valid " + templateValues.getValue()[0]);
          continue;
        }

        // replace template value
        template.updateField(templateValues.getKey(), replaceValue);

        // field updated pop
        fieldsStack.pop();
      }

      out.println("Configuration updated.");

      // save file
      Path warp10ConfigurationPath = Paths.get(warp10Configuration);
      String outputFileName = warp10ConfigurationPath.getFileName().toString();
      outputFileName = outputFileName.replace("template", "conf");
      String outputPath = readInputPath(reader, out, "save config:output path", warp10ConfigurationPath.getParent().toString());
      String outputFilename = readInputString(reader, out, "save config:output filename", outputFileName);

      if (Strings.isNullOrEmpty(outputPath) || Strings.isNullOrEmpty(outputFilename)) {
        throw new Exception("Path or filename empty, unable to save configuration file!");
      }

      StringBuilder sb = new StringBuilder();
      sb.append(outputPath);
      if (!outputPath.endsWith(File.separator)) {
        sb.append(File.separator);
      }
      sb.append(outputFilename);

      warp10Configuration = sb.toString();
      template.saveConfig(warp10Configuration);

      out.println("Configuration saved. filepath=" + warp10Configuration);
      out.println("Reading warp10 configuration " + warp10Configuration);
      return warp10Configuration;
    } catch (Exception exp) {
      throw new WorfException("Unexpected Worf error:" + exp.getMessage());
    }
  }

  public int run(Properties config, Properties worfConfig) throws WorfException {
    this.worfConfig = worfConfig;
    try {
      String line = null;
      TokenCommand currentCommand = null;
      Completer defaultCompleter = null;

      // extract token + oss keys
      // create keystore
      WorfKeyMaster worfKeyMaster = new WorfKeyMaster(config);

      if (!worfKeyMaster.loadKeyStore()) {
        out.println("Unable to load warp10 keystore.");
        out.flush();
        return -1;
      }

      out.println("Warp10 keystore initialized.");
      // completers
      defaultCompleter = new StringsCompleter("quit", "exit", "cancel", "read", "write", "encodeToken", "decodeToken", "uuidgen");

      reader.addCompleter(defaultCompleter);
      reader.setPrompt("warp10> ");

      while ((line = reader.readLine()) != null) {
        line = line.trim();

        // Select command
        // ---------------------------------------------------------
        if (line.equalsIgnoreCase("encodeToken")) {
          currentCommand = new EncodeTokenCommand();
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        } else if (line.equalsIgnoreCase("decodeToken")) {
          currentCommand = new DecodeTokenCommand();
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        } else if (line.equalsIgnoreCase("cancel")) {
          currentCommand = null;
          reader.setPrompt(getPromptMessage(currentCommand));
          // loop nothing more to do
          continue;
        }

        // execute command
        // ----------------------------------------------------------
        if (currentCommand != null && currentCommand.isReady()) {
          if (currentCommand.execute(line.toLowerCase(), worfKeyMaster, out)) {
            // reset current command
            currentCommand = null;
          }
        }

        // get input command
        // ----------------------------------------------------------
        getInputField(currentCommand, line, out);
        reader.setPrompt(getPromptMessage(currentCommand));

        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
          break;
        }

      }

      System.out.println("Bye!");
      return 0;
    } catch (Exception exp) {
      throw new WorfException("Unexpected Worf error:" + exp.getMessage(), exp);
    }
  }

  private static String readInputPath(ConsoleReader reader, PrintWriter out, String prompt, String defaultPath) {
    try {
      // save file
      StringBuilder sb = new StringBuilder();
      sb.append("warp10:");
      sb.append(prompt);
      if (!Strings.isNullOrEmpty(defaultPath)) {
        sb.append(", default(");
        sb.append(defaultPath);
        sb.append(")");
      }
      sb.append(">");

      reader.setPrompt(sb.toString());
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (Strings.isNullOrEmpty(line) && Strings.isNullOrEmpty(defaultPath)) {
          continue;
        }

        if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(defaultPath)) {
          return defaultPath;
        }

        if (line.equalsIgnoreCase("cancel")) {
          break;
        }

        Path outputPath = Paths.get(line);
        if (Files.notExists(outputPath)) {
          out.println("The path " + line + " does not exists");
          continue;
        }
        return line;
      }
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the path. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInputPath(ConsoleReader reader, PrintWriter out, String prompt) {
    return readInputPath(reader, out, prompt, null);
  }

  private static String readHost(ConsoleReader reader, PrintWriter out, String prompt) {
    try {
      String inputString = readInputString(reader, out, prompt);

      if (InetAddresses.isInetAddress(inputString)) {
        return inputString;
      }
      out.println("Error, " + inputString + " is not a valid inet address");
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the host. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInteger(ConsoleReader reader, PrintWriter out, String prompt) {
    try {
      String inputString = readInputString(reader, out, prompt);

      if (NumberUtils.isNumber(inputString)) {
        return inputString;
      }

      if (InetAddresses.isInetAddress(inputString)) {
        return inputString;
      }
      out.println("Error, " + inputString + " is not a number");
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the host. error=" + exp.getMessage());
    }
    return null;
  }

  private static String readInputString(ConsoleReader reader, PrintWriter out, String prompt, String defaultString) {
    try {
      // save file
      StringBuilder sb = new StringBuilder("warp10:");
      sb.append(prompt);
      if (!Strings.isNullOrEmpty(defaultString)) {
        sb.append(", default(");
        sb.append(defaultString);
        sb.append(")");
      }
      sb.append(">");
      reader.setPrompt(sb.toString());
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (Strings.isNullOrEmpty(line) && Strings.isNullOrEmpty(defaultString)) {
          continue;
        }
        if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(defaultString)) {
          return defaultString;
        }
        if (line.equalsIgnoreCase("cancel")) {
          break;
        }
        return line;
      }
    } catch (Exception exp) {
      if (WorfCLI.verbose) {
        exp.printStackTrace();
      }
      out.println("Error, unable to read the string. error=" + exp.getMessage());
    }
    return null;

  }

  private static String readInputString(ConsoleReader reader, PrintWriter out, String prompt) {
    return readInputString(reader, out, prompt, null);
  }


  private void getInputField(TokenCommand command, String input, PrintWriter out) {
    if (command == null) {
      return;
    }

    switch (command.commandName) {
      case "encodeToken":
        EncodeTokenCommand encodeTokenCommand = (EncodeTokenCommand) command;

        if (encodeTokenCommand.tokenType == null) {
          encodeTokenCommand.tokenType = getTokenType(input, out);
        } else if (encodeTokenCommand.application == null) {
          encodeTokenCommand.application = getApplicationName(input, out);
        } else if (encodeTokenCommand.producer == null) {
          encodeTokenCommand.producer = getUUID(input, out, WorfCLI.P_UUID, null);
        } else if (encodeTokenCommand.owner == null) {
          encodeTokenCommand.owner = getUUID(input, out, WorfCLI.O_UUID, encodeTokenCommand.producer);
        } else if (encodeTokenCommand.ttl == 0L) {
          encodeTokenCommand.ttl = getTTL(input, out);
        }
        break;

      case "decodeToken":
        DecodeTokenCommand decodeTokenCommand = (DecodeTokenCommand) command;
        if (decodeTokenCommand.token == null) {
          decodeTokenCommand.token = getString(input, out);
        }
        break;
    }
  }

  private String getPromptMessage(TokenCommand command) {
    if (command == null) {
      return "warp10>";
    }

    StringBuilder sb = new StringBuilder();
    String defaultValue = null;

    sb.append(command.commandName);

    if (command instanceof EncodeTokenCommand) {
      EncodeTokenCommand encodeTokenCommand = (EncodeTokenCommand) command;

      // updates prompt
      if (encodeTokenCommand.tokenType == null) {
        sb.append("/token type (read|write)");
      } else if (encodeTokenCommand.application == null) {
        sb.append("/application name");
        defaultValue = getApplicationName(null, null);
      } else if (encodeTokenCommand.producer == null) {
        sb.append("/data producer UUID");
        defaultValue = getUUID(null, null, WorfCLI.P_UUID, null);
      } else if (encodeTokenCommand.owner == null) {
        sb.append("/data owner UUID");
        defaultValue = getUUID(null, null, WorfCLI.O_UUID, encodeTokenCommand.producer);
      } else if (encodeTokenCommand.ttl == 0L) {
        sb.append("/token ttl (ms) ");
      } else {
        sb.append("(generate | cancel)");
      }
    }

    if (command instanceof DecodeTokenCommand) {
      DecodeTokenCommand decodeTokenCommand = (DecodeTokenCommand) command;

      if (decodeTokenCommand.token == null) {
        sb.append("/token");
      } else if (decodeTokenCommand.writeToken != null) {
        sb.append("convert to read token ? (yes)");
      } else {
        sb.append("(decode | cancel)");
      }

    }

    // ads default value to the prompt
    if (!Strings.isNullOrEmpty(defaultValue)) {
      sb.append(", default (");
      sb.append(defaultValue);
      sb.append(")");
    }

    sb.append(">");

    return sb.toString();
  }


  private static TokenType getTokenType(String line, PrintWriter out) {
    try {
      if (Strings.isNullOrEmpty(line)) {
        return null;
      }

      return TokenType.valueOf(line.toUpperCase());
    } catch (Exception exp) {
      out.println("token type " + line + " unknown. (read|write) expected");
      return null;
    }
  }

  private String getString(String line, PrintWriter out) {
    try {
      if (Strings.isNullOrEmpty(line)) {
        return null;
      } else {
        return line.trim();
      }
    } catch (Exception exp) {
      if (out != null) {
        out.println("Unable to get application name cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private String getApplicationName(String line, PrintWriter out) {
    try {
      String appName = Worf.getDefault(worfConfig, out, line, WorfCLI.APPNAME);

      if (Strings.isNullOrEmpty(appName)) {
        return null;
      }
      return appName;
    } catch( Exception exp) {
      if (out!=null) {
        out.println("Unable to get application name cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private String getUUID(String line, PrintWriter out, String worfDefault, String uuidDefault) {
    try {
      String strUuid = Worf.getDefault(worfConfig, out, line, worfDefault);
      UUID uuid = null;

      if (Strings.isNullOrEmpty(strUuid) && Strings.isNullOrEmpty(uuidDefault)) {
        return null;
      }

      // no input and default available
      if (Strings.isNullOrEmpty(line) && !Strings.isNullOrEmpty(uuidDefault)) {
        return uuidDefault;
      }

      if ("uuidgen".equals(strUuid)) {
        // generate uuid from cmd line
        uuid = UUID.randomUUID();
        if (out!=null) {
          out.println("uuid generated=" + strUuid);
        }
      } else {
        // take default
        uuid = UUID.fromString(strUuid);
      }
      return uuid.toString();
    } catch( Exception exp) {
      if (out!=null) {
        out.println("UUID not valid cause=" + exp.getMessage());
      }
      return null;
    }
  }

  private static long getTTL(String line, PrintWriter out) {
    try {
      long ttl = Long.valueOf(line);

      long ttlMax = Long.MAX_VALUE - System.currentTimeMillis();

      if (ttl >= ttlMax) {
        out.println("TTL can not be upper than " + ttlMax);
        return 0L;
      }

      return ttl;
    } catch (NumberFormatException exp) {
      out.println(line + " is not a long");
      return 0L;
    }
  }
}
