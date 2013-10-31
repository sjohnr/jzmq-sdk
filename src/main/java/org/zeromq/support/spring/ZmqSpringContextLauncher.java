/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.support.spring;

import com.google.common.base.Joiner;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.util.List;

public class ZmqSpringContextLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqSpringContextLauncher.class);

  private static final String CLI_OPT_CONFIG_LOCATION = "configLocation";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    ZmqSpringContextLauncher launcher = new ZmqSpringContextLauncher();
    OptionParser optionParser = launcher.newOptionParser();
    OptionSet opts = optionParser.parse(args);

    List<String> configLocations = (List<String>) opts.valuesOf(CLI_OPT_CONFIG_LOCATION);
    if (configLocations.isEmpty()) {
      optionParser.printHelpOn(System.out);
      System.exit(0);
    }

    ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext();
    for (String configLocation : configLocations) {
      applicationContext.setConfigLocation(configLocation);
    }
    applicationContext.refresh();
    LOG.info("Found spring_context: [{}]. Now deploying zmq components ...", Joiner.on(',').join(configLocations));
    new ZmqSpringContext(applicationContext).deploy();
  }

  private OptionParser newOptionParser() {
    OptionParser optionParser = new OptionParser();
    optionParser.allowsUnrecognizedOptions();
    optionParser.formatHelpWith(new BuiltinHelpFormatter(1024, 8));
    optionParser.accepts(CLI_OPT_CONFIG_LOCATION, "Config location(s) for spring context (" +
                                                  "'classpath' format will be recognized). " +
                                                  "It's allowed to specify multiple occurences of '--configLocation'.")
                .withRequiredArg()
                .ofType(String.class);
    return optionParser;
  }
}
