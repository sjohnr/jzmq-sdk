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

package org.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class TestRecorder {

  private static final String MDC_ATTR = "testInfo";

  private Logger _logger = LoggerFactory.getLogger(TestRecorder.class);

  public TestRecorder start() {
    StackTraceElement testCase = Thread.currentThread().getStackTrace()[2];
    String className = testCase.getClassName().substring(testCase.getClassName().lastIndexOf('.') + 1);
    String methodName = testCase.getMethodName();

    MDC.put(MDC_ATTR, String.format("%s#%s", className, methodName));

    _logger.info("\n" +
                 "####################################################################################################################################################################################" +
                 "####################################################################################################################################################################################" +
                 "");

    return this;
  }

  public void log(String log) {
    _logger.info(log);
  }

  public void logQoS(Number rate, String rateStr) {
    _logger.info("QoS: " + rate + " " + rateStr);
  }

  public TestRecorder reset() {
    MDC.remove(MDC_ATTR);
    return this;
  }
}
