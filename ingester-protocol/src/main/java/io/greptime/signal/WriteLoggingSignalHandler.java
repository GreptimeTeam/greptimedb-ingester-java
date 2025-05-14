/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.signal;

import io.greptime.Util;
import io.greptime.common.SPI;
import io.greptime.common.signal.FileSignal;
import io.greptime.common.signal.FileSignalHelper;
import io.greptime.common.signal.SignalHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A signal handler that can reset WRITE_LOGGING by {@link Util#resetWriteLogging()}.
 */
@SPI(priority = 95)
public class WriteLoggingSignalHandler implements SignalHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WriteLoggingSignalHandler.class);

    @Override
    public void handle(String signalName) {
        if (FileSignalHelper.ignoreSignal(FileSignal.WriteLogging)) {
            LOG.info("`WRITE_LOGGING`={}.", Util.isWriteLogging());
            return;
        }

        boolean oldValue = Util.resetWriteLogging();
        LOG.info("Reset `WRITE_LOGGING` to {} triggered by signal: {}.", !oldValue, signalName);
    }
}
