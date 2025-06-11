/*
 * Copyright (C) 2025 Isima, Inc.
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
package io.isima.trino.bios;

import io.trino.spi.TrinoException;

import static io.isima.trino.bios.BiosClient.SCHEMA_CONTEXTS;
import static io.isima.trino.bios.BiosClient.SCHEMA_CONTEXTS_RAW;
import static io.isima.trino.bios.BiosClient.SCHEMA_CONTEXTS_SKETCHES;
import static io.isima.trino.bios.BiosClient.SCHEMA_SIGNALS;
import static io.isima.trino.bios.BiosClient.SCHEMA_SIGNALS_RAW;
import static io.isima.trino.bios.BiosClient.SCHEMA_SIGNALS_SKETCHES;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public enum BiosTableKind
{
    CONTEXT,
    CONTEXT_RAW,
    CONTEXT_SKETCHES,
    SIGNAL,
    SIGNAL_RAW,
    SIGNAL_SKETCHES;

    public static BiosTableKind getTableKind(String schemaName)
    {
        switch (schemaName) {
            case SCHEMA_CONTEXTS:
                return BiosTableKind.CONTEXT;
            case SCHEMA_CONTEXTS_RAW:
                return BiosTableKind.CONTEXT_RAW;
            case SCHEMA_CONTEXTS_SKETCHES:
                return BiosTableKind.CONTEXT_SKETCHES;
            case SCHEMA_SIGNALS:
                return BiosTableKind.SIGNAL;
            case SCHEMA_SIGNALS_RAW:
                return BiosTableKind.SIGNAL_RAW;
            case SCHEMA_SIGNALS_SKETCHES:
                return BiosTableKind.SIGNAL_SKETCHES;
            default:
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "bi(OS) was given invalid schema name: " + schemaName);
        }
    }
}
