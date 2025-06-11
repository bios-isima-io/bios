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
package io.isima.bios.admin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

import io.isima.bios.common.TestUtils;
import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.ColumnDefinitionsBuilder;
import io.isima.bios.models.proto.DataProto;
import java.util.Map;
import org.junit.Test;

public class AdminUtilsTest {

  @Test
  public void testPopulateColumnDefinitions() throws Exception {
    final var signalConfig = TestUtils.loadStreamJson(TestUtils.SIMPLE_ALL_TYPES_SIGNAL);
    final Map<String, ColumnDefinition> definitions =
        new ColumnDefinitionsBuilder().addAttributes(signalConfig.getAllBiosAttributes()).build();

    // verification
    final var attributes = signalConfig.getAttributes();
    assertThat(attributes.size(), is(12));
    assertThat(definitions.size(), is(12));
    final var iter = definitions.entrySet().iterator();
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    int index = 0;

    final var definition0 = iter.next();
    assertThat(definition0.getKey(), is("stringAttribute"));
    assertThat(definition0.getValue().getName(), is(definition0.getKey()));
    assertThat(definition0.getValue().getType(), is(DataProto.AttributeType.STRING));
    assertThat(definition0.getValue().getIndexInValueArray(), is(0));
    assertThat(definition0.getValue().getPosition(), is(index++));

    final var definition1 = iter.next();
    assertThat(definition1.getKey(), is("integerAttribute"));
    assertThat(definition1.getValue().getName(), is(definition1.getKey()));
    assertThat(definition1.getValue().getType(), is(DataProto.AttributeType.INTEGER));
    assertThat(definition1.getValue().getIndexInValueArray(), is(0));
    assertThat(definition1.getValue().getPosition(), is(index++));

    final var definition2 = iter.next();
    assertThat(definition2.getKey(), is("decimalAttribute"));
    assertThat(definition2.getValue().getName(), is(definition2.getKey()));
    assertThat(definition2.getValue().getType(), is(DataProto.AttributeType.DECIMAL));
    assertThat(definition2.getValue().getIndexInValueArray(), is(0));
    assertThat(definition2.getValue().getPosition(), is(index++));

    final var definition3 = iter.next();
    assertThat(definition3.getKey(), is("booleanAttribute"));
    assertThat(definition3.getValue().getName(), is(definition3.getKey()));
    assertThat(definition3.getValue().getType(), is(DataProto.AttributeType.BOOLEAN));
    assertThat(definition3.getValue().getIndexInValueArray(), is(0));
    assertThat(definition3.getValue().getPosition(), is(index++));

    final var definition4 = iter.next();
    assertThat(definition4.getKey(), is("blobAttribute"));
    assertThat(definition4.getValue().getName(), is(definition4.getKey()));
    assertThat(definition4.getValue().getType(), is(DataProto.AttributeType.BLOB));
    assertThat(definition4.getValue().getIndexInValueArray(), is(0));
    assertThat(definition4.getValue().getPosition(), is(index++));

    final var definition5 = iter.next();
    assertThat(definition5.getKey(), is("enumAttribute"));
    assertThat(definition5.getValue().getName(), is(definition5.getKey()));
    assertThat(definition5.getValue().getType(), is(DataProto.AttributeType.STRING));
    assertThat(definition5.getValue().getIndexInValueArray(), is(1));
    assertThat(definition5.getValue().getPosition(), is(index++));

    final var definition6 = iter.next();
    assertThat(definition6.getKey(), is("stringAttributeWithDefault"));
    assertThat(definition6.getValue().getName(), is(definition6.getKey()));
    assertThat(definition6.getValue().getType(), is(DataProto.AttributeType.STRING));
    assertThat(definition6.getValue().getIndexInValueArray(), is(2));
    assertThat(definition6.getValue().getPosition(), is(index++));

    final var definition7 = iter.next();
    assertThat(definition7.getKey(), is("integerAttributeWithDefault"));
    assertThat(definition7.getValue().getName(), is(definition7.getKey()));
    assertThat(definition7.getValue().getType(), is(DataProto.AttributeType.INTEGER));
    assertThat(definition7.getValue().getIndexInValueArray(), is(1));
    assertThat(definition7.getValue().getPosition(), is(index++));

    final var definition8 = iter.next();
    assertThat(definition8.getKey(), is("decimalAttributeWithDefault"));
    assertThat(definition8.getValue().getName(), is(definition8.getKey()));
    assertThat(definition8.getValue().getType(), is(DataProto.AttributeType.DECIMAL));
    assertThat(definition8.getValue().getIndexInValueArray(), is(1));
    assertThat(definition8.getValue().getPosition(), is(index++));

    final var definition9 = iter.next();
    assertThat(definition9.getKey(), is("booleanAttributeWithDefault"));
    assertThat(definition9.getValue().getName(), is(definition9.getKey()));
    assertThat(definition9.getValue().getType(), is(DataProto.AttributeType.BOOLEAN));
    assertThat(definition9.getValue().getIndexInValueArray(), is(1));
    assertThat(definition9.getValue().getPosition(), is(index++));

    final var definition10 = iter.next();
    assertThat(definition10.getKey(), is("blobAttributeWithDefault"));
    assertThat(definition10.getValue().getName(), is(definition10.getKey()));
    assertThat(definition10.getValue().getType(), is(DataProto.AttributeType.BLOB));
    assertThat(definition10.getValue().getIndexInValueArray(), is(1));
    assertThat(definition10.getValue().getPosition(), is(index++));

    final var definition11 = iter.next();
    assertThat(definition11.getKey(), is("enumAttributeWithDefault"));
    assertThat(definition11.getValue().getName(), is(definition11.getKey()));
    assertThat(definition11.getValue().getType(), is(DataProto.AttributeType.STRING));
    assertThat(definition11.getValue().getIndexInValueArray(), is(3));
    assertThat(definition11.getValue().getPosition(), is(index++));

    assertFalse(iter.hasNext());
  }
}
