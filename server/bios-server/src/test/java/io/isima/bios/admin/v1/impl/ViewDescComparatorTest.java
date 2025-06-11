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
package io.isima.bios.admin.v1.impl;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamComparators;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@AllArgsConstructor
public class ViewDescComparatorTest {
  private static ObjectMapper mapper;

  private static Consumer<ViewDesc> mod(Consumer<ViewDesc> modifier) {
    return modifier;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return List.of(
        new Object[][] {
          {mod((view) -> {}), mod((view) -> {}), true},
          {mod((view) -> view.setName(null)), mod((view) -> view.setName(null)), true},
          {null, mod((view) -> view.setName("otherName")), false},
          {null, mod((view) -> view.setName(null)), false},
          {null, mod((view) -> view.setName(null)), false},
          {mod((view) -> view.setGroupBy(null)), mod((view) -> view.setGroupBy(null)), true},
          {null, mod((view) -> view.setGroupBy(List.of("number", "int"))), false},
          {null, mod((view) -> view.setGroupBy(List.of("int", "number", "boolean"))), false},
          {null, mod((view) -> view.setGroupBy(List.of("INT", "NUMBER"))), true},
          {null, mod((view) -> view.setGroupBy(null)), false},
          {mod((view) -> view.setAttributes(null)), mod((view) -> view.setAttributes(null)), true},
          {null, mod((view) -> view.setAttributes(List.of("attr1", "attr3"))), false},
          {null, mod((view) -> view.setAttributes(List.of("attr1"))), false},
          {null, mod((view) -> view.setAttributes(List.of("ATTR1", "ATTR2"))), true},
          {null, mod((view) -> view.setAttributes(null)), false},
          {
            mod((view) -> view.setDataSketches(null)),
            mod((view) -> view.setDataSketches(null)),
            true
          },
          {null, mod((view) -> view.setDataSketches(null)), false},
          {null, mod((view) -> view.setDataSketches(List.of())), false},
          {
            null, mod((view) -> view.setDataSketches(List.of(DataSketchType.DISTINCT_COUNT))), false
          },
          {
            mod((view) -> view.setSchemaVersion(null)),
            mod((view) -> view.setSchemaVersion(null)),
            true
          },
          {null, mod((view) -> view.setSchemaVersion(999L)), true},
          {null, mod((view) -> view.setSchemaVersion(null)), true},
          {
            mod((view) -> view.setFeatureAsContextName(null)),
            mod((view) -> view.setFeatureAsContextName(null)),
            true
          },
          {null, mod((view) -> view.setFeatureAsContextName("otherName")), false},
          {null, mod((view) -> view.setFeatureAsContextName(null)), false},
          {null, mod((view) -> view.setFeatureAsContextName("REMOTECONTEXT")), false},
          {mod((view) -> view.setLastNItems(null)), mod((view) -> view.setLastNItems(null)), true},
          {null, mod((view) -> view.setLastNItems(null)), false},
          {null, mod((view) -> view.setLastNItems(10L)), false},
          {mod((view) -> view.setLastNTtl(null)), mod((view) -> view.setLastNTtl(null)), true},
          {null, mod((view) -> view.setLastNTtl(null)), false},
          {null, mod((view) -> view.setLastNTtl(10L)), false},
          {
            mod((view) -> view.setIndexTableEnabled(null)),
            mod((view) -> view.setIndexTableEnabled(null)),
            true
          },
          {null, mod((view) -> view.setIndexTableEnabled(null)), true},
          {null, mod((view) -> view.setIndexTableEnabled(true)), false},
          {
            mod((view) -> view.setIndexTableEnabled(true)),
            mod((view) -> view.setIndexTableEnabled(null)),
            false
          },
          {mod((view) -> view.setIndexType(null)), mod((view) -> view.setIndexType(null)), true},
          {null, mod((view) -> view.setIndexType(null)), false},
          {null, mod((view) -> view.setIndexType(IndexType.RANGE_QUERY)), false},
          {
            mod((view) -> view.setTimeIndexInterval(null)),
            mod((view) -> view.setTimeIndexInterval(null)),
            true
          },
          {null, mod((view) -> view.setTimeIndexInterval(null)), false},
          {null, mod((view) -> view.setTimeIndexInterval(900000L)), false},
          {mod((view) -> view.setSnapshot(null)), mod((view) -> view.setSnapshot(null)), true},
          {null, mod((view) -> view.setSnapshot(null)), false},
          {null, mod((view) -> view.setSnapshot(false)), false},
          {mod((view) -> view.setSnapshot(false)), mod((view) -> view.setSnapshot(null)), true},
          {
            mod((view) -> view.setWriteTimeIndexing(null)),
            mod((view) -> view.setWriteTimeIndexing(null)),
            true
          },
          {null, mod((view) -> view.setWriteTimeIndexing(null)), false},
          {null, mod((view) -> view.setWriteTimeIndexing(false)), false},
          {
            mod((view) -> view.setWriteTimeIndexing(false)),
            mod((view) -> view.setWriteTimeIndexing(null)),
            true
          },
        });
  }

  private Consumer<ViewDesc> modifierLeft;

  private Consumer<ViewDesc> modifierRight;

  private Boolean expectedResult;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  @Test
  public void test() throws IOException {
    final String src =
        "{"
            + "  'name': 'EmptyAttributeViewGroupIntNum',"
            + "  'groupBy': ['int','number'],"
            + "  'attributes': ['attr1', 'attr2'],"
            + "  'dataSketches': ['LastN'],"
            + "  'schemaVersion': 345,"
            + "  'featureAsContextName': 'remoteContext',"
            + "  'lastNItems': 15,"
            + "  'lastNTtl': 3600,"
            + "  'indexTableEnabled': false,"
            + "  'indexType': 'ExactMatch',"
            + "  'timeIndexInterval': 300000,"
            + "  'snapshot': true,"
            + "  'writeTimeIndexing': 'true'"
            + "}";

    final ViewDesc viewDesc = mapper.readValue(src.replaceAll("'", "\""), ViewDesc.class);

    // as an option, we test the duplicate method
    final ViewDesc viewDesc2 = viewDesc.duplicate();
    assertEquals(viewDesc2, viewDesc);

    if (modifierLeft != null) {
      modifierLeft.accept(viewDesc);
    }
    if (modifierRight != null) {
      modifierRight.accept(viewDesc2);
    }
    assertEquals(expectedResult, StreamComparators.equals(viewDesc, viewDesc2));
  }
}
