/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.Pair;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.segment.IndexBuilder;
import io.druid.segment.StorageAdapter;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class DimensionsFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", new DateTime("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("1", "2"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("3"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "4", "dim1", "1", "dim2", ImmutableList.of("4", "5"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "5", "dim1", "5", "dim2", ImmutableList.of("4", "5"))),
      PARSER.parse(ImmutableMap.<String, Object>of("dim0", "6", "dim1", "1")),
			PARSER.parse(ImmutableMap.<String, Object>of("dim0", "7", "dim1", "a"))
  );

  public DimensionsFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(DimensionsFilterTest.class.getName());
  }

  @Test
  public void testSingleValueStringColumnWithoutNulls()
  {
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim0", "dim1" }), ImmutableList.<String>of("2","5"));
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim0", "dim2" }), ImmutableList.<String>of("3","4","5"));
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim1", "dim2" }), ImmutableList.<String>of("5"));
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim0", "dim1", "dim2" }), ImmutableList.<String>of("5"));
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim6", "dim7" }), ImmutableList.<String>of("0","1","2","3","4","5","6", "7"));
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim1", "dim6" }), ImmutableList.<String>of("0"));
    assertFilterMatches(new SelectorDimFilter(null, null, null, new String[]{ "dim2", "dim6" }), ImmutableList.<String>of("1","2","6","7"));
  }

  @Test
  public void testSelectorWithLookupExtractionFn()
  {
    final Map<String, String> stringMap = ImmutableMap.of(
        "a", "7"
    );
    LookupExtractor mapExtractor = new MapLookupExtractor(stringMap, false);
    LookupExtractionFn lookupFn = new LookupExtractionFn(mapExtractor, true, null, false, true);

    assertFilterMatches(new SelectorDimFilter(null, null, lookupFn, new String[]{ "dim0", "dim1" }), ImmutableList.<String>of("2","5","7"));
  }
}
