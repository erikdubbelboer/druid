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

package io.druid.query.aggregation.post;

import com.google.common.collect.Lists;
import io.druid.query.aggregation.CountAggregator;
import io.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class LongLeastPostAggregatorTest
{
  @Test
  public void testCompute()
  {
    LongLeastPostAggregator greatestPostAggregator;
    CountAggregator agg = new CountAggregator("rows");
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 6
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    greatestPostAggregator = new LongLeastPostAggregator("least", postAggregatorList);
    Assert.assertEquals(Long.valueOf(3), greatestPostAggregator.compute(metricValues));
  }

  @Test
  public void testComparator()
  {
    LongLeastPostAggregator greatestPostAggregator;
    CountAggregator agg = new CountAggregator("rows");
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put(agg.getName(), agg.get());

    List<PostAggregator> postAggregatorList =
        Lists.newArrayList(
            new ConstantPostAggregator(
                "roku", 2
            ),
            new FieldAccessPostAggregator(
                "rows", "rows"
            )
        );

    greatestPostAggregator = new LongLeastPostAggregator("least", postAggregatorList);
    Comparator comp = greatestPostAggregator.getComparator();
    Object before = greatestPostAggregator.compute(metricValues);
    agg.aggregate();
    agg.aggregate();
    agg.aggregate();
    metricValues.put(agg.getName(), agg.get());
    Object after = greatestPostAggregator.compute(metricValues);

    Assert.assertEquals(-1, comp.compare(before, after));
    Assert.assertEquals(0, comp.compare(before, before));
    Assert.assertEquals(0, comp.compare(after, after));
    Assert.assertEquals(1, comp.compare(after, before));
  }
}
