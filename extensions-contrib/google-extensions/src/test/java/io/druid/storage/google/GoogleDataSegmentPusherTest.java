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

package io.druid.storage.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.metamx.common.MapUtils;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class GoogleDataSegmentPusherTest extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String bucket = "bucket";
  private static final String prefix = "prefix";
  private static final String path = "prefix/test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";
  private static final DataSegment dataSegment = new DataSegment(
      "test",
      new Interval("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.<String, Object>of("bucket", bucket, "path", path),
      null,
      null,
      new NoneShardSpec(),
      0,
      1
  );

  private GoogleStorage storage;
  private GoogleAccountConfig googleAccountConfig;
  private ObjectMapper jsonMapper;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    googleAccountConfig = new GoogleAccountConfig();
    googleAccountConfig.setBucket(bucket);
    googleAccountConfig.setPrefix(prefix);

    jsonMapper = new DefaultObjectMapper();
  }

  @Test
  public void testPush() throws Exception
  {
    GoogleDataSegmentPusher pusher = new GoogleDataSegmentPusher(storage, googleAccountConfig, jsonMapper);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    DataSegment segmentToPush = new DataSegment(
        "foo",
        new Interval("2015/2016"),
        "0",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new NoneShardSpec(),
        0,
        size
    );

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
  }

  @Test
  public void uploadDataSegmentTest() throws IOException
  {
    final int version = 9;
    final File compressedSegmentData = new File("index.zip");
    final File descriptorFile = new File("descriptor.json");

    GoogleDataSegmentPusher pusher = createMockBuilder(
        GoogleDataSegmentPusher.class
    ).withConstructor(
        storage,
        googleAccountConfig,
         jsonMapper
    ).addMockedMethod("insert", File.class, String.class, String.class).createMock();

    final String storageDir = DataSegmentPusherUtil.getStorageDir(dataSegment);
    final String indexPath = prefix + "/" + storageDir + "/" + "index.zip";
    final String descriptorPath = prefix + "/" + storageDir + "/" + "descriptor.json";

    pusher.insert(compressedSegmentData, "application/zip", indexPath);
    expectLastCall();
    pusher.insert(descriptorFile, "application/json", descriptorPath);
    expectLastCall();

    replayAll();

    DataSegment pushedDataSegment = pusher.uploadDataSegment(
        dataSegment,
        version,
        compressedSegmentData,
        0,
        descriptorFile
    );

    assertEquals(compressedSegmentData.length(), pushedDataSegment.getSize());
    assertEquals(version, (int) pushedDataSegment.getBinaryVersion());
    Map<String, Object> loadSpec = pushedDataSegment.getLoadSpec();
    assertEquals(GoogleStorageDruidModule.SCHEME, MapUtils.getString(loadSpec, "type"));
    assertEquals(indexPath, MapUtils.getString(loadSpec, "path"));

    verifyAll();
  }
}
