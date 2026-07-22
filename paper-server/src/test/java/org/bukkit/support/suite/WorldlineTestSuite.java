package org.bukkit.support.suite;

import io.papermc.paper.worldline.WorldlineControlServerTest;
import io.papermc.paper.worldline.WorldlineControlRequestExecutorTest;
import io.papermc.paper.worldline.WorldlineDestinationAttachmentTest;
import io.papermc.paper.worldline.WorldlineMainThreadOperationTest;
import io.papermc.paper.worldline.WorldlineResumeContextTest;
import io.papermc.paper.worldline.WorldlineTransferLifecycleTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

@Suite
@SuiteDisplayName("Worldline server tests")
@SelectClasses({WorldlineControlServerTest.class, WorldlineControlRequestExecutorTest.class,
    WorldlineDestinationAttachmentTest.class, WorldlineMainThreadOperationTest.class,
    WorldlineResumeContextTest.class, WorldlineTransferLifecycleTest.class})
public class WorldlineTestSuite {
}
