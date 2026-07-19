package org.bukkit.support.suite;

import io.papermc.paper.worldline.WorldlineControlServerTest;
import io.papermc.paper.worldline.WorldlineDestinationAttachmentTest;
import io.papermc.paper.worldline.WorldlineResumeContextTest;
import io.papermc.paper.worldline.WorldlineTransferLifecycleTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

@Suite
@SuiteDisplayName("Worldline server tests")
@SelectClasses({WorldlineControlServerTest.class, WorldlineDestinationAttachmentTest.class,
    WorldlineResumeContextTest.class, WorldlineTransferLifecycleTest.class})
public class WorldlineTestSuite {
}
