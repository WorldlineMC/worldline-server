package org.bukkit.support.suite;

import io.papermc.paper.worldline.WorldlineControlServerTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;

@Suite
@SuiteDisplayName("Worldline server tests")
@SelectClasses(WorldlineControlServerTest.class)
public class WorldlineTestSuite {
}
