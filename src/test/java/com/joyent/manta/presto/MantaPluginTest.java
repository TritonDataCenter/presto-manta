package com.joyent.manta.presto;

import com.facebook.presto.spi.connector.ConnectorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;

@Test
public class MantaPluginTest {
    public void canGetConnectorFactories() {
        final MantaPlugin plugin = new MantaPlugin();
        Assert.assertNotNull(plugin.getConnectorFactories());
    }

    public void confirmThereIsOnlyOneConnectorFactory() {
        final MantaPlugin plugin = new MantaPlugin();
        final Iterable<ConnectorFactory> factories = plugin.getConnectorFactories();
        final Iterator<ConnectorFactory> itr = factories.iterator();
        Assert.assertTrue(itr.hasNext(),
                "There was no factories available");
        final ConnectorFactory factory = itr.next();
        Assert.assertNotNull(factory);

        Assert.assertFalse(itr.hasNext(),
                "There should only be a single factory returned");
    }
}
