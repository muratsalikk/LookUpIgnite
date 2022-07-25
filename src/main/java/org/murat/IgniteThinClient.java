package org.murat;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IgniteThinClient {

    IgniteCache<Integer, String> cache ;
    IgniteThinClient() {
        // Preparing IgniteConfiguration using Java APIs
        IgniteConfiguration cfg = new IgniteConfiguration();

        // The node will be started as a client node.
        cfg.setClientMode(true);

        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);

        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("192.168.1.106:47500..47530"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
        // Starting the node
        Ignite ignite = Ignition.start(cfg);
        this.cache = ignite.cache("country_codes");
        System.out.println("constructor" + cache.get(49));
    }

    String getCountryCode(int i) {
        System.out.println("getcountry" + cache.get(i));
        return cache.get(i);
    }

    List<Integer> getCodeArray() {
        List<Integer> keys = new ArrayList<>();
        cache.query(new ScanQuery<Integer,String>(null)).forEach(entry -> keys.add(entry.getKey()));
        return keys;
    }


}
