package org.murat;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

public class EntryPoint {

    void startCache() {
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

        // Create an IgniteCache and put some values in it.
        try {
            Connection con = new OracleDBConnection().connect();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select * from country_codes");
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("country_codes");
            while (rs.next()) {
                cache.put(rs.getInt(1), rs.getString(2));
                cache.put(rs.getInt(1), String.valueOf(rs.getInt(3)));
            }
            con.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(">> Created the cache and add the values.");

        // Disconnect from the cluster.
        ignite.close();
    }
}

