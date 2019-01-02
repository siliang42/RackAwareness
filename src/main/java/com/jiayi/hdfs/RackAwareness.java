package com.jiayi.hdfs;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: CONCENTRATE
 * @time: 2019/1/2.
 */
public class RackAwareness implements DNSToSwitchMapping {

    /**
     * --Topology Configuration--
     * 	<property>
     * 	  <name>net.topology.node.switch.mapping.impl</name>
     * 	  <value>com.jiayi.hdfs.RackAwareness</value>
     * 	</property>
     * @param names
     * @return
     */
    @Override
    public List<String> resolve(List<String> names) {
        //ips jiayi1 192.168.206.101
        List<String> racks = new ArrayList<>();
        //获取机架ip
        if(names != null && names.size()> 0){
            for(String name : names){
                int hostNum = 0;
                if(name.startsWith("jiayi")){
                    hostNum = Integer.parseInt(name.substring("jiayi".length())) + 100;
                }else if(name.startsWith("192")){
                    hostNum = Integer.parseInt(name.substring(name.lastIndexOf(".") + 1));
                }else {
                    throw new RuntimeException("name value error");
                }

                //自定义 机架感知 rack1 jiayi1 jiayi2
                //              rack2 jiayi3 jiayi4
                if(hostNum < 103){
                    racks.add("/rack1/" + hostNum);
                }else {
                    racks.add("/rack2/" +hostNum);
                }
            }
        }

        return racks;
    }

    @Override
    public void reloadCachedMappings() {

    }

    @Override
    public void reloadCachedMappings(List<String> names) {

    }
}
