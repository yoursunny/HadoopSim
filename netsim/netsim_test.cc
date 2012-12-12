#include "netsim/netsim.h"
#include "gtest/gtest.h"
namespace HadoopNetSim {

/*
TOPOLOGY
manager0 \              / slave0
           sw1 ---- sw2 - manager1
  slave1 /              \ slave2

*/

TEST(NetSimTest, NetSim) {
  EXPECT_EXIT({
    Topology topology;
    char topo_json[] = "{\"version\":1,\"nodes\":{\"sw1\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"sw2\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"manager0\":{\"type\":\"host\",\"ip\":\"10.0.0.1\",\"devices\":[\"eth0\"]},\"manager1\":{\"type\":\"host\",\"ip\":\"10.0.0.2\",\"devices\":[\"eth0\"]},\"slave0\":{\"type\":\"host\",\"ip\":\"10.0.1.1\",\"devices\":[\"eth0\"]},\"slave1\":{\"type\":\"host\",\"ip\":\"10.0.1.2\",\"devices\":[\"eth0\"]},\"slave2\":{\"type\":\"host\",\"ip\":\"10.0.1.3\",\"devices\":[\"eth0\"]}},\"links\":{\"1\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"manager0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"2\":{\"node1\":\"sw2\",\"port1\":\"eth1\",\"node2\":\"manager1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"3\":{\"node1\":\"sw2\",\"port1\":\"eth0\",\"node2\":\"slave0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"4\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"slave1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"5\":{\"node1\":\"sw2\",\"port1\":\"eth2\",\"node2\":\"slave2\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"6\":{\"node1\":\"sw1\",\"port1\":\"eth2\",\"node2\":\"sw2\",\"port2\":\"eth3\",\"type\":\"eth10G\"}}}";
    topology.LoadString(topo_json);

    NetSim netsim;
    netsim.BuildTopology(topology);
    if (netsim.GetHostIP("manager1") != ns3::Ipv4Address("10.0.0.2")) ::exit(1);
    std::unordered_set<HostName> managers; managers.insert("manager0"); managers.insert("manager1");
    netsim.InstallApps(managers);
    printf("send %lu\n",netsim.NameRequest("slave0","manager0",100000,TransmitCb_null,NULL));
    ns3::Simulator::Run();
    printf("time %f\n",ns3::Simulator::Now().GetSeconds());
    ::exit(0);
  }, ::testing::ExitedWithCode(0), "");
}

};//namespace HadoopNetSim