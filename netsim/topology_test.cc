#include "netsim/topology.h"
#include "gtest/gtest.h"
namespace HadoopNetSim {

TEST(NetSimTest, Topology) {
  Topology topology;
  char json[] = "{\"version\":1,\"type\":\"star\",\"nodes\":{\"host1\":{\"type\":\"host\",\"ip\":\"192.168.0.1\",\"devices\":[\"eth0\"]},\"host2\":{\"type\":\"host\",\"ip\":\"192.168.0.2\",\"devices\":[\"eth0\"]},\"sw1\":{\"type\":\"switch\",\"ip\":\"192.168.0.254\",\"devices\":[\"e0/0\",\"e0/1\"]}},\"links\":{\"1\":{\"node1\":\"sw1\",\"port1\":\"e0/0\",\"node2\":\"host1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"2\":{\"node1\":\"sw1\",\"port1\":\"e0/1\",\"node2\":\"host2\",\"port2\":\"eth0\",\"type\":\"eth1G\"}}}";
  topology.LoadString(json);

  ASSERT_EQ(3, topology.nodes().size());
  ASSERT_EQ(2, topology.links().size());
  ASSERT_EQ(kTTStar, topology.type());
  ASSERT_EQ(2, topology.graph().count("sw1"));
  ASSERT_EQ(1, topology.graph().count("host1"));
  std::pair<std::unordered_multimap<HostName,LinkId>::const_iterator,std::unordered_multimap<HostName,LinkId>::const_iterator> host1outlinks = topology.graph().equal_range("host1");
  bool has_minus1 = false;
  for (std::unordered_multimap<HostName,LinkId>::const_iterator it = host1outlinks.first; it != host1outlinks.second; ++it) {
    if (it->second == -1) has_minus1 = true;
  }
  ASSERT_TRUE(has_minus1);
}

TEST(NetSimTest, TopologyRackRowPathLength) {
  Topology topology;
  char json[] = "{\"version\":1,\"type\":\"rackrow\",\"nodes\":{\"coreA\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"coreB\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"row0A\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"row0B\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"row1A\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"row1B\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"rack0\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"rack1\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"rack2\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"manager\":{\"type\":\"host\",\"ip\":\"10.0.0.1\",\"devices\":[\"eth0\"]},\"gn04\":{\"type\":\"host\",\"ip\":\"10.0.0.2\",\"devices\":[\"eth0\"]},\"Pico\":{\"type\":\"host\",\"ip\":\"10.0.0.3\",\"devices\":[\"eth0\"]},\"gn05\":{\"type\":\"host\",\"ip\":\"10.0.0.4\",\"devices\":[\"eth0\"]},\"Silo\":{\"type\":\"host\",\"ip\":\"10.0.0.5\",\"devices\":[\"eth0\"]}},\"links\":{\"1\":{\"node1\":\"coreA\",\"port1\":\"eth0\",\"node2\":\"row0A\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"2\":{\"node1\":\"coreB\",\"port1\":\"eth0\",\"node2\":\"row0A\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"3\":{\"node1\":\"coreA\",\"port1\":\"eth1\",\"node2\":\"row0B\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"4\":{\"node1\":\"coreB\",\"port1\":\"eth1\",\"node2\":\"row0B\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"5\":{\"node1\":\"coreA\",\"port1\":\"eth2\",\"node2\":\"row1A\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"6\":{\"node1\":\"coreB\",\"port1\":\"eth2\",\"node2\":\"row1A\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"7\":{\"node1\":\"coreA\",\"port1\":\"eth3\",\"node2\":\"row1B\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"8\":{\"node1\":\"coreB\",\"port1\":\"eth3\",\"node2\":\"row1B\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"9\":{\"node1\":\"row0A\",\"port1\":\"eth2\",\"node2\":\"rack0\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"10\":{\"node1\":\"row0B\",\"port1\":\"eth2\",\"node2\":\"rack0\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"11\":{\"node1\":\"row0A\",\"port1\":\"eth3\",\"node2\":\"rack1\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"12\":{\"node1\":\"row0B\",\"port1\":\"eth3\",\"node2\":\"rack1\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"13\":{\"node1\":\"row1A\",\"port1\":\"eth2\",\"node2\":\"rack2\",\"port2\":\"eth0\",\"type\":\"eth10G\"},\"14\":{\"node1\":\"row1B\",\"port1\":\"eth2\",\"node2\":\"rack2\",\"port2\":\"eth1\",\"type\":\"eth10G\"},\"15\":{\"node1\":\"rack0\",\"port1\":\"eth2\",\"node2\":\"manager\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"16\":{\"node1\":\"rack0\",\"port1\":\"eth3\",\"node2\":\"gn04\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"17\":{\"node1\":\"rack1\",\"port1\":\"eth2\",\"node2\":\"Pico\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"18\":{\"node1\":\"rack1\",\"port1\":\"eth3\",\"node2\":\"gn05\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"19\":{\"node1\":\"rack2\",\"port1\":\"eth2\",\"node2\":\"Silo\",\"port2\":\"eth0\",\"type\":\"eth1G\"}}}";
  topology.LoadString(json);

  ASSERT_EQ(0, topology.PathLength("coreA", "coreA"));
  ASSERT_EQ(0, topology.PathLength("Pico", "Pico"));
  ASSERT_EQ(1, topology.PathLength("rack1", "Pico"));
  ASSERT_EQ(2, topology.PathLength("Pico", "gn05"));
  ASSERT_EQ(2, topology.PathLength("row0A", "Pico"));
  ASSERT_EQ(2, topology.PathLength("rack0", "rack1"));
  ASSERT_EQ(2, topology.PathLength("row0A", "row1A"));
  ASSERT_EQ(2, topology.PathLength("row0A", "row0B"));
  ASSERT_EQ(3, topology.PathLength("coreA", "Pico"));
  ASSERT_EQ(3, topology.PathLength("rack0", "Pico"));
  ASSERT_EQ(4, topology.PathLength("gn04", "Pico"));
  ASSERT_EQ(4, topology.PathLength("row1B", "Pico"));
  ASSERT_EQ(5, topology.PathLength("rack2", "Pico"));
  ASSERT_EQ(6, topology.PathLength("Silo", "Pico"));
  ASSERT_EQ("rack0", topology.nodes().at("manager")->rack());
  ASSERT_EQ("rack0", topology.nodes().at("gn04")->rack());
  ASSERT_EQ("rack1", topology.nodes().at("Pico")->rack());
  ASSERT_EQ("rack1", topology.nodes().at("gn05")->rack());
  ASSERT_EQ("rack2", topology.nodes().at("Silo")->rack());
}

};//namespace HadoopNetSim
