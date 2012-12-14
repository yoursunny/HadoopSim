#include "netsim/topology.h"
#include "gtest/gtest.h"
namespace HadoopNetSim {

TEST(NetSimTest, Topology) {
  Topology topology;
  char json[] = "{\"version\":1,\"type\":\"star\",\"nodes\":{\"host1\":{\"type\":\"host\",\"ip\":\"192.168.0.1\",\"devices\":[\"eth0\"]},\"host2\":{\"type\":\"host\",\"ip\":\"192.168.0.2\",\"devices\":[\"eth0\"]},\"sw1\":{\"type\":\"switch\",\"ip\":\"192.168.0.254\",\"devices\":[\"e0/0\",\"e0/1\"]}},\"links\":{\"1\":{\"node1\":\"sw1\",\"port1\":\"e0/0\",\"node2\":\"host1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"2\":{\"node1\":\"sw1\",\"port1\":\"e0/1\",\"node2\":\"host2\",\"port2\":\"eth0\",\"type\":\"eth1G\"}}}";
  topology.LoadString(json);

  ASSERT_EQ(3, topology.nodes().size());
  ASSERT_EQ(2, topology.links().size());
}

};//namespace HadoopNetSim
