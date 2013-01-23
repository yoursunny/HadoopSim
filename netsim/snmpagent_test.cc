#include "netsim/snmpagent.h"
#include "gtest/gtest.h"
namespace HadoopNetSim {

ns3::Ptr<MsgInfo> SnmpAgentTest_received = NULL;

void SnmpAgentTest_Recv(ns3::Ptr<MsgInfo> msg) {
  SnmpAgentTest_received = msg;
}

void SnmpAgentTest_Send(ns3::Ptr<SnmpAgent> source) {
  ns3::Ptr<MsgInfo> msg = ns3::Create<MsgInfo>();
  msg->set_id(72); msg->set_type(kMTSnmp); msg->set_size(2013); msg->set_srcdst("source","sink");
  msg->set_cb(ns3::MakeCallback(&SnmpAgentTest_Recv));
  assert(source->Send(msg));
}

void SnmpAgentTest_Body() {
  ns3::NodeContainer nodes; nodes.Create(2);
  ns3::PointToPointHelper ptp;
  ptp.SetDeviceAttribute("DataRate", ns3::StringValue("1Gbps"));
  ptp.SetDeviceAttribute("Mtu", ns3::UintegerValue(9000));
  ptp.SetChannelAttribute("Delay", ns3::TimeValue(ns3::NanoSeconds(6560)));
  ns3::NetDeviceContainer devices = ptp.Install(nodes);
  ns3::InternetStackHelper inetstack; inetstack.Install(nodes);
  ns3::Ipv4AddressHelper ip4addr; ip4addr.SetBase("192.168.72.0", "255.255.255.0");
  ns3::Ipv4InterfaceContainer ifs = ip4addr.Assign(devices);
  
  std::unordered_map<HostName,ns3::Ipv4Address> agents;
  agents["sink"] = ifs.GetAddress(0);
  agents["source"] = ifs.GetAddress(1);
  
  ns3::Ptr<SnmpAgent> sink = ns3::CreateObject<SnmpAgent>(&agents);
  nodes.Get(0)->AddApplication(sink);
  sink->SetStartTime(ns3::Seconds(0));
  ns3::Ptr<SnmpAgent> source = ns3::CreateObject<SnmpAgent>(&agents);
  nodes.Get(1)->AddApplication(source);
  source->SetStartTime(ns3::Seconds(1));
  
  ns3::Simulator::Schedule(ns3::Seconds(2), &SnmpAgentTest_Send, source);
  
  ns3::Simulator::Run();
  ns3::Simulator::Destroy();

  assert(SnmpAgentTest_received != NULL);
  assert(SnmpAgentTest_received->id() == 72);
  assert(SnmpAgentTest_received->success());
  ::exit(0);
}

TEST(NetSimTest, SnmpAgent) {
  EXPECT_EXIT({
    SnmpAgentTest_Body();
  }, ::testing::ExitedWithCode(0), "");
}

};//namespace HadoopNetSim
