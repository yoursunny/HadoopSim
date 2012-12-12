#include "netsim/netsim.h"
#include "netsim/nameserver.h"
#include "netsim/nameclient.h"
namespace HadoopNetSim {

NetSim::NetSim(void) {
  this->setup_status_ = kSSNone;
}

void NetSim::BuildTopology(const Topology& topo) {
  assert(this->setup_status_ == kSSNone);
  this->BuildNodes(topo.nodes());
  this->BuildLinks(topo.links());
  this->AssignIPs(topo.nodes());
  ns3::Config::SetDefault("ns3::TcpSocket::SegmentSize", ns3::UintegerValue(8960));
  this->setup_status_ = kSSBuildTopology;
}

void NetSim::BuildNodes(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes) {
  ns3::NodeContainer nodes; nodes.Create(topo_nodes.size());
  ns3::InternetStackHelper inetstack; inetstack.Install(nodes);
  std::unordered_map<HostName,ns3::Ptr<Node>>::const_iterator it1; ns3::NodeContainer::Iterator it2;
  for (it1 = topo_nodes.cbegin(), it2 = nodes.Begin(); it1 != topo_nodes.cend() && it2 != nodes.End(); ++it1,++it2) {
    this->nodes_[it1->first] = (*it2);
  }
}

void NetSim::BuildLinks(const std::unordered_map<LinkId,ns3::Ptr<Link>>& topo_links) {
  for (std::unordered_map<LinkId,ns3::Ptr<Link>>::const_iterator it = topo_links.cbegin(); it != topo_links.cend(); ++it) {
    ns3::Ptr<Link> link = it->second;
    ns3::Ptr<ns3::Node> node1 = this->nodes_[link->node1()];
    ns3::Ptr<ns3::Node> node2 = this->nodes_[link->node1()];
    ns3::NodeContainer nodes; nodes.Add(node1); nodes.Add(node2);
    ns3::PointToPointHelper ptp;
    switch (link->type()) {
      case kLTEth1G:
        ptp.SetDeviceAttribute("DataRate", ns3::StringValue("1Gbps"));
        break;
      case kLTEth10G:
        ptp.SetDeviceAttribute("DataRate", ns3::StringValue("10Gbps"));
        break;
    }
    ptp.SetDeviceAttribute("Mtu", ns3::UintegerValue(9000));
    ptp.SetChannelAttribute("Delay", ns3::TimeValue(ns3::NanoSeconds(6560)));
    ns3::NetDeviceContainer devices = ptp.Install(nodes);
    this->links_[link->id()] = devices.Get(0);
    this->links_[link->rid()] = devices.Get(1);
  }
}

void NetSim::AssignIPs(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes) {
  for (std::unordered_map<HostName,ns3::Ptr<Node>>::const_iterator it = topo_nodes.cbegin(); it != topo_nodes.cend(); ++it) {
    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
    if (it->second->ip() == ns3::Ipv4Address::GetAny()) continue;
    ns3::Ptr<ns3::Ipv4> ipv4 = node->GetObject<ns3::Ipv4>();
    assert(ipv4->GetNInterfaces() > 0);
    ipv4->AddAddress(0, ns3::Ipv4InterfaceAddress(it->second->ip(), ns3::Ipv4Mask(0xff000000)));
  }
  ns3::Ipv4GlobalRoutingHelper::PopulateRoutingTables();
}

ns3::Ipv4Address NetSim::GetHostIP(HostName host) {
  if (this->nodes_.count(host) == 0) return ns3::Ipv4Address::GetAny();
  return this->GetNodeIP(this->nodes_[host]);
}

ns3::Ipv4Address NetSim::GetNodeIP(ns3::Ptr<ns3::Node> node) {
  ns3::Ptr<ns3::Ipv4> ipv4 = node->GetObject<ns3::Ipv4>();
  for (uint32_t if_i = 0; if_i < ipv4->GetNInterfaces(); ++if_i) {
    for (uint32_t addr_i = 0; addr_i < ipv4->GetNAddresses(if_i); ++addr_i) {
      ns3::Ipv4Address addr = ipv4->GetAddress(if_i, addr_i).GetLocal();
      if (addr != ns3::Ipv4Address::GetLoopback()) return addr;
    }
  }
  return ns3::Ipv4Address::GetAny();
}

void NetSim::InstallApps(const std::unordered_set<HostName>& managers) {
  assert(this->setup_status_ == kSSBuildTopology);
  this->PopulateIPList(managers);
  
  ns3::ApplicationContainer s_apps, c_apps;
  for (std::unordered_map<HostName,ns3::Ipv4Address>::const_iterator it = this->managers_.cbegin(); it != this->managers_.cend(); ++it) {
    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
    ns3::Ptr<NameServer> ns_app = ns3::CreateObject<NameServer>();
    node->AddApplication(ns_app); s_apps.Add(ns_app);
  }
  for (std::unordered_map<HostName,ns3::Ipv4Address>::const_iterator it = this->slaves_.cbegin(); it != this->slaves_.cend(); ++it) {
    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
    ns3::Ptr<NameClient> nc_app = ns3::CreateObject<NameClient>(&this->managers_);
    node->AddApplication(nc_app); c_apps.Add(nc_app);
  }
  s_apps.Start(ns3::Seconds(0.0));
  c_apps.Start(ns3::Seconds(1.0));
  
  this->setup_status_ = kSSInstallApps;
}

void NetSim::PopulateIPList(const std::unordered_set<HostName>& managers) {
  for (std::unordered_map<HostName,ns3::Ptr<ns3::Node>>::const_iterator it = this->nodes_.cbegin(); it != this->nodes_.cend(); ++it) {
    if (managers.count(it->first) == 1) {
      this->managers_[it->first] = this->GetNodeIP(it->second);
    } else {
      this->slaves_[it->first] = this->GetNodeIP(it->second);
    }
  }
}

ns3::Time NetSim::GetReadyTime(void) {
  return ns3::Seconds(5.0);
}

MsgId NetSim::NameRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  if (this->slaves_.count(src) == 0 || this->managers_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTNameRequest, src, dst, size, cb, userobj);
  return msg->id();
}

MsgId NetSim::NameResponse(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  if (this->managers_.count(src) == 0 || this->slaves_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTNameResponse, src, dst, size, cb, userobj);
  return msg->id();
}

MsgId NetSim::DataRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  assert(false);//unimplemented
  if (this->slaves_.count(src) == 0 || this->managers_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTDataRequest, src, dst, size, cb, userobj);
  return msg->id();
}

MsgId NetSim::DataResponse(MsgId in_reply_to, HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  assert(false);//unimplemented
  if (this->managers_.count(src) == 0 || this->slaves_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTDataResponse, src, dst, size, cb, userobj);
  return msg->id();
}

template<typename apptype> ns3::Ptr<apptype> NetSim::GetNodeApp(ns3::Ptr<ns3::Node> node) {
  for (uint32_t app_i = 0; app_i < node->GetNApplications(); ++app_i) {
    ns3::Ptr<ns3::Application> app = node->GetApplication(app);
    if (apptype::GetTypeId() == app->GetInstanceTypeId()) {
      return ns3::Ptr<apptype>(static_cast<apptype*>(ns3::PeekPointer(app)));
    }
  }
  return NULL;
}

ns3::Ptr<MsgInfo> NetSim::MakeMsg(MsgType type, HostName src, HostName dst, size_t size, TransmitCb& cb, void* userobj) {
  ns3::Ptr<MsgInfo> msg = ns3::Create<MsgInfo>();
  msg->set_id(this->msgidgen_.Next());
  msg->set_type(type);
  msg->set_src(src);
  msg->set_dst(dst);
  msg->set_size(size);
  msg->set_cb(cb);
  msg->set_userobj(userobj);
  return msg;
}

ns3::Ptr<LinkStat> NetSim::GetLinkStat(LinkId link) {
  assert(false);//unimplemented
  return NULL;
}


};//namespace HadoopNetSim
