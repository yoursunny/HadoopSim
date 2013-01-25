#include "netsim/netsim.h"
#include "netsim/nameserver.h"
#include "netsim/nameclient.h"
#include "netsim/dataserver.h"
#include "netsim/dataclient.h"
#include "netsim/snmpagent.h"
#include "netsim/importagent.h"
#include "netsim/linkstat.h"
namespace HadoopNetSim {

NetSim::NetSim(void) {
  this->setup_status_ = kSSNone;
  this->ready_cb_ = ns3::MakeNullCallback<void,NetSim*>();
}

void NetSim::BuildTopology(const Topology& topo) {
  assert(this->setup_status_ == kSSNone);
  this->BuildNodes(topo.nodes());
  this->BuildLinks(topo.links());
  //this->AssignIPs(topo.nodes());
  this->ConfigureRouting(topo.nodes());
  this->SetNetworkParameters();
  this->setup_status_ = kSSBuildTopology;
}

void NetSim::BuildNodes(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes) {
  ns3::NodeContainer nodes; nodes.Create(topo_nodes.size());
  ns3::InternetStackHelper inetstack; inetstack.Install(nodes);
  std::unordered_map<HostName,ns3::Ptr<Node>>::const_iterator it1; ns3::NodeContainer::Iterator it2;
  for (it1 = topo_nodes.cbegin(), it2 = nodes.Begin(); it1 != topo_nodes.cend() && it2 != nodes.End(); ++it1,++it2) {
    this->nodes_[it1->first] = (*it2);
    if (it1->second->type() == kNTSwitch) this->switches_.insert(it1->first);
  }
}

void NetSim::BuildLinks(const std::unordered_map<LinkId,ns3::Ptr<Link>>& topo_links) {
  this->ipv4addr_.SetBase(ns3::Ipv4Address("10.0.0.0"), ns3::Ipv4Mask("255.255.255.252"), ns3::Ipv4Address("0.0.0.1"));//Ipv4AddressHelper does not support /31 prefix length
  for (std::unordered_map<LinkId,ns3::Ptr<Link>>::const_iterator it = topo_links.cbegin(); it != topo_links.cend(); ++it) {
    ns3::Ptr<Link> link = it->second;
    ns3::Ptr<ns3::Node> node1 = this->nodes_[link->node1()];
    ns3::Ptr<ns3::Node> node2 = this->nodes_[link->node2()];
    ns3::NodeContainer nodes(node1, node2);

    ns3::PointToPointHelper ptp;
    //ns3::CsmaHelper csma;
    switch (link->type()) {
      case kLTEth1G:
        ptp.SetDeviceAttribute("DataRate", ns3::StringValue("1Gbps"));
        //csma.SetChannelAttribute("DataRate", ns3::StringValue("1Gbps"));
        break;
      case kLTEth10G:
        ptp.SetDeviceAttribute("DataRate", ns3::StringValue("10Gbps"));
        //csma.SetChannelAttribute("DataRate", ns3::StringValue("10Gbps"));
        break;
    }
    ptp.SetDeviceAttribute("Mtu", ns3::UintegerValue(9000));
    //csma.SetDeviceAttribute("Mtu", ns3::UintegerValue(9000));
    ptp.SetChannelAttribute("Delay", ns3::TimeValue(ns3::NanoSeconds(6560)));
    //csma.SetChannelAttribute("Delay", ns3::TimeValue(ns3::NanoSeconds(6560)));
    ns3::NetDeviceContainer devices = ptp.Install(nodes);
    //ns3::NetDeviceContainer devices = csma.Install(nodes);

    this->links_[link->id()] = devices.Get(0);
    this->links_[link->rid()] = devices.Get(1);
    devices.Get(0)->AggregateObject(ns3::CreateObject<LinkStatReader>(link->id(), devices.Get(0)));
    devices.Get(1)->AggregateObject(ns3::CreateObject<LinkStatReader>(link->rid(), devices.Get(1)));

    this->ipv4addr_.Assign(devices); this->ipv4addr_.NewNetwork();
  }
}

//void NetSim::AssignIPs(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes) {
//  for (std::unordered_map<HostName,ns3::Ptr<Node>>::const_iterator it = topo_nodes.cbegin(); it != topo_nodes.cend(); ++it) {
//    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
//    if (it->second->ip() == ns3::Ipv4Address::GetAny()) continue;
//    ns3::Ptr<ns3::Ipv4> ipv4 = node->GetObject<ns3::Ipv4>();
//    for (uint32_t dev_i = 0; dev_i < node->GetNDevices(); ++dev_i) {
//      ns3::Ptr<ns3::NetDevice> device = node->GetDevice(dev_i);
//      if (device->GetInstanceTypeId() == ns3::CsmaNetDevice::GetTypeId()) {
//        int32_t if_i1 = ipv4->GetInterfaceForDevice(device);
//        uint32_t if_i = if_i1 >= 0 ? (uint32_t)if_i1 : ipv4->AddInterface(device);
//        ipv4->AddAddress(if_i, ns3::Ipv4InterfaceAddress(it->second->ip(), ns3::Ipv4Mask("255.0.0.0")));
//        break;
//      }
//    }
//  }
//}

void NetSim::ConfigureRouting(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes) {
//  ns3::BridgeHelper bridge;
//  bridge.SetDeviceAttribute("Mtu", ns3::UintegerValue(9000));
//  for (std::unordered_map<HostName,ns3::Ptr<ns3::Node>>::iterator it = this->nodes_.begin(); it != this->nodes_.end(); ++it) {
//    if (topo_nodes.at(it->first)->type() != kNTSwitch) continue;
//    ns3::Ptr<ns3::Node> node = it->second;
//    ns3::NetDeviceContainer devices;
//    for (uint32_t dev_i = 0; dev_i < node->GetNDevices(); ++dev_i) {
//      ns3::Ptr<ns3::NetDevice> device = node->GetDevice(dev_i);
//      if (device->GetInstanceTypeId() == ns3::CsmaNetDevice::GetTypeId()) {
//        devices.Add(device);
//      }
//    }
//    bridge.Install(it->second, devices);
//  }
  ns3::Ipv4GlobalRoutingHelper::PopulateRoutingTables();
}

void NetSim::SetNetworkParameters(void) {
  ns3::Config::SetDefault("ns3::TcpSocket::SegmentSize", ns3::UintegerValue(8960));
  //ns3::Config::SetDefault("ns3::TcpSocket::ConnCount", ns3::UintegerValue(1024));
  //ns3::Config::SetDefault("ns3::DropTailQueue::MaxPackets", ns3::UintegerValue(100));
  //ns3::Config::SetDefault("ns3::RttEstimator::InitialEstimation", ns3::TimeValue(ns3::MilliSeconds(20)));
  //ns3::Config::SetDefault("ns3::RttEstimator::MinRTO", ns3::TimeValue(ns3::MilliSeconds(1)));
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
    ns3::Ptr<NameServer> ns_app = ns3::CreateObject<NameServer>(it->first);
    node->AddApplication(ns_app); s_apps.Add(ns_app);
  }
  for (std::unordered_map<HostName,ns3::Ipv4Address>::const_iterator it = this->slaves_.cbegin(); it != this->slaves_.cend(); ++it) {
    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
    ns3::Ptr<NameClient> nc_app = ns3::CreateObject<NameClient>(it->first, &this->managers_);
    node->AddApplication(nc_app); c_apps.Add(nc_app);
    ns3::Ptr<DataServer> ds_app = ns3::CreateObject<DataServer>(it->first);
    node->AddApplication(ds_app); s_apps.Add(ds_app);
    ns3::Ptr<DataClient> dc_app = ns3::CreateObject<DataClient>(it->first, &this->slaves_);
    node->AddApplication(dc_app); c_apps.Add(dc_app);
  }
  for (std::unordered_map<HostName,ns3::Ipv4Address>::const_iterator it = this->all_nodes_.cbegin(); it != this->all_nodes_.cend(); ++it) {
    ns3::Ptr<ns3::Node> node = this->nodes_[it->first];
    ns3::Ptr<SnmpAgent> snmp_app = ns3::CreateObject<SnmpAgent>(&this->all_nodes_);
    node->AddApplication(snmp_app); s_apps.Add(snmp_app);
    ns3::Ptr<ImportAgent> import_app = ns3::CreateObject<ImportAgent>(it->first, &this->all_nodes_);
    node->AddApplication(import_app); s_apps.Add(import_app);
  }
  s_apps.Start(ns3::Seconds(0.0));
  c_apps.Start(ns3::Seconds(1.0));
  ns3::Simulator::Schedule(ns3::Seconds(5.0), &NetSim::FireReadyCb, this);
  
  this->setup_status_ = kSSInstallApps;
}

void NetSim::PopulateIPList(const std::unordered_set<HostName>& managers) {
  for (std::unordered_map<HostName,ns3::Ptr<ns3::Node>>::const_iterator it = this->nodes_.cbegin(); it != this->nodes_.cend(); ++it) {
    HostName name = it->first;
    ns3::Ipv4Address ip = this->GetNodeIP(it->second);
    this->all_nodes_[name] = ip;
    if (this->switches_.count(name) == 0) {
      if (managers.count(it->first) == 1) {
        this->managers_[it->first] = this->GetNodeIP(it->second);
      } else {
        this->slaves_[it->first] = this->GetNodeIP(it->second);
      }
    }
  }
}

void NetSim::FireReadyCb(void) {
  assert(this->setup_status_ == kSSInstallApps);
  if (!this->ready_cb_.IsNull()) this->ready_cb_(this);
  this->setup_status_ = kSSReady;
}

MsgId NetSim::NameRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (src == dst || this->slaves_.count(src) == 0 || this->managers_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<NameClient> app = this->GetNodeApp<NameClient>(this->nodes_[src]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTNameRequest, src, dst, size, cb, userobj);
  if (!app->NameRequest(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::NameResponse(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (src == dst || this->managers_.count(src) == 0 || this->slaves_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<NameServer> app = this->GetNodeApp<NameServer>(this->nodes_[src]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTNameResponse, src, dst, size, cb, userobj);
  if (!app->NameResponse(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::DataRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (src == dst || this->slaves_.count(src) == 0 || this->slaves_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<DataClient> app = this->GetNodeApp<DataClient>(this->nodes_[src]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTDataRequest, src, dst, size, cb, userobj);
  if (!app->DataRequest(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::DataResponse(MsgId in_reply_to, HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (src == dst || this->slaves_.count(src) == 0 || this->slaves_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<DataServer> app = this->GetNodeApp<DataServer>(this->nodes_[src]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTDataResponse, src, dst, size, cb, userobj);
  msg->set_in_reply_to(in_reply_to);
  if (!app->DataResponse(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::Snmp(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (src == dst || this->all_nodes_.count(src) == 0 || this->all_nodes_.count(dst) == 0) return MsgId_invalid;
  ns3::Ptr<SnmpAgent> app = this->GetNodeApp<SnmpAgent>(this->nodes_[src]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTSnmp, src, dst, size, cb, userobj);
  if (!app->Send(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::ImportRequest(const std::vector<HostName>& pipeline, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (pipeline.size() < 2) return MsgId_invalid;
  std::unordered_set<HostName> hosts;
  for (std::vector<HostName>::const_iterator it = pipeline.cbegin(); it != pipeline.cend(); ++it) {
    if (this->all_nodes_.count(*it) == 0) return MsgId_invalid;
    std::pair<std::unordered_set<HostName>::iterator,bool> inserted = hosts.insert(*it);
    if (!inserted.second) return MsgId_invalid;//duplicate
  }

  ns3::Ptr<ImportAgent> app = this->GetNodeApp<ImportAgent>(this->nodes_[pipeline[0]]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTImportRequest, pipeline, size, cb, userobj);
  if (!app->ImportRequest(msg)) return MsgId_invalid;
  return msg->id();
}

MsgId NetSim::ImportResponse(MsgId in_reply_to, const std::vector<HostName>& pipeline, size_t size, TransmitCb cb, void* userobj) {
  this->AssertReady();
  if (pipeline.size() < 2 || this->all_nodes_.count(pipeline[0]) == 0) return MsgId_invalid;
  ns3::Ptr<ImportAgent> app = this->GetNodeApp<ImportAgent>(this->nodes_[pipeline[0]]);
  assert(app != NULL);
  ns3::Ptr<MsgInfo> msg = this->MakeMsg(kMTImportResponse, pipeline, size, cb, userobj);
  msg->set_in_reply_to(in_reply_to);
  if (!app->ImportResponse(msg)) return MsgId_invalid;
  return msg->id();
}

template<typename apptype> ns3::Ptr<apptype> NetSim::GetNodeApp(ns3::Ptr<ns3::Node> node) {
  assert(node != NULL);
  for (uint32_t app_i = 0; app_i < node->GetNApplications(); ++app_i) {
    ns3::Ptr<ns3::Application> app = node->GetApplication(app_i);
    if (apptype::GetTypeId() == app->GetInstanceTypeId()) {
      return ns3::Ptr<apptype>(static_cast<apptype*>(ns3::PeekPointer(app)));
    }
  }
  return NULL;
}

ns3::Ptr<MsgInfo> NetSim::MakeMsg(MsgType type, HostName src, HostName dst, size_t size, TransmitCb& cb, void* userobj) {
  ns3::Ptr<MsgInfo> msg = this->MakeMsgInternal(type, size, cb, userobj);
  msg->set_srcdst(src,dst);
  return msg;
}

ns3::Ptr<MsgInfo> NetSim::MakeMsg(MsgType type, const std::vector<HostName>& pipeline, size_t size, TransmitCb& cb, void* userobj) {
  ns3::Ptr<MsgInfo> msg = this->MakeMsgInternal(type, size, cb, userobj);
  msg->set_pipeline(pipeline);
  return msg;
}

ns3::Ptr<MsgInfo> NetSim::MakeMsgInternal(MsgType type, size_t size, TransmitCb& cb, void* userobj) {
  ns3::Ptr<MsgInfo> msg = ns3::Create<MsgInfo>();
  msg->set_id(this->msgidgen_.Next());
  msg->set_type(type);
  msg->set_size(size);
  msg->set_cb(cb);
  msg->set_userobj(userobj);
  return msg;
}

ns3::Ptr<LinkStat> NetSim::GetLinkStat(LinkId link) {
  ns3::Ptr<ns3::NetDevice> device = this->links_.at(link);
  return device->GetObject<LinkStatReader>()->Read();
}


};//namespace HadoopNetSim
