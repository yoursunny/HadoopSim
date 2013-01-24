#include "netsim/nameclient.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

NameClient::NameClient(HostName localhost, std::unordered_map<HostName,ns3::Ipv4Address>* name_servers) {
  assert(name_servers != NULL);
  assert(!name_servers->empty());
  this->localhost_ = localhost;
  this->name_servers_ = name_servers;
}

ns3::TypeId NameClient::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::NameClient")
                           .SetParent<ns3::Application>();
  return tid;
}

void NameClient::StartApplication() {
  for (std::unordered_map<HostName,ns3::Ipv4Address>::const_iterator it = this->name_servers_->cbegin(); it != this->name_servers_->cend(); ++it) {
    ns3::Ptr<ns3::Socket> sock = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
    sock->Bind();
    sock->Connect(ns3::InetSocketAddress(it->second, kNameServerPort));
    ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, sock, false);
    this->mts_[it->first] = mt;
  }
}

bool NameClient::NameRequest(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTNameRequest);
  if (this->mts_.count(msg->dst()) == 0) return false;
  ns3::Ptr<MsgTransport> mt = this->mts_[msg->dst()];
  mt->Send(msg);
  return true;
}

};//namespace HadoopNetSim
