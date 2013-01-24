#include "netsim/nameserver.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

NameServer::NameServer(HostName localhost) {
  this->localhost_ = localhost;
  this->sock_ = NULL;
}

ns3::TypeId NameServer::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::NameServer")
                           .SetParent<ns3::Application>();
  return tid;
}

void NameServer::StartApplication() {
  this->sock_ = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
  ns3::InetSocketAddress addr = ns3::InetSocketAddress(ns3::Ipv4Address::GetAny(), kNameServerPort);
  this->sock_->Bind(addr);
  this->sock_->Listen();
  this->sock_->SetAcceptCallback(ns3::MakeNullCallback<bool,ns3::Ptr<ns3::Socket>,const ns3::Address&>(),
                                 ns3::MakeCallback(&NameServer::HandleAccept, this));
}

void NameServer::HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from) {
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, sock);
  mt->set_recv_cb(ns3::MakeCallback(&NameServer::HandleRecv, this));
  this->new_mts_.push_back(mt);
}

void NameServer::HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  ns3::Ptr<MsgTransport> old_mt = this->mts_[msg->src()];
  if (old_mt == mt) return;
  this->mts_[msg->src()] = mt;
  this->new_mts_.remove(mt);
}

bool NameServer::NameResponse(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTNameResponse);
  if (this->mts_.count(msg->dst()) == 0) return false;
  ns3::Ptr<MsgTransport> mt = this->mts_[msg->dst()];
  mt->Send(msg);
  return true;
}

};//namespace HadoopNetSim
