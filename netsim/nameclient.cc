#include "netsim/nameserver.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

NameClient::NameClient(void) {
  this->running_ = false;
  this->sock_ = NULL;
}

static ns3::TypeId NameServer::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::NameServer")
                           .SetParent<ns3::Application>()
                           .AddConstructor<NameServer>();
}

void NameServer::StartApplication() {
  assert(!this->running_);
  this->running_ = true;

  this->sock_listen_ = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
  ns3::InetSocketAddress addr = ns3::InetSocketAddress(ns3::Ipv4Address::GetAny(), kNameServerPort);
  this->sock_listen_->Bind(addr);
  this->sock_listen_->Listen();
  this->sock_listen_->ShutdownSend();

  this->sock_listen_->SetAcceptCallback(ns3::MakeNullCallback<bool, ns3::Ptr<ns3::Socket>, const ns3::Address&>(),
                                        ns3::MakeCallback(&NameServer::HandleAccept, this));
}

void NameServer::StopApplication() {
  assert(this->running_);
  this->running_ = false;

  this->sock_listen_->Close();
  this->sock_listen_ = NULL;
}

void NameServer::HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from) {
  sock->SetRecvCallback(ns3::MakeCallback(&NameServer::HandleRead, this));
  this->sock_accepted_.push_back(sock);
}

void NameServer::HandleRead(ns3::Ptr<ns3::Socket> sock) {
  ns3::Ptr<ns3::Packet> pkt; ns3::Address from;
  while (NULL != (pkt = sock->RecvFrom(from))) {
    if (pkt->GetSize() == 0) break;
    printf("NameServer read %d\n", pkt->GetSize());
  }
}

};//namespace HadoopNetSim
