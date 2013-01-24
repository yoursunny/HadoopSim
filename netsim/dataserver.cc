#include "netsim/dataserver.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

DataServer::DataServer(HostName localhost) {
  this->localhost_ = localhost;
  this->sock_ = NULL;
}

ns3::TypeId DataServer::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::DataServer")
                           .SetParent<ns3::Application>();
  return tid;
}

void DataServer::StartApplication() {
  this->sock_ = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
  ns3::InetSocketAddress addr = ns3::InetSocketAddress(ns3::Ipv4Address::GetAny(), kDataServerPort);
  this->sock_->Bind(addr);
  this->sock_->Listen();
  this->sock_->SetAcceptCallback(ns3::MakeNullCallback<bool,ns3::Ptr<ns3::Socket>,const ns3::Address&>(),
                                 ns3::MakeCallback(&DataServer::HandleAccept, this));
}

void DataServer::HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from) {
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, sock);
  //printf("DataServer::HandleAccept %"PRIxMAX"\n", (uintmax_t)ns3::PeekPointer(mt));
  mt->set_recv_cb(ns3::MakeCallback(&DataServer::HandleRecv, this));
  this->new_mts_.push_back(mt);
}

void DataServer::HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  //printf("DataServer::HandleRecv %"PRIxMAX"\n", (uintmax_t)ns3::PeekPointer(mt));
  this->mts_[msg->id()] = mt;
  this->new_mts_.remove(mt);
}

bool DataServer::DataResponse(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTDataResponse);
  if (this->mts_.count(msg->in_reply_to()) == 0) return false;
  ns3::Ptr<MsgTransport> mt = this->mts_[msg->in_reply_to()];
  mt->set_send_cb(ns3::MakeCallback(&DataServer::HandleSend, this));
  mt->Send(msg);
  return true;
}

void DataServer::HandleSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  mt->sock()->Close();
  this->mts_.erase(msg->in_reply_to());
}

};//namespace HadoopNetSim
