#include "netsim/importagent.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

ImportAgent::ImportAgent(HostName localhost, std::unordered_map<HostName,ns3::Ipv4Address>* agents) {
  assert(agents != NULL);
  assert(!agents->empty());
  this->localhost_ = localhost;
  this->agents_ = agents;
  this->sock_ = NULL;
}

ns3::TypeId ImportAgent::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::ImportAgent")
                           .SetParent<ns3::Application>();
  return tid;
}

void ImportAgent::StartApplication() {
  this->sock_ = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
  ns3::InetSocketAddress addr = ns3::InetSocketAddress(ns3::Ipv4Address::GetAny(), kImportPort);
  this->sock_->Bind(addr);
  this->sock_->Listen();
  this->sock_->SetAcceptCallback(ns3::MakeNullCallback<bool,ns3::Ptr<ns3::Socket>,const ns3::Address&>(),
                                 ns3::MakeCallback(&ImportAgent::SockAccept, this));
}

bool ImportAgent::ImportRequest(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTImportRequest);
  assert(msg->src() == this->localhost_);
  
  ns3::Ptr<MsgTransport> mt = this->OpenConnection(msg->pipeline()[1]);
  mt->set_recv_cb(ns3::MakeCallback(&ImportAgent::SourceResponseRecv, this));
  mt->Send(msg);
  this->sources_[msg->id()] = mt;
  return true;
}

ns3::Ptr<MsgTransport> ImportAgent::OpenConnection(HostName peer) {
  ns3::Ipv4Address addr = this->agents_->at(peer);
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, this->GetNode(), ns3::InetSocketAddress(addr, kImportPort));
  return mt;
}

void ImportAgent::SourceResponseRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  mt->sock()->ShutdownSend();
  this->sources_.erase(msg->in_reply_to());
}

void ImportAgent::SockAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from) {
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, sock);
  mt->set_progress_cb(ns3::MakeCallback(&ImportAgent::NewMTRequestProgress, this));
  this->new_mts_.push_back(mt);
}

void ImportAgent::NewMTRequestProgress(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg, size_t progress) {
  size_t old_size = this->new_mts_.size();
  this->new_mts_.remove(mt);
  if (old_size == this->new_mts_.size()) return;//already processed
  assert(msg->type() == kMTImportRequest);
  mt->set_recv_cb(ns3::MakeNullCallback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>>());
  
  if (this->localhost_ == msg->dst()) {
    this->sinks_[msg->id()] = mt;
  } else {
    mt->set_progress_cb(ns3::MakeCallback(&ImportAgent::IntermediateRequestProgress, this));
    mt->set_send_cb(ns3::MakeCallback(&ImportAgent::IntermediateResponseSend, this));

    HostName next_host = msg->FindNextHost(this->localhost_);
    assert(next_host != HostName_invalid);

    ns3::Ptr<MsgTransport> mt2 = this->OpenConnection(next_host);
    mt2->set_progress_cb(ns3::MakeCallback(&ImportAgent::IntermediateResponseProgress, this));
    mt2->SendPrepare(msg); mt2->SendPump(msg, progress);
    this->intermediates_[msg->id()] = std::make_pair(mt, mt2);
  }
}

void ImportAgent::IntermediateRequestProgress(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg, size_t progress) {
  ns3::Ptr<MsgTransport> mt2 = this->intermediates_.at(msg->id()).second;
  mt2->SendPump(msg, progress);
}

void ImportAgent::IntermediateResponseProgress(ns3::Ptr<MsgTransport> mt2, ns3::Ptr<MsgInfo> msg, size_t progress) {
  ns3::Ptr<MsgTransport> mt = this->intermediates_.at(msg->in_reply_to()).first;
  if (mt->PeekSendingMsg() != msg->id()) mt->SendPrepare(msg);
  mt->SendPump(msg, progress);
}

void ImportAgent::IntermediateResponseSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  mt->sock()->Close();
  this->intermediates_.erase(msg->in_reply_to());
}

bool ImportAgent::ImportResponse(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTImportResponse);
  assert(msg->src() == this->localhost_);

  if (this->sinks_.count(msg->in_reply_to()) == 0) return false;
  ns3::Ptr<MsgTransport> mt = this->sinks_[msg->in_reply_to()];
  mt->set_send_cb(ns3::MakeCallback(&ImportAgent::SinkResponseSend, this));
  mt->Send(msg);
  return true;
}
    
void ImportAgent::SinkResponseSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  mt->sock()->Close();
  this->sinks_.erase(msg->in_reply_to());
}

};//namespace HadoopNetSim
