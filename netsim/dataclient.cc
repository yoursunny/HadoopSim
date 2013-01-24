#include "netsim/dataclient.h"
#include "netsim/portnumber.h"
//#include <../src/internet/model/rtt-estimator.h>
//#include <../src/internet/model/tcp-newreno.h>
namespace HadoopNetSim {

DataClient::DataClient(std::unordered_map<HostName,ns3::Ipv4Address>* data_servers) {
  assert(data_servers != NULL);
  assert(!data_servers->empty());
  this->data_servers_ = data_servers;
}

ns3::TypeId DataClient::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::DataClient")
                           .SetParent<ns3::Application>();
  return tid;
}

bool DataClient::DataRequest(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTDataRequest);
  if (this->data_servers_->count(msg->dst()) == 0) return false;
  //ns3::Ptr<ns3::RttEstimator> rtt = ns3::CreateObject<ns3::RttMeanDeviation>();
  //rtt->SetAttribute("MaxMultiplier", ns3::UintegerValue(1));
  ns3::Ptr<ns3::Socket> sock = ns3::Socket::CreateSocket(this->GetNode(), ns3::TcpSocketFactory::GetTypeId());
  //sock->GetObject<ns3::TcpNewReno>()->SetRtt(rtt);
  sock->Bind();
  sock->Connect(ns3::InetSocketAddress((*this->data_servers_)[msg->dst()], kDataServerPort));
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(sock, false);
  //printf("DataClient::DataRequest %"PRIxMAX"\n", (uintmax_t)ns3::PeekPointer(mt));
  mt->set_send_cb(ns3::MakeCallback(&DataClient::HandleSend, this));
  mt->set_recv_cb(ns3::MakeCallback(&DataClient::HandleRecv, this));
  mt->Send(msg);
  this->mts_[msg->id()] = mt;
  return true;
}

void DataClient::HandleSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  //printf("DataClient::HandleSend %"PRIxMAX"\n", (uintmax_t)ns3::PeekPointer(mt));
  //mt->sock()->ShutdownSend();
  //cannot ShutdownSend here: packets won't send out
}

void DataClient::HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg) {
  mt->sock()->ShutdownSend();
  this->mts_.erase(msg->in_reply_to());
}

};//namespace HadoopNetSim
