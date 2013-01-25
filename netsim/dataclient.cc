#include "netsim/dataclient.h"
#include "netsim/portnumber.h"
//#include <../src/internet/model/rtt-estimator.h>
//#include <../src/internet/model/tcp-newreno.h>
namespace HadoopNetSim {

DataClient::DataClient(HostName localhost, std::unordered_map<HostName,ns3::Ipv4Address>* data_servers) {
  assert(data_servers != NULL);
  assert(!data_servers->empty());
  this->localhost_ = localhost;
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
  ns3::Ptr<MsgTransport> mt = ns3::Create<MsgTransport>(this->localhost_, this->GetNode(), ns3::InetSocketAddress((*this->data_servers_)[msg->dst()], kDataServerPort));
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
