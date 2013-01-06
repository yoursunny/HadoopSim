#include "netsim/snmpagent.h"
#include "netsim/portnumber.h"
namespace HadoopNetSim {

SnmpAgent::SnmpAgent(std::unordered_map<HostName,ns3::Ipv4Address>* agents) {
  this->agents_ = agents;
  this->sock_ = NULL;
}

ns3::TypeId SnmpAgent::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::SnmpAgent")
                           .SetParent<ns3::Application>();
  return tid;
}

void SnmpAgent::StartApplication() {
  this->sock_ = ns3::Socket::CreateSocket(this->GetNode(), ns3::UdpSocketFactory::GetTypeId());
  ns3::InetSocketAddress addr = ns3::InetSocketAddress(ns3::Ipv4Address::GetAny(), kSnmpPort);
  this->sock_->Bind(addr);
  this->sock_->Listen();
  this->sock_->SetRecvCallback(ns3::MakeCallback(&SnmpAgent::HandleRecv, this));
}

bool SnmpAgent::Send(ns3::Ptr<MsgInfo> msg) {
  assert(msg->type() == kMTSnmp);
  if (this->agents_->count(msg->dst()) == 0) return false;
  msg->set_start(ns3::Simulator::Now());
  msg->Ref();//in-flight message reference; WARNING: memory leak on packet loss
  ns3::Ptr<ns3::Packet> pkt = ns3::Create<ns3::Packet>(msg->size());
  pkt->AddByteTag(MsgTag(msg));
  return -1 != this->sock_->SendTo(pkt, 0, ns3::InetSocketAddress((*this->agents_)[msg->dst()], kSnmpPort));
}

void SnmpAgent::HandleRecv(ns3::Ptr<ns3::Socket> sock) {
  ns3::Ptr<ns3::Packet> pkt;
  while (NULL != (pkt = this->sock_->Recv())) {
    ns3::ByteTagIterator it = pkt->GetByteTagIterator();
    while (it.HasNext()) {
      ns3::ByteTagIterator::Item item = it.Next();
      if (item.GetTypeId() != MsgTag::GetTypeId()) continue;
      MsgTag tag; item.GetTag(tag);
      ns3::Ptr<MsgInfo> msg = tag.msg();
      msg->set_success(true);
      msg->set_finish(ns3::Simulator::Now());
      msg->Unref();//in-flight message reference
      if (!msg->cb().IsNull()) msg->cb()(msg);
    }
  }
}

};//namespace HadoopNetSim
