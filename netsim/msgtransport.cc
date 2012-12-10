#include "netsim/msgtransport.h"
#include <algorithm>
namespace HadoopNetSim {

MsgTag::MsgTag(void) {
  this->msg_ = NULL;
}

MsgTag::MsgTag(ns3::Ptr<MsgInfo> msg) {
  this->msg_ = ns3::PeekPointer(msg);
}

MsgTag& MsgTag::operator=(const MsgTag& other) {
  this->msg_ = other.msg_;
}

ns3::TypeId MsgTag::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::MsgTag")
                           .SetParent<ns3::Tag>()
                           .AddConstructor<MsgTag>();
  return tid;
}

void MsgTag::Serialize(ns3::TagBuffer buf) const {
  buf.Write((const uint8_t*)&(this->msg_), sizeof(MsgInfo*));
}

void MsgTag::Deserialize(ns3::TagBuffer buf) {
  buf.Read((uint8_t*)&(this->msg_), sizeof(MsgInfo*));
}

TransmitState::TransmitState(ns3::Ptr<MsgInfo> msg) {
  this->msg_ = msg;
  this->count_ = 0;
  this->tag_ = MsgTag(msg);
}

MsgTransport::MsgTransport(ns3::Ptr<ns3::Socket> socket) {
  this->socket_ = socket;
  socket->SetRecvCallback(ns3::MakeCallback(&MsgTransport::RecvData, this));
  this->send_cb_ = TransmitCb_null;
  this->recv_cb_ = TransmitCb_null;
  this->send_block_ = false;
}

void MsgTransport::Send(ns3::Ptr<MsgInfo> msg) {
  msg->set_start(ns3::Simulator::Now());
  ns3::Ptr<TransmitState> ts = ns3::Create<TransmitState>(msg);
  this->send_queue_.push(ts);
  this->SendData();
}

void MsgTransport::SendData(void) {
  while (!this->send_block_ && !this->send_queue_.empty()) {
    ns3::Ptr<TransmitState> ts = this->send_queue_.front();
    uint32_t send_size = std::min((uint32_t)ts->remaining(), (uint32_t)this->socket_->GetTxAvailable());
    if (send_size == 0) {
      this->send_block_ = true;
      ns3::Simulator::Schedule(ns3::Seconds(0.005), &MsgTransport::SendMaybeUnblock, this);
      return;
    }
    ns3::Ptr<ns3::Packet> pkt = ns3::Create<ns3::Packet>(send_size);
    pkt->AddByteTag(ts->tag());
    int send_actual = this->socket_->Send(pkt);
    //printf("SendData size=%u actual=%d\n", send_size, send_actual);
    if (send_actual < 0) {
      this->send_block_ = true;
      break;
    } else {
      this->send_block_ = ((uint32_t)send_actual < send_size);
      ts->inc_count(send_actual);
    }
    if (ts->IsComplete()) {
      this->send_queue_.pop();
      this->send_cb_(ts->msg());
    }
  }
}

void MsgTransport::SendMaybeUnblock(void) {
  this->send_block_ = false;
  ns3::Simulator::ScheduleNow(&MsgTransport::SendData, this);
}

void MsgTransport::SendUnblock(ns3::Ptr<ns3::Socket>, uint32_t) {
  this->send_block_ = false;
  ns3::Simulator::ScheduleNow(&MsgTransport::SendData, this);
}

void MsgTransport::RecvData(ns3::Ptr<ns3::Socket>) {
  ns3::Ptr<ns3::Packet> pkt;
  while (NULL != (pkt = this->socket_->Recv())) {
    ns3::ByteTagIterator it = pkt->GetByteTagIterator();
    while (it.HasNext()) {
      ns3::ByteTagIterator::Item item = it.Next();
      if (item.GetTypeId() != MsgTag::GetTypeId()) continue;
      MsgTag tag; item.GetTag(tag);
      ns3::Ptr<MsgInfo> msg = tag.msg();
      uint32_t count = item.GetEnd() - item.GetStart();
      ns3::Ptr<TransmitState> ts = NULL;
      if (this->recv_map_.count(msg->id()) == 0) {
        ts = ns3::Create<TransmitState>(msg);
        this->recv_map_[msg->id()] = ts;
      } else {
        ts = this->recv_map_[msg->id()];
      }
      ts->inc_count(count);
      //printf("RecvData id=%u count=%u remain=%u\n", msg->id(), count, ts->remaining());
      if (ts->IsComplete()) {
        this->recv_map_.erase(msg->id());
        msg->set_finish(ns3::Simulator::Now());
        this->recv_cb_(msg);
      }
    }
  }
}


};//namespace HadoopNetSim
