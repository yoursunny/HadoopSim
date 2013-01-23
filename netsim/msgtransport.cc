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
  this->pumped_ = 0;
  this->tag_ = MsgTag(msg);
}

MsgTransport::MsgTransport(ns3::Ptr<ns3::Socket> socket, bool connected) {
  assert(socket != NULL);
  this->sock_ = socket;
  this->connected_ = connected;
  socket->SetRecvCallback(ns3::MakeCallback(&MsgTransport::RecvData, this));
  if (!connected) {
    socket->SetConnectCallback(ns3::MakeCallback(&MsgTransport::SocketConnect, this),
                               ns3::MakeCallback(&MsgTransport::SocketConnectFail, this));
  }
  socket->SetCloseCallbacks(ns3::MakeCallback(&MsgTransport::SocketNormalClose, this),
                            ns3::MakeCallback(&MsgTransport::SocketErrorClose, this));
  this->send_cb_ = ns3::MakeNullCallback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>>();
  this->recv_cb_ = ns3::MakeNullCallback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>>();
  this->evt_cb_ = ns3::MakeNullCallback<void,ns3::Ptr<MsgTransport>,MsgTransportEvt>();
  this->send_block_ = false;
}

MsgTransport::~MsgTransport(void) {
  this->sock_->SetRecvCallback(ns3::MakeNullCallback<void,ns3::Ptr<ns3::Socket>>());
  this->sock_->SetConnectCallback(ns3::MakeNullCallback<void,ns3::Ptr<ns3::Socket>>(),
                                  ns3::MakeNullCallback<void,ns3::Ptr<ns3::Socket>>());
  this->sock_->SetCloseCallbacks(ns3::MakeNullCallback<void,ns3::Ptr<ns3::Socket>>(),
                                 ns3::MakeNullCallback<void,ns3::Ptr<ns3::Socket>>());
}

void MsgTransport::SendPrepare(ns3::Ptr<MsgInfo> msg) {
  assert(msg->size() > 0);
  msg->set_start(ns3::Simulator::Now());
  msg->Ref();//in-flight message reference
  ns3::Ptr<TransmitState> ts = ns3::Create<TransmitState>(msg);
  //printf("MsgTransport::Send id=%u size=%u\n", msg->id(), msg->size());
  this->send_queue_.push(ts);
  this->send_map_[msg->id()] = ts;
}

void MsgTransport::SendPump(ns3::Ptr<MsgInfo> msg, size_t max_progress) {
  assert(max_progress <= msg->size());
  ns3::Ptr<TransmitState> ts = this->send_map_.at(msg->id());
  assert(ts->pumped() <= max_progress);
  ts->set_pumped(max_progress);
  ns3::Simulator::ScheduleNow(&MsgTransport::SendData, this);
}

void MsgTransport::SendData(void) {
  while (this->connected_ && !this->send_block_ && !this->send_queue_.empty()) {
    ns3::Ptr<TransmitState> ts = this->send_queue_.front();
    uint32_t send_size = std::min((uint32_t)ts->remaining_pumped(), (uint32_t)this->sock_->GetTxAvailable());
    if (send_size == 0) {
      this->send_block_ = true;
      ns3::Simulator::Schedule(ns3::Seconds(0.005), &MsgTransport::SendMaybeUnblock, this);
      return;
    }
    ns3::Ptr<ns3::Packet> pkt = ns3::Create<ns3::Packet>(send_size);
    pkt->AddByteTag(ts->tag());
    int send_actual = this->sock_->Send(pkt);
    //printf("SendData size=%u actual=%d\n", send_size, send_actual);
    if (send_actual < 0) {
      this->send_block_ = true;
      if (!this->evt_cb_.IsNull()) this->evt_cb_(ns3::Ptr<MsgTransport>(this), kMTESendError);
      break;
    } else {
      this->send_block_ = ((uint32_t)send_actual < send_size);
      ts->inc_count(send_actual);
    }
    if (ts->IsComplete()) {
      //printf("MsgTransport::SendData complete\n");
      this->send_queue_.pop();
      this->send_map_.erase(ts->msg()->id());
      if (!this->send_cb_.IsNull()) this->send_cb_(ns3::Ptr<MsgTransport>(this), ts->msg());
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

void MsgTransport::RecvData(ns3::Ptr<ns3::Socket> s) {
  ns3::Ptr<ns3::Packet> pkt;
  while (NULL != (pkt = this->sock_->Recv())) {
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
      //printf("RecvData id=%u count=%u remain=%u now=%f\n", msg->id(), count, ts->remaining(), ns3::Simulator::Now().GetSeconds());
      if (!this->progress_cb_.IsNull()) this->progress_cb_(ns3::Ptr<MsgTransport>(this), msg, ts->count());
      if (ts->IsComplete()) {
        this->recv_map_.erase(msg->id());
        msg->set_success(true);
        msg->set_finish(ns3::Simulator::Now());
        msg->Unref();//in-flight message reference
        if (!this->recv_cb_.IsNull()) this->recv_cb_(ns3::Ptr<MsgTransport>(this), msg);
        if (!msg->cb().IsNull()) msg->cb()(msg);
      }
    }
  }
}

void MsgTransport::SocketConnect(ns3::Ptr<ns3::Socket>) {
  //printf("MsgTransport::SocketConnect %"PRIxMAX"\n", (uintmax_t)this);
  this->connected_ = true;
  this->SendData();
}

void MsgTransport::SocketConnectFail(ns3::Ptr<ns3::Socket>) {
  //printf("MsgTransport::SocketConnectFail %"PRIxMAX"\n", (uintmax_t)this);
  if (!this->evt_cb_.IsNull()) this->evt_cb_(ns3::Ptr<MsgTransport>(this), kMTEConnectError);
}

void MsgTransport::SocketNormalClose(ns3::Ptr<ns3::Socket>) {
  //printf("MsgTransport::SocketNormalClose %"PRIxMAX"\n", (uintmax_t)this);
  this->connected_ = false;
  if (!this->evt_cb_.IsNull()) this->evt_cb_(ns3::Ptr<MsgTransport>(this), kMTEClose);
}

void MsgTransport::SocketErrorClose(ns3::Ptr<ns3::Socket>) {
  //printf("MsgTransport::SocketErrorClose %"PRIxMAX"\n", (uintmax_t)this);
  this->connected_ = false;
  if (!this->evt_cb_.IsNull()) this->evt_cb_(ns3::Ptr<MsgTransport>(this), kMTEReset);
}



};//namespace HadoopNetSim
