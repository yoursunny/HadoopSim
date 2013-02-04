#ifndef HADOOPSIM_NETSIM_MSGTRANSPORT_H_
#define HADOOPSIM_NETSIM_MSGTRANSPORT_H_
#include "netsim/defs.h"
#include <queue>
#include <unordered_map>
#include <iostream>
#include "netsim/msginfo.h"
namespace HadoopNetSim {

class MsgTag : public ns3::Tag {//bytetag
  //contains a pointer to MsgInfo; another entity must keep ns3::Ptr<MsgInfo> so MsgInfo doesn't get deleted
  public:
    MsgTag(void);
    MsgTag(ns3::Ptr<MsgInfo> msg);
    MsgTag(const MsgTag& other) { this->operator=(other); }
    virtual ~MsgTag(void) {}
    MsgTag& operator=(const MsgTag& other);

    static ns3::TypeId GetTypeId(void);
    virtual ns3::TypeId GetInstanceTypeId(void) const { return MsgTag::GetTypeId(); }
    
    ns3::Ptr<MsgInfo> msg(void) const { return ns3::Ptr<MsgInfo>(this->msg_); }
    
    virtual uint32_t GetSerializedSize(void) const { return sizeof(MsgInfo*); }
    virtual void Serialize(ns3::TagBuffer buf) const;
    virtual void Deserialize(ns3::TagBuffer buf);
    virtual void Print(std::ostream& os) const {}
    
  private:
    MsgInfo* msg_;
};

class TransmitState : public ns3::SimpleRefCount<TransmitState> {//a sending or receiving message
  public:
    TransmitState(ns3::Ptr<MsgInfo> msg);
    virtual ~TransmitState(void) {}
    ns3::Ptr<MsgInfo> msg(void) const { return this->msg_; }
    size_t count(void) const { return this->count_; }
    void set_count(size_t value) { this->count_ = value; }
    void inc_count(size_t diff) { this->count_ += diff; }
    size_t pumped(void) const { return this->pumped_; }
    void set_pumped(size_t value) { this->pumped_ = value; }
    size_t remaining(void) const { return this->IsComplete() ? 0 : this->msg()->size() - this->count_; }
    size_t remaining_pumped(void) const { return this->pumped_ - this->count_; }
    bool IsComplete(void) const { return this->count_ >= this->msg()->size(); }
    const MsgTag& tag(void) const { return this->tag_; }

  private:
    ns3::Ptr<MsgInfo> msg_;
    size_t count_;//octets already sent or received
    size_t pumped_;//octets allowed to be sent
    MsgTag tag_;
    DISALLOW_COPY_AND_ASSIGN(TransmitState);
};

enum MsgTransportEvt {
  kMTENone,
  kMTEConnectError,
  kMTESendError,
  kMTEClose,
  kMTEReset
};

//pump messages into socket, and/or receive full messages from socket
class MsgTransport : public ns3::SimpleRefCount<MsgTransport> {
  public:
    MsgTransport(HostName localhost, ns3::Ptr<ns3::Socket> socket, bool connected = true);//use existing socket, optionally wait for Connect succeed
    MsgTransport(HostName localhost, ns3::Ptr<ns3::Node> local_node, const ns3::InetSocketAddress& remote_addr);//create new socket, retry if Connect fails
    virtual ~MsgTransport(void);
    ns3::Ptr<ns3::Socket> sock(void) const { return this->sock_; }
    uint16_t connect_attempts(void) const { return this->connect_attempts_; }
    void Send(ns3::Ptr<MsgInfo> msg);
    void SendPrepare(ns3::Ptr<MsgInfo> msg);//queue a message to send, but don't start sending
    void SendPump(ns3::Ptr<MsgInfo> msg, size_t max_progress);//send up to max_progress octets of a message; if a message is in front of queue but not fully pumped, subsequent messages have to wait
    MsgId PeekSendingMsg(void) const;//peek MsgId of send_queue.front
    void set_send_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> value) { this->send_cb_ = value; }//fires when a message is sent
    void set_progress_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>,size_t> value) { this->progress_cb_ = value; }//reports progress about a message being received
    void set_recv_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> value) { this->recv_cb_ = value; }//fires when a message is received
    void set_evt_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,MsgTransportEvt> value) { this->evt_cb_ = value; }//fires when any MsgTransportEvt happens
    
  private:
    HostName localhost_;
    ns3::Ptr<ns3::Socket> sock_;
    bool connect_retry_;
    uint16_t connect_attempts_;
    ns3::Ptr<ns3::Node> local_node_;
    ns3::Address remote_addr_;
    
    bool connected_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> send_cb_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>,size_t> progress_cb_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> recv_cb_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,MsgTransportEvt> evt_cb_;
    std::queue<ns3::Ptr<TransmitState>> send_queue_;
    std::unordered_map<MsgId,ns3::Ptr<TransmitState>> send_map_;
    bool send_block_;//true if send buffer is full
    std::unordered_map<MsgId,ns3::Ptr<TransmitState>> recv_map_;
    
    void Initialize(HostName localhost);

    void SendData(void);
    void SendMaybeUnblock(void);
    void SendUnblock(ns3::Ptr<ns3::Socket>, uint32_t);
    void RecvData(ns3::Ptr<ns3::Socket>);
    
    void invoke_send_cb(ns3::Ptr<MsgInfo> msg) { if (!this->send_cb_.IsNull()) this->send_cb_(this, msg); }
    void invoke_progress_cb(ns3::Ptr<MsgInfo> msg, size_t progress) { if (!this->progress_cb_.IsNull()) this->progress_cb_(this, msg, progress); }
    void invoke_recv_cb(ns3::Ptr<MsgInfo> msg) { if (!this->recv_cb_.IsNull()) this->recv_cb_(this, msg); }
    void invoke_evt_cb(MsgTransportEvt evt) { if (!this->evt_cb_.IsNull()) this->evt_cb_(this, evt); }
    
    MsgId PeekFirstMsgId(void) const;//returns first MsgId in send queue, or MsgId_invalid if send queue is empty
    void Connect(void);
    void SetSocketCallbacks(bool connected);
    void ClearSocketCallbacks(void);
    void SocketConnect(ns3::Ptr<ns3::Socket>);
    void SocketConnectFail(ns3::Ptr<ns3::Socket>);
    void SocketNormalClose(ns3::Ptr<ns3::Socket>);
    void SocketErrorClose(ns3::Ptr<ns3::Socket>);
    
    DISALLOW_COPY_AND_ASSIGN(MsgTransport);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_MSGTRANSPORT_H_
