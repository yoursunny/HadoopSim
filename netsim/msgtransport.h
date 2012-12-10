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
    size_t remaining(void) const { return this->IsComplete() ? 0 : this->msg()->size() - this->count_; }
    bool IsComplete(void) const { return this->count_ >= this->msg()->size(); }
    const MsgTag& tag(void) const { return this->tag_; }

  private:
    ns3::Ptr<MsgInfo> msg_;
    size_t count_;//octets already sent or received
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
    MsgTransport(ns3::Ptr<ns3::Socket> socket, bool connected = true);
    virtual ~MsgTransport(void) {}
    ns3::Ptr<ns3::Socket> socket(void) { return this->socket_; }
    HostName peer(void) const { return this->peer_; }//remote host of last sent/received message
    void Send(ns3::Ptr<MsgInfo> msg);//queue a message to send
    void set_send_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> value) { this->send_cb_ = value; }//fires when a message is sent
    void set_recv_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> value) { this->recv_cb_ = value; }//fires when a message is received
    void set_evt_cb(ns3::Callback<void,ns3::Ptr<MsgTransport>,MsgTransportEvt> value) { this->evt_cb_ = value; }//fires when any MsgTransportEvt happens
    
  private:
    ns3::Ptr<ns3::Socket> socket_;
    bool connected_;
    HostName peer_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> send_cb_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,ns3::Ptr<MsgInfo>> recv_cb_;
    ns3::Callback<void,ns3::Ptr<MsgTransport>,MsgTransportEvt> evt_cb_;
    std::queue<ns3::Ptr<TransmitState>> send_queue_;
    bool send_block_;//true if send buffer is full
    std::unordered_map<MsgId,ns3::Ptr<TransmitState>> recv_map_;
    
    void SendData(void);
    void SendMaybeUnblock(void);
    void SendUnblock(ns3::Ptr<ns3::Socket>, uint32_t);
    void RecvData(ns3::Ptr<ns3::Socket>);
    
    void SocketConnect(ns3::Ptr<ns3::Socket>);
    void SocketConnectFail(ns3::Ptr<ns3::Socket>);
    void SocketNormalClose(ns3::Ptr<ns3::Socket>);
    void SocketErrorClose(ns3::Ptr<ns3::Socket>);
    
    DISALLOW_COPY_AND_ASSIGN(MsgTransport);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_MSGTRANSPORT_H_
