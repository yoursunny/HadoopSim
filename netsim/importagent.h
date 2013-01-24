#ifndef HADOOPSIM_NETSIM_IMPORTAGENT_H_
#define HADOOPSIM_NETSIM_IMPORTAGENT_H_
#include "netsim/defs.h"
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class ImportAgent : public ns3::Application {
  public:
    ImportAgent(HostName localhost, std::unordered_map<HostName,ns3::Ipv4Address>* agents);
    static ns3::TypeId GetTypeId(void);
    bool ImportRequest(ns3::Ptr<MsgInfo> msg);
    bool ImportResponse(ns3::Ptr<MsgInfo> msg);
    //ImportRequest makes a new TCP connection to next node in pipeline,
    //  MT goes to sources.
    //TCP connection request is always accepted,
    //  and MT is initially placed in new_mts.
    //Once (first part of) a ImportRequest is received:
    //  if localhost is msg->dst, MT moves to sinks;
    //  else, create an associated MT to next node in pipeline, both MTs go to intermediates.
    //After a ImportResponse is received in full, close MT.
    
  private:
    HostName localhost_;
    std::unordered_map<HostName,ns3::Ipv4Address>* agents_;

    std::unordered_map<MsgId,ns3::Ptr<MsgTransport>> sources_;
    ns3::Ptr<ns3::Socket> sock_;//listening socket
    std::list<ns3::Ptr<MsgTransport>> new_mts_;
    std::unordered_map<MsgId,std::pair<ns3::Ptr<MsgTransport>,ns3::Ptr<MsgTransport>>> intermediates_;
    std::unordered_map<MsgId,ns3::Ptr<MsgTransport>> sinks_;
    void StartApplication();
    void StopApplication() {}
    
    ns3::Ptr<MsgTransport> OpenConnection(HostName peer);
    void SourceResponseRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);//source receives ImportResponse in full: close MT

    void SockAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from);//accept TCP connection request
    void NewMTRequestProgress(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg, size_t progress);//new MT receives part of ImportRequest
    
    void IntermediateRequestProgress(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg, size_t progress);//intermediate pair.first receives part of ImportRequest: pump to pair.second
    void IntermediateResponseProgress(ns3::Ptr<MsgTransport> mt2, ns3::Ptr<MsgInfo> msg, size_t progress);//intermediate pair.second receives part of ImportResponse: pump to pair.first
    void IntermediateResponseSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);//intermediate pair.first sends ImportResponse in full: close MT pair
    
    void SinkResponseSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);//sink sends ImportResponse in full: close MT
    
    DISALLOW_COPY_AND_ASSIGN(ImportAgent);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_IMPORTAGENT_H_
