#ifndef HADOOPSIM_NETSIM_NAMESERVER_H_
#define HADOOPSIM_NETSIM_NAMESERVER_H_
#include "netsim/defs.h"
#include <list>
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class NameServer : public ns3::Application {
  public:
    NameServer(HostName localhost);
    static ns3::TypeId GetTypeId(void);
    bool NameResponse(ns3::Ptr<MsgInfo> msg);
    
  private:
    HostName localhost_;
    ns3::Ptr<ns3::Socket> sock_;
    std::list<ns3::Ptr<MsgTransport>> new_mts_;//mt that has not received a message
    std::unordered_map<HostName,ns3::Ptr<MsgTransport>> mts_;//peer=>active mt
    void StartApplication();
    void StopApplication() {}
    
    void HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from);
    void HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);
    ns3::Ptr<MsgTransport> FindMTByPeer(HostName peer) const;
    
    DISALLOW_COPY_AND_ASSIGN(NameServer);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NAMESERVER_H_
