#ifndef HADOOPSIM_NETSIM_DATASERVER_H_
#define HADOOPSIM_NETSIM_DATASERVER_H_
#include "netsim/defs.h"
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class DataServer : public ns3::Application {
  public:
    DataServer(HostName localhost);
    static ns3::TypeId GetTypeId(void);
    bool DataResponse(ns3::Ptr<MsgInfo> msg);
    
  private:
    HostName localhost_;
    ns3::Ptr<ns3::Socket> sock_;
    std::list<ns3::Ptr<MsgTransport>> new_mts_;//mt that has not received a message
    std::unordered_map<MsgId,ns3::Ptr<MsgTransport>> mts_;//data request msgid=>active mt
    void StartApplication();
    void StopApplication() {}
    
    void HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from);
    void HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);
    void HandleSend(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);
    
    DISALLOW_COPY_AND_ASSIGN(DataServer);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_DATASERVER_H_
