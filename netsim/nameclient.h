#ifndef HADOOPSIM_NETSIM_NAMECLIENT_H_
#define HADOOPSIM_NETSIM_NAMECLIENT_H_
#include "netsim/defs.h"
#include <vector>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class NameClient : public ns3::Application {
  public:
    NameClient(std::vector<std::pair<HostName,ns3::Ipv4Address>>* name_servers);
    static ns3::TypeId GetTypeId(void);
    bool NameRequest(ns3::Ptr<MsgInfo> msg);
    
  private:
    std::vector<std::pair<HostName,ns3::Ipv4Address>>* name_servers_;
    std::unordered_map<HostName,ns3::Ptr<MsgTransport>> mts_;//peer=>mt
    void StartApplication();
    void StopApplication() {}
    
    void HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);

    DISALLOW_COPY_AND_ASSIGN(NameClient);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NAMECLIENT_H_
