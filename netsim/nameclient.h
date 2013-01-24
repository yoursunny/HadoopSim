#ifndef HADOOPSIM_NETSIM_NAMECLIENT_H_
#define HADOOPSIM_NETSIM_NAMECLIENT_H_
#include "netsim/defs.h"
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class NameClient : public ns3::Application {
  public:
    NameClient(HostName localhost, std::unordered_map<HostName,ns3::Ipv4Address>* name_servers);
    static ns3::TypeId GetTypeId(void);
    bool NameRequest(ns3::Ptr<MsgInfo> msg);
    
  private:
    HostName localhost_;
    std::unordered_map<HostName,ns3::Ipv4Address>* name_servers_;
    std::unordered_map<HostName,ns3::Ptr<MsgTransport>> mts_;//peer=>mt
    void StartApplication();
    void StopApplication() {}
    
    DISALLOW_COPY_AND_ASSIGN(NameClient);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NAMECLIENT_H_
