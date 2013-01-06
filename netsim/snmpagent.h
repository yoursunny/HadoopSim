#ifndef HADOOPSIM_NETSIM_SNMPAGENT_H_
#define HADOOPSIM_NETSIM_SNMPAGENT_H_
#include "netsim/defs.h"
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class SnmpAgent : public ns3::Application {
  public:
    SnmpAgent(std::unordered_map<HostName,ns3::Ipv4Address>* agents);
    static ns3::TypeId GetTypeId(void);
    bool Send(ns3::Ptr<MsgInfo> msg);
    
  private:
    std::unordered_map<HostName,ns3::Ipv4Address>* agents_;
    ns3::Ptr<ns3::Socket> sock_;
    void StartApplication();
    void StopApplication() {}
    
    void HandleRecv(ns3::Ptr<ns3::Socket> sock);
    
    DISALLOW_COPY_AND_ASSIGN(SnmpAgent);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_SNMPAGENT_H_
