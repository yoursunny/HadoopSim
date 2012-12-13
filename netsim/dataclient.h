#ifndef HADOOPSIM_NETSIM_DATACLIENT_H_
#define HADOOPSIM_NETSIM_DATACLIENT_H_
#include "netsim/defs.h"
#include <unordered_map>
#include "netsim/msgtransport.h"
namespace HadoopNetSim {

class DataClient : public ns3::Application {
  public:
    DataClient(std::unordered_map<HostName,ns3::Ipv4Address>* data_servers);
    static ns3::TypeId GetTypeId(void);
    bool DataRequest(ns3::Ptr<MsgInfo> msg);
    
  private:
    std::unordered_map<HostName,ns3::Ipv4Address>* data_servers_;
    std::unordered_map<MsgId,ns3::Ptr<MsgTransport>> mts_;
    void StartApplication() {}
    void StopApplication() {}
    
    void HandleRecv(ns3::Ptr<MsgTransport> mt, ns3::Ptr<MsgInfo> msg);

    DISALLOW_COPY_AND_ASSIGN(DataClient);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NAMECLIENT_H_
