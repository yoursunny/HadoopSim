#ifndef HADOOPSIM_NETSIM_NAMECLIENT_H_
#define HADOOPSIM_NETSIM_NAMECLIENT_H_
#include "netsim/defs.h"
namespace HadoopNetSim {

class NameClient : public ns3::Application {
  public:
    NameClient(void);
    static ns3::TypeId GetTypeId(void);
    
  private:
    bool running_;
    ns3::Ptr<ns3::Socket> sock_;
    void StartApplication();
    void StopApplication();
    
    void HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from);
    void HandleRead(ns3::Ptr<ns3::Socket> sock);
    
    DISALLOW_COPY_AND_ASSIGN(NameClient);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_NAMECLIENT_H_
