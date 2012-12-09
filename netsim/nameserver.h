#ifndef HADOOPSIM_NETSIM_NAMESERVER_H_
#define HADOOPSIM_NETSIM_NAMESERVER_H_
#include "netsim/defs.h"
#include <list>
namespace HadoopNetSim {

class NameServer : public ns3::Application {
  public:
    NameServer(void);
    static ns3::TypeId GetTypeId(void);
    
  private:
    bool running_;
    ns3::Ptr<ns3::Socket> sock_listen_;
    std::list<ns3::Ptr<ns3::Socket>> sock_accepted_;
    void StartApplication();
    void StopApplication();
    
    void HandleAccept(ns3::Ptr<ns3::Socket> sock, const ns3::Address& from);
    void HandleRead(ns3::Ptr<ns3::Socket> sock);
    
    DISALLOW_COPY_AND_ASSIGN(NameServer);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_NAMESERVER_H_
