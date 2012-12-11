#ifndef HADOOPSIM_NETSIM_NETSIM_H_
#define HADOOPSIM_NETSIM_NETSIM_H_
#include "netsim/defs.h"
#include <vector>
#include <map>
#include "netsim/topology.h"
#include "netsim/linkstat.h"
#include "netsim/msginfo.h"
#include "netsim/msgidgen.h"
namespace HadoopNetSim {

class NetSim {
  public:
    NetSim(void);
    const ns3::Ipv4Address& host_ip(const HostName& host);//get first IPv4 address of a host

    //----setup----
    void BuildTopology(const Topology& topo);//build network topology
    void InstallApps(const std::vector<HostName>& name_servers);//install apps on hosts

    //----transmit----
    //transmit a message 'now', call <cb> on completion
    MsgId NameRequest(const HostName& src, const HostName& dst, size_t size, TransmitCb& cb, void* userobj);
    MsgId NameResponse(const HostName& src, const HostName& dst, size_t size, TransmitCb& cb, void* userobj);
    MsgId DataRequest(const HostName& src, const HostName& dst, size_t size, TransmitCb& cb, void* userobj);
    MsgId DataResponse(MsgId in_reply_to, const HostName& src, const HostName& dst, size_t size, TransmitCb& cb, void* userobj);
    
    //----stat----
    ns3::Ptr<LinkStat> GetLinkStat(LinkId link);//get link utilization and queue usage 'now'
    
  private:
    MsgIdGen msgidgen_;
    std::map<HostName,ns3::Ptr<ns3::Node>> nodes_;//hostname=>node

    DISALLOW_COPY_AND_ASSIGN(NetSim);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NETSIM_H_
