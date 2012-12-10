#ifndef HADOOPSIM_NETSIM_NETSIM_H_
#define HADOOPSIM_NETSIM_NETSIM_H_
#include "netsim/defs.h"
#include <vector>
#include <map>
#include "netsim/topology.h"
#include "netsim/linkstat.h"
#include "netsim/msginfo.h"
namespace HadoopNetSim {

class NetSim {
  public:
    NetSim(void);
    
    const std::set<HostName>& ns_hosts(void) { return this->ns_hosts_; }//get name servers
    void set_ns_hosts(const std::set<HostName>& value) { this->ns_hosts_ = value; }//set name servers
    const ns3::Ipv4Address& host_ip(const HostName& host);//get first IPv4 address of a host

    //----setup----
    void BuildTopology(const Topology& topo);//build network topology
    void InstallNameServer(void);//install NameServer app on name servers
    void InstallNameClient(void);//install NameClient app on all nodes other than name servers
    void InstallDataServer(void);//install DataServer app on all nodes other than name servers

    //----transmit----
    //transmit a message 'now', call <cb> on completion
    bool NameRequest(MsgId msgid, const HostName& src, const HostName& dst, size_t size, TransmitCb& cb);
    bool NameResponse(MsgId msgid, const HostName& src, const HostName& dst, size_t size, TransmitCb& cb);
    bool DataRequest(MsgId msgid, const HostName& src, const HostName& dst, size_t size, TransmitCb& cb);
    bool DataResponse(MsgId msgid, MsgId in_reply_to, const HostName& src, const HostName& dst, size_t size, TransmitCb& cb);
    
    //----stat----
    ns3::Ptr<LinkStat> GetLinkStat(LinkId link);//get link utilization and queue usage 'now'
    
  private:
    std::set<HostName> ns_hosts_;
    std::map<HostName,ns3::Ptr<ns3::Node>> nodes_;//hostname=>node
    DISALLOW_COPY_AND_ASSIGN(NetSim);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NETSIM_H_
