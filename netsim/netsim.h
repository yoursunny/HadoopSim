#ifndef HADOOPSIM_NETSIM_NETSIM_H_
#define HADOOPSIM_NETSIM_NETSIM_H_
#include "netsim/defs.h"
#include <unordered_set>
#include <unordered_map>
#include "netsim/topology.h"
#include "netsim/linkstat.h"
#include "netsim/msginfo.h"
#include "netsim/msgidgen.h"
namespace HadoopNetSim {

class NetSim {
  public:
    NetSim(void);

    //----setup----
    void BuildTopology(const Topology& topo);//build network topology
    ns3::Ipv4Address GetHostIP(HostName host);//get first IPv4 address of a host; returns ns3::Ipv4Address:GetAny() if there's no address; note: NetSim currently does not honor IP addresses from Topology
    void InstallApps(const std::unordered_set<HostName>& managers);//install apps on hosts
    bool ready(void) const { return this->setup_status_ == kSSReady; }
    void set_ready_cb(ns3::Callback<void,NetSim*> value) { this->ready_cb_ = value; }//schedule a callback when apps are initialized

    //----transmit----
    //transmit a message 'now', call <cb> on completion; returns MsgId_invalid on failure
    MsgId NameRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj);
    MsgId NameResponse(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj);
    MsgId DataRequest(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj);
    MsgId DataResponse(MsgId in_reply_to, HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj);
    MsgId Snmp(HostName src, HostName dst, size_t size, TransmitCb cb, void* userobj);
    MsgId ImportRequest(const std::vector<HostName>& pipeline, size_t size, TransmitCb cb, void* userobj);
    MsgId ImportResponse(MsgId in_reply_to, const std::vector<HostName>& pipeline, size_t size, TransmitCb cb, void* userobj);//pipeline should be the reverse of ImportRequest's
    
    //----stat----
    ns3::Ptr<LinkStat> GetLinkStat(LinkId link);//get link utilization and queue usage 'now'
    
  private:
    enum SetupStatus {
      kSSNone,
      kSSBuildTopology,
      kSSInstallApps,
      kSSReady
    };
    
    SetupStatus setup_status_;
    ns3::Callback<void,NetSim*> ready_cb_;
    ns3::Ipv4AddressHelper ipv4addr_;
    MsgIdGenerator msgidgen_;
    std::unordered_map<HostName,ns3::Ptr<ns3::Node>> nodes_;//hostname=>node
    std::unordered_map<LinkId,ns3::Ptr<ns3::NetDevice>> links_;//linkid=>outgoing interface
    std::unordered_set<HostName> switches_;
    std::unordered_map<HostName,ns3::Ipv4Address> all_nodes_;//managers+slaves+switches
    std::unordered_map<HostName,ns3::Ipv4Address> managers_;
    std::unordered_map<HostName,ns3::Ipv4Address> slaves_;

    //----setup----
    void AssertReady(void) { assert(this->setup_status_ == kSSReady); }
    void BuildNodes(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes);
    void BuildLinks(const std::unordered_map<LinkId,ns3::Ptr<Link>>& topo_links);
    //void AssignIPs(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes);
    void ConfigureRouting(const std::unordered_map<HostName,ns3::Ptr<Node>>& topo_nodes);
    void SetNetworkParameters(void);
    ns3::Ipv4Address GetNodeIP(ns3::Ptr<ns3::Node> node);
    void PopulateIPList(const std::unordered_set<HostName>& managers);
    void FireReadyCb(void);
    
    //----transmit----
    template<typename apptype> ns3::Ptr<apptype> GetNodeApp(ns3::Ptr<ns3::Node> node);
    ns3::Ptr<MsgInfo> MakeMsg(MsgType type, HostName src, HostName dst, size_t size, TransmitCb& cb, void* userobj);
    ns3::Ptr<MsgInfo> MakeMsg(MsgType type, const std::vector<HostName>& pipeline, size_t size, TransmitCb& cb, void* userobj);
    ns3::Ptr<MsgInfo> MakeMsgInternal(MsgType type, size_t size, TransmitCb& cb, void* userobj);

    DISALLOW_COPY_AND_ASSIGN(NetSim);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_NETSIM_H_
