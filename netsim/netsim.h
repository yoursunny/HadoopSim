#ifndef HADOOPSIM_NETSIM_NETSIM_H_
#define HADOOPSIM_NETSIM_NETSIM_H_
#include "netsim/defs.h"
#include <vector>
#include <map>
namespace HadoopNetSim {

enum TransmitType {
  kTTNameRequest,
  kTTNameResponse,
  kTTDataRequest,
  kTTDataResponse
};

typedef ns3::Callback<void,MsgId,TransmitType,const std::string&,const std::string&,bool,ns3::Time> TransmitCb;//void cb(msgid,transmit_type,src_host,dst_host,success,finish_time)

class LinkStat {
  public:
    LinkStat(LinkId id, float utilization, float queue_usage);
    LinkStat(const LinkStat& other) { this->operator=(other); }
    LinkStat& operator=(const LinkStat& other);
    
    LinkId id(void) const { return this->id_; }
    float utilization(void) const { return this->utilization_; }
    float queue_usage(void) const { return this->queue_usage_; }
  private:
    LinkId id_;
    float utilization_;//sent packets / link capacity
    float queue_usage_;//queue length / queue capacity
};

class NetSim {
  public:
    NetSim(void);
    
    const std::set<std::string>& ns_hosts(void) { return this->ns_hosts_; }//get name servers
    void set_ns_hosts(const std::set<std::string>& value) { this->ns_hosts_ = value; }//set name servers
    const ns3::Ipv4Address& host_ip(const std::string& host);//get first IPv4 address of a host

    //----setup----
    void BuildTopology(const Topology& topo);//build network topology
    void InstallNameServer(void);//install NameServer app on name servers
    void InstallNameClient(void);//install NameClient app on all nodes other than name servers
    void InstallDataServer(void);//install DataServer app on all nodes other than name servers

    //----transmit----
    //all transmissions start 'now', and call <cb> on completion
    bool NameRequest(MsgId msgid, const std::string& nc_host, const std::string& ns_host, size_t size, TransmitCb& cb);//transmit a message of <size> on NameClient-NameServer connection from <nc_host> to <ns_host>
    bool NameResponse(MsgId msgid, size_t size, TransmitCb& cb);//transmit a message of <size> on NameServer-NameClient connection in reply to NameRequest <msgid>
    bool DataRequest(MsgId msgid, const std::string& dc_host, const std::string& ds_host, size_t size, TransmitCb& cb);//install DataClient on <dc_host> to create a connection to <ds_host> and transmit a message of <size>
    bool DataResponse(MsgId msgid, size_t size, TransmitCb& cb);//transmit a message of <size> on DataServer-DataClient connection in reply to DataRequest <msgid>
    
    //----stat----
    LinkStat GetLinkStat(LinkId link);//get link utilization and queue usage 'now'
    
  private:
    std::set<std::string> ns_hosts_;
    std::map<std::string,ns3::Ptr<ns3::Node>> nodes_;//hostname=>node
    DISALLOW_COPY_AND_ASSIGN(NetSim);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_NETSIM_H_
