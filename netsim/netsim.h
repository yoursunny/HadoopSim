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

typedef ns3::Callback<void,MsgId,TransmitType,const std::string&,const std::string&,bool,ns3::Time> TransmitCb;//void cb(msgid,src_host,dst_host,success,time)
typedef ns3::Callback<void,const ns3::Time&,const ns3::Time&,LinkId,float> LinkUtilCb;//void cb(interval_begin,interval_end,link,value)
typedef ns3::Callback<void,const ns3::Time&,LinkId,size_t> QueueLengthCb;//void cb(time,link,value)

class NetSim {
  public:
    NetSim(void);
    
    const std::set<std::string>& ns_hosts(void) { return this->ns_hosts_; }//get name servers
    void set_ns_hosts(const std::set<std::string>& value) { this->ns_hosts_ = value; }//set name servers
    uint32_t host_ip(const std::string& host);//get first IPv4 address of a host

    //----setup----
    void BuildTopology(const Topology& topo);//build network topology
    void InstallNameServer(void);//install NameServer app on name servers
    void InstallNameClient(void);//install NameClient app on all nodes other than name servers
    void InstallDataServer(void);//install DataServer app on all nodes other than name servers

    //----transmit----
    bool NameRequest(const ns3::Time& time, MsgId msgid, const std::string& nc_host, const std::string& ns_host, size_t size, TransmitCb& cb);//at <time>, transmit a message of <size> on NameClient-NameServer connection from <nc_host> to <ns_host>, call <cb> on completion
    bool NameResponse(const ns3::Time& time, MsgId msgid, size_t size, TransmitCb& cb);//at <time>, transmit a message of <size> on NameServer-NameClient connection in reply to NameRequest <msgid>, call <cb> on completion
    bool DataRequest(const ns3::Time& time, MsgId msgid, const std::string& dc_host, const std::string& ds_host, size_t size, TransmitCb& cb);//at <time>, install DataClient on <dc_host> to create a connection to <ds_host> and transmit a message of <size>, call <cb> on completion
    bool DataResponse(const ns3::Time& time, MsgId msgid, size_t size, TransmitCb& cb);//at <time>, transmit a message of <size> on DataServer-DataClient connection in reply to DataRequest <msgid>, uninstall DataClient and call <cb> on completion
    
    //----stat----
    bool CalcLinkUtil(const ns3::Time& interval_begin, const ns3::Time& interval_end, LinkId link, LinkUtilCb& cb);//request average link utilization of <link> between <interval_begin> and <interval_end>, call <cb> when result available (at <interval_end>)
    bool CalcQueueLength(const ns3::Time& time, LinkId link);//request output queue length of <link> at <time>, call <cb> when result available (at <time>)
    
  private:
    std::set<std::string> ns_hosts_;
    std::map<std::string,ns3::Node> nodes_;//hostname=>node
    DISALLOW_COPY_AND_ASSIGN(NetSim);
};

}//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_NETSIM_H_
