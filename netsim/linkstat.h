#ifndef HADOOPSIM_NETSIM_LINKSTAT_H_
#define HADOOPSIM_NETSIM_LINKSTAT_H_
#include "netsim/defs.h"
#include "netsim/topology.h"
namespace HadoopNetSim {

class LinkStat;

class LinkStatReader : public ns3::Object {
  public:
    LinkStatReader(LinkId id, ns3::Ptr<ns3::NetDevice> device);
    virtual ~LinkStatReader(void) {}
    static ns3::TypeId GetTypeId(void);

    LinkId id(void) const { return this->id_; }
    uint64_t bandwidth() const;

    ns3::Ptr<LinkStat> Read(void);
    float CalcBandwidthUtilization(uint32_t total_bytes_prev, double timestamp_prev, uint32_t total_bytes_now, double timestamp_now);
    float CalcQueueUtilization(uint32_t queue_length);

  private:
    LinkId id_;
    ns3::Ptr<ns3::NetDevice> device_;//outgoing device
    ns3::DataRate datarate_;
    ns3::Ptr<ns3::DropTailQueue> queue_;//outgoing queue
    uint32_t queue_capacity_;//queue capacity (packets or bytes)
    
    DISALLOW_COPY_AND_ASSIGN(LinkStatReader);
};

class LinkStat : public ns3::SimpleRefCount<LinkStat> {
  public:
    LinkStat(ns3::Ptr<LinkStatReader> lsr, uint32_t total_bytes, uint32_t queue_length);
    virtual ~LinkStat(void) {}
    
    LinkId id(void) const { return this->lsr_->id(); }
    uint64_t bandwidth(void) const { return this->lsr_->bandwidth(); }
    float bandwidth_utilization(const ns3::Ptr<LinkStat> previous) const;
    float queue_utilization(void) const;

    float bandwidth_utilization(void) const __attribute__ ((deprecated)) { return NAN; }//deprecated

  private:
    ns3::Ptr<LinkStatReader> lsr_;
    double timestamp_;//timestamp (seconds)
    uint32_t total_bytes_;//total outgoing bytes since device start
    uint32_t queue_length_;//queue length (packets or bytes)
    
    DISALLOW_COPY_AND_ASSIGN(LinkStat);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_LINKSTAT_H_
