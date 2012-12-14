#ifndef HADOOPSIM_NETSIM_LINKSTAT_H_
#define HADOOPSIM_NETSIM_LINKSTAT_H_
#include "netsim/defs.h"
#include "netsim/topology.h"
namespace HadoopNetSim {

class LinkStat : public ns3::SimpleRefCount<LinkStat> {
  public:
    LinkStat(LinkId id, ns3::Ptr<ns3::NetDevice> device);
    virtual ~LinkStat(void) {}
    
    LinkId id(void) const { return this->id_; }
    float bandwidth_utilization(void) const { return this->bandwidth_utilization_; }
    float queue_utilization(void) const { return this->queue_utilization_; }

  private:
    LinkId id_;
    float bandwidth_utilization_;//sent packets / link capacity
    float queue_utilization_;//queue length / queue capacity
    
    float ReadQueueUtilization(ns3::Ptr<ns3::NetDevice> device);
    
    DISALLOW_COPY_AND_ASSIGN(LinkStat);
};

class BandwidthUtilizationCollector : public ns3::Object {
  public:
    BandwidthUtilizationCollector(void);
    virtual ~BandwidthUtilizationCollector(void) {}
    static ns3::TypeId GetTypeId(void);

    ns3::Ptr<ns3::NetDevice> device(void) const { return this->device_; }
    void set_device(ns3::Ptr<ns3::NetDevice> value);
    void Schedule(void);
    float Read(void);

  private:
    ns3::Ptr<ns3::NetDevice> device_;//outgoing device
    ns3::Ptr<ns3::Queue> queue_;//outgoing queue
    bool scheduled_;
    uint32_t last_total_bytes_;//total bytes in one second
    
    void ScheduleInternal(void);
    void Collect(void);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_LINKSTAT_H_
