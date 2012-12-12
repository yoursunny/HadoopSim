#ifndef HADOOPSIM_NETSIM_LINKSTAT_H_
#define HADOOPSIM_NETSIM_LINKSTAT_H_
#include "netsim/defs.h"
#include "netsim/topology.h"
namespace HadoopNetSim {

class LinkStat : public ns3::SimpleRefCount<LinkStat> {
  public:
    LinkStat(LinkId id, float bw_util, float queue_util);
    virtual ~LinkStat(void) {}
    
    LinkId id(void) const { return this->id_; }
    float bw_util(void) const { return this->bw_util_; }
    float queue_util(void) const { return this->queue_util_; }

  private:
    LinkId id_;
    float bw_util_;//sent packets / link capacity
    float queue_util_;//queue length / queue capacity
    
    DISALLOW_COPY_AND_ASSIGN(LinkStat);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_LINKSTAT_H_
