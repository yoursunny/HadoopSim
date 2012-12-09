#include "netsim/linkstat.h"
namespace HadoopNetSim {

LinkStat::LinkStat(LinkId id, float bw_util, float queue_util) {
  assert(id != 0);
  assert(bw_util >= 0 && bw_util <= 1);
  assert(queue_util >= 0 && queue_util <= 1);
  this->id_ = id;
  this->bw_util_ = bw_util;
  this->queue_util_ = queue_util;
}

};//namespace HadoopNetSim
