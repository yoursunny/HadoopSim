#ifndef HADOOPSIM_NETSIM_MSGIDGEN_H_
#define HADOOPSIM_NETSIM_MSGIDGEN_H_
#include "netsim/defs.h"
namespace HadoopNetSim {

typedef uint64_t MsgId;

class MsgIdGenerator {
  public:
    MsgIdGenerator(void) { this->last_id_ = 0; }
    MsgId Next(void) {
      if (this->last_id_ == UINT64_MAX) this->last_id_ = 0;
      return ++this->last_id_;
    }
    
  private:
    MsgId last_id_;

    DISALLOW_COPY_AND_ASSIGN(MsgIdGenerator);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_MSGIDGEN_H_
