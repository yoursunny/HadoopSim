#include "netsim/msginfo.h"
namespace HadoopNetSim {

MsgInfo::MsgInfo(void) {
  this->id_ = 0;
  this->type_ = kMTNone;
  this->size_ = 0;
  this->success_ = false;
  this->cb_ = TransmitCb_null;
  this->userobj_ = NULL;
}

};//namespace HadoopNetSim
