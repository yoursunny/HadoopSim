#include "netsim/msginfo.h"
namespace HadoopNetSim {

MsgInfo::MsgInfo(void) {
  this->id_ = MsgId_invalid;
  this->in_reply_to_ = MsgId_invalid;
  this->type_ = kMTNone;
  this->size_ = 0;
  this->success_ = false;
  this->cb_ = TransmitCb_null;
  this->userobj_ = NULL;
}

void MsgInfo::set_pipeline(const std::vector<HostName>& value) {
  assert(value.size() >= 2);
  std::unordered_set<HostName> hosts;
  for (std::vector<HostName>::const_iterator it = value.cbegin(); it != value.cend(); ++it) {
    std::pair<std::unordered_set<HostName>::iterator,bool> inserted = hosts.insert(*it);
    assert(inserted.second);
  }
  this->pipeline_ = value;
}

void MsgInfo::set_srcdst(HostName src, HostName dst) {
  std::vector<HostName> pipeline;
  pipeline.push_back(src);
  pipeline.push_back(dst);
  this->set_pipeline(pipeline);
}

HostName MsgInfo::FindNextHost(HostName localhost) const {
  std::vector<HostName>::const_iterator it;
  for (it = this->pipeline_.cbegin(); it != this->pipeline_.cend(); ++it) {
    if (*it == localhost) {
      if (++it != this->pipeline_.cend()) return *it;
    }
  }
  return HostName_invalid;
}

};//namespace HadoopNetSim
