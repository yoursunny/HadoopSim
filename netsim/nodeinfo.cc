#include "netsim/nodeinfo.h"
namespace HadoopNetSim {

NodeInfo::NodeInfo(void) {
  this->type_ = kNTNone,
  this->is_manager_ = false;
}

ns3::TypeId NodeInfo::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::NodeInfo")
    .SetParent<ns3::Object>()
    .AddConstructor<NodeInfo>();
}


}//namespace HadoopNetSim
