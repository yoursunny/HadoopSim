#ifndef HADOOPSIM_NETSIM_NODEINFO_H_
#define HADOOPSIM_NETSIM_NODEINFO_H_
#include "netsim/defs.h"
#include "netsim/topology.h"
namespace HadoopNetSim {

class NodeInfo : public ns3::Object {
  public:
    NodeInfo(void);
    static ns3::TypeId GetTypeId(void);
    
    const std::string& name(void) const { return this->name_; }
    void set_name(const std::string& value) { this->name_ = value; }
    NodeType type(void) const { return this->type_; }
    void set_type(NodeType value) { this->type_ = value; }
    bool is_manager(void) const { return this->is_manager_; }
    void set_is_manager(bool value) { this->is_manager_ = value; }
    
  private:
    std::string name_;
    NodeType type_;
    bool is_manager_;
    DISALLOW_COPY_AND_ASSIGN(NodeInfo);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NS3_NODEINFO_H_
