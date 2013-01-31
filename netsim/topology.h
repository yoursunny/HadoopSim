#ifndef HADOOPSIM_NETSIM_TOPOLOGY_H_
#define HADOOPSIM_NETSIM_TOPOLOGY_H_
#include "netsim/defs.h"
#include <unordered_set>
#include <unordered_map>
#include "json/json.h"
namespace HadoopNetSim {

enum NodeType {
  kNTNone,
  kNTHost,
  kNTSwitch
};

typedef std::string HostName;
static const HostName HostName_invalid = "";
typedef std::string DeviceName;
typedef std::string RackName;   // only used for Hadoop cluster nodes

class Node : public ns3::SimpleRefCount<Node> {
  public:
    Node(void);
    virtual ~Node(void) {}

    HostName name(void) const { return this->name_; }
    NodeType type(void) const { return this->type_; }
    RackName rack(void) const { return this->rack_; }
    void set_rack(RackName rack) { this->rack_ = rack; }
    const ns3::Ipv4Address& ip(void) const { return this->ip_; }
    const std::unordered_set<DeviceName>& devices(void) const { return this->devices_; }

    void FromJson(::json_value* o);

  private:
    HostName name_;
    NodeType type_;
    RackName rack_;
    ns3::Ipv4Address ip_;
    std::unordered_set<DeviceName> devices_;

    DISALLOW_COPY_AND_ASSIGN(Node);
};

typedef int32_t LinkId;
static const LinkId LinkId_invalid = 0;
//LinkId>0: from node1.port1 to node2.port2
//LinkId<0: from node2.port2 to node1.port1

enum LinkType {
  kLTNone,
  kLTEth1G,
  kLTEth10G
};

class Link : public ns3::SimpleRefCount<Node> {
  public:
    Link(void);
    virtual ~Link(void) {}

    LinkId id(void) const { return this->id_; }
    LinkId rid(void) const { return -this->id_; }//LinkId from node2.port2 to node1.port1
    HostName node1(void) const { return this->node1_; }
    DeviceName port1(void) const { return this->port1_; }
    HostName node2(void) const { return this->node2_; }
    DeviceName port2(void) const { return this->port2_; }
    LinkType type(void) const { return this->type_; }

    void FromJson(::json_value* o);

  private:
    LinkId id_;//LinkId from node1.port1 to node2.port2
    HostName node1_;
    DeviceName port1_;
    HostName node2_;
    DeviceName port2_;
    LinkType type_;

    DISALLOW_COPY_AND_ASSIGN(Link);
};

enum TopoType {
  kTTNone,
  kTTGeneric,
  kTTStar,
  kTTRackRow
};

class Topology {
  public:
    Topology(void) {}
    TopoType type(void) const { return this->type_; }
    const std::unordered_map<HostName,ns3::Ptr<Node>>& nodes(void) const { return this->nodes_; }
    const std::unordered_map<LinkId,ns3::Ptr<Link>>& links(void) const { return this->links_; }
    const std::unordered_multimap<HostName,LinkId>& graph(void) const { return this->graph_; }

    void Load(const std::string& filename);
    void LoadString(char* json);

    uint16_t PathLength(HostName src, HostName dst);//calculate path length between two nodes; works for kTTStar and kTTRackRow only

  private:
    enum RackRowLayer {
      kRRLNone = 0,
      kRRLCore = 1,
      kRRLEndOfRow = 2,
      kRRLTopOfRack = 3,
      kRRLHost = 4
    };
    class RackRowPosition {
      public:
        RackRowPosition(ns3::Ptr<Node> node, RackRowLayer layer, int index = -1);
        RackRowPosition(const RackRowPosition& other) { this->operator=(other); }
        RackRowPosition& operator=(const RackRowPosition& other);
        ns3::Ptr<Node> node(void) const { return this->node_; }
        RackRowLayer layer(void) const { return this->layer_; }
        int index(void) const { return this->index_; }
      private:
        ns3::Ptr<Node> node_;
        RackRowLayer layer_;
        int index_;
    };

    TopoType type_;
    std::unordered_map<HostName,ns3::Ptr<Node>> nodes_;//name=>Node
    std::unordered_map<LinkId,ns3::Ptr<Link>> links_;//positive LinkId=>Link
    std::unordered_multimap<HostName,LinkId> graph_;//host=>array of outgoing links

    RackRowPosition RackRow_Position(ns3::Ptr<Node> node);
    RackRowPosition RackRow_Up(RackRowPosition nodepos);

    DISALLOW_COPY_AND_ASSIGN(Topology);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_TOPOLOGY_H_
