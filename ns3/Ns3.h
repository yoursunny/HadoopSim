/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef NS3_H
#define NS3_H

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/csma-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/ipv4-global-routing-helper.h"
#include "../Cluster.h"

typedef enum TopoType {
    BUS,
    STAR,
    DUMBBELL,
    TREE,
    FATTREE
}TopoType;

void setTopology(int topoType, std::vector<MachineNode> &nodeSet);
void transferHeartBeat(std::string src, size_t bytes, ns3::Time now);
void fetchRawData(std::string dest, std::string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, ns3::Time now);
void fetchMapData(std::string dest, std::string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, ns3::Time now);

#endif // NS3_H
