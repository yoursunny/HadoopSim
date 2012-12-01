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
#include <assert.h>
#include <string>
#include <map>
#include <vector>
#include <iostream>
#include <sstream>
#include "../Cluster.h"
using namespace ns3;
using namespace std;

typedef enum TopoType {
    BUS,
    STAR,
    DUMBBELL,
    TREE,
    FATTREE
}TopoType;

void setTopology(int topoType, vector<MachineNode> &nodeSet);
void transferHeartBeat(string src, size_t bytes, Time now);
void fetchRawData(string dest, string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, Time now);
void fetchMapData(string dest, string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, Time now);

#endif
