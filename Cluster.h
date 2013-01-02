/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef CLUSTER_H
#define CLUSTER_H

#include <string>
#include <vector>
#include "netsim/netsim.h"

const int MaxMapSlots = 2;
const int MaxReduceSlots = 2;
const int ClusterTopoTypes = 3;
const std::string TopoTypeArray[ClusterTopoTypes] = {"star", "rackrow", "fattree"};

class MachineNode {
public:
    MachineNode(void) {}
    virtual ~MachineNode(void) {}
    void configMachineNode(std::string hostName, std::string rackName = "");
    const std::string getRackName(void) const;
    const std::string getHostName(void) const;
private:
    std::string hostName;
    std::string rackName;
};

void setupCluster(int topoType, std::string topologyFile, bool needDebug, std::string debugDir);
const MachineNode& getClusterMasterNodes(void);
const std::vector<MachineNode>& getClusterSlaveNodes(void);
HadoopNetSim::NetSim *getNetSim(void);

#endif // CLUSTER_H
