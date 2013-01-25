#ifndef CLUSTER_H
#define CLUSTER_H

#include <string>
#include <vector>
#include "netsim/netsim.h"

const int MaxMapSlots = 2;
const int MaxReduceSlots = 2;
const int ClusterTopoTypes = 3;
const HadoopNetSim::TopoType TopoTypeArray[ClusterTopoTypes] = {HadoopNetSim::kTTStar, HadoopNetSim::kTTRackRow, HadoopNetSim::kTTNone/*fattree*/};

class MachineNode {
public:
    MachineNode(void) {}
    virtual ~MachineNode(void) {}
    void configMachineNode(std::string hostName, std::string rackName = "");
    const std::string getRackName(void) const;
    const std::string getHostName(void) const;
    void setRackName(std::string rack) { this->rackName = rack; }
private:
    std::string hostName;
    std::string rackName;
};

void setupCluster(int topoType, int nodesPerRack, std::string topologyFile, bool needDebug, std::string debugDir);
const MachineNode& getClusterMasterNodes(void);
const std::vector<MachineNode>& getClusterSlaveNodes(void);
std::vector<std::string> getClusterSlaveNodeName(void);
const std::vector<std::string>& getSwitches(void);
const std::unordered_multimap<HadoopNetSim::HostName, HadoopNetSim::LinkId>& getClusterGraph(void);
std::vector<HadoopNetSim::LinkId> getNodeOutLinks(std::string nodeName);
const std::unordered_map<HadoopNetSim::RackName, std::vector<HadoopNetSim::HostName>> &getRackSet(void);
int getHopNumber(HadoopNetSim::HostName src, HadoopNetSim::HostName dst);
HadoopNetSim::NetSim *getNetSim(void);

#endif // CLUSTER_H
