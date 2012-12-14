/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include "Cluster.h"
#include "HadoopSim.h"
using namespace HadoopNetSim;
using namespace std;

/* static variables */
static NetSim netSim;
static MachineNode masterNode;
static vector<MachineNode> slaveNodeSet;

void MachineNode::configMachineNode(string hostName, string rackName)
{
    this->hostName = hostName;
    this->rackName = rackName;
}

const string MachineNode::getRackName(void) const
{
    return this->rackName;
}

const string MachineNode::getHostName(void) const
{
    return this->hostName;
}

void setupCluster(int topoType, string topologyFile)
{
    Topology topology;
    topology.Load(topologyFile);
    if (topoType >= ClusterTopoTypes || topology.topotype().compare(TopoTypeArray[topoType]) != 0) {
        cout<<"Cluster topology type does not match topo json file.\n";
        exit(1);
    }

    // One NameNode&JobTracker in HadoopSim
    unordered_set<HostName> manager;
    manager.insert("manager");

    // construct MachineNode vector
    unordered_map<HostName,ns3::Ptr<Node>> netSimNode = topology.nodes();
    unordered_map<HostName,ns3::Ptr<Node>>::iterator it = netSimNode.begin();
    while(it != netSimNode.end()) {
        if (it->second->type() == kNTHost) {
            if (it->second->name().compare("manager") == 0) {
                masterNode.configMachineNode(it->second->name());
            } else {
                MachineNode node;
                node.configMachineNode(it->second->name());
                slaveNodeSet.push_back(node);
            }
        }
        ++it;
    }

    // setup NetSim network topology
    netSim.BuildTopology(topology);
    netSim.InstallApps(manager);
    netSim.set_ready_cb(ns3::MakeCallback(&completeCluster));
}

const MachineNode& getClusterMasterNodes(void)
{
    return masterNode;
}

const vector<MachineNode>& getClusterSlaveNodes(void)
{
    return slaveNodeSet;
}

HadoopNetSim::NetSim& getNetSim(void)
{
    return netSim;
}
