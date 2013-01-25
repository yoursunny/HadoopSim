#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include <iostream>
#include "Cluster.h"
#include "HadoopSim.h"
#include "NetMonitor.h"
using namespace HadoopNetSim;
using namespace std;

/* static variables */
static NetSim netSim;
static MachineNode masterNode;
static vector<MachineNode> slaveNodeSet;
static vector<string> switchSet;
static vector<string> allDevices;
static unordered_multimap<HostName, LinkId> clusterGraph;
static unordered_map<RackName, vector<HostName>> rackSet;
static int rackNum = 0;
static Topology topology;

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

void printClusterGraph()
{
    cout<<"Cluster Graph: \n";
    for(size_t i = 0; i < allDevices.size(); i++) {
        cout<<"HostName = "<<allDevices[i]<<", # of outgoing links = "<<clusterGraph.count(allDevices[i])<<endl;
        cout<<"\t";
        auto range = clusterGraph.equal_range(allDevices[i]);
        for_each(
            range.first,
            range.second,
            [](unordered_multimap<HostName, LinkId>::value_type& x) { cout<<" "<<x.second; }
        );
        cout<<endl;
    }
}

void printRackSet()
{
    assert(rackNum == rackSet.size());
    cout<<"Rack Set: # rack = "<<rackNum<<"\n";
    for(unordered_map<RackName, vector<HostName>>::iterator it = rackSet.begin(); it != rackSet.end(); ++it) {
        cout<<"RackName = "<<it->first<<endl;
        cout<<"\t";
        for(size_t i = 0; i < it->second.size(); ++i) {
            cout<<" "<<it->second[i];
        }
        cout<<endl;
    }
}

void setupCluster(int topoType, int nodesPerRack, string topologyFile, bool needDebug, string debugDir)
{
    topology.Load(topologyFile);
    if (topoType >= ClusterTopoTypes || topology.type() != TopoTypeArray[topoType]) {
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
                masterNode.configMachineNode(it->second->name(), it->second->rack());
                rackSet[it->second->rack()].push_back("manager");
                allDevices.push_back("manager");
            } else {
                MachineNode node;
                node.configMachineNode(it->second->name(), it->second->rack());
                slaveNodeSet.push_back(node);
                rackSet[it->second->rack()].push_back(it->second->name());
                allDevices.push_back(it->second->name());
            }
        } else if (it->second->type() == kNTSwitch) {
            switchSet.push_back(it->second->name());
            allDevices.push_back(it->second->name());
        }
        ++it;
    }
    clusterGraph = topology.graph();
    printClusterGraph();

    if (TopoTypeArray[topoType] == kTTRackRow) {
        rackNum = (int)rackSet.size();
    } else if (TopoTypeArray[topoType] == kTTStar) {
        rackSet.clear();
        rackNum = (int)ceil((slaveNodeSet.size()+1) * 1.0 / nodesPerRack);
        size_t k = 0;
        for(int i = 0; i < rackNum; ++i) {
            string rackPrefix = "rack";
            rackPrefix.append(to_string(i));
            for(int j = 0; j < nodesPerRack; ++j) {
                if (i == 0 && j == 0)
                    rackSet[rackPrefix].push_back("manager");
                else {
                    rackSet[rackPrefix].push_back(slaveNodeSet[k].getHostName());
                    ++k;
                    if (k >= slaveNodeSet.size()) goto Done;
                }
            }
        }
    }
Done:
    printRackSet();

    // setup NetSim network topology
    netSim.BuildTopology(topology);
    netSim.InstallApps(manager);
    netSim.set_ready_cb(ns3::MakeCallback(&completeCluster));

    if (needDebug) {    // only used for default scheduler
        enableNetMonitor(topology.links(), debugDir);
    }
}

const MachineNode& getClusterMasterNodes(void)
{
    return masterNode;
}

const vector<MachineNode>& getClusterSlaveNodes(void)
{
    return slaveNodeSet;
}

vector<string> getClusterSlaveNodeName(void)
{
    vector<string> nodeNames;
    for(size_t i = 0; i < slaveNodeSet.size(); ++i) {
        nodeNames.push_back(slaveNodeSet[i].getHostName());
    }
    return nodeNames;
}

const vector<string>& getSwitches(void)
{
    return switchSet;
}

const unordered_multimap<HostName, LinkId>& getClusterGraph(void)
{
    return clusterGraph;
}

vector<LinkId> getNodeOutLinks(string nodeName)
{
    auto range = clusterGraph.equal_range(nodeName);
    assert(range.first != clusterGraph.end());
    vector<LinkId> links;
    for_each(
        range.first,
        range.second,
        [&](unordered_multimap<HostName, LinkId>::value_type& x) { links.push_back(x.second); }
    );
    return links;
}

const unordered_map<RackName, vector<HostName>> &getRackSet(void)
{
    return rackSet;
}

int getHopNumber(HostName src, HostName dst)
{
    return topology.PathLength(src, dst);
}

NetSim *getNetSim(void)
{
    return &netSim;
}
