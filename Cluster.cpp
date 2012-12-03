/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include "Cluster.h"
#include "TopologyReader.h"
#include "ns3/Ns3.h"
using namespace std;

vector<MachineNode> nodeSet;

MachineNode::MachineNode(string rackName, string hostName, string ipAddr)
{
    this->rackName = rackName;
    this->hostName = hostName;
    this->ipAddr = ipAddr;
}

string MachineNode::getRackName()
{
    return this->rackName;
}

string MachineNode::getHostName()
{
    return this->hostName;
}

string MachineNode::getIpAddr()
{
    return this->ipAddr;
}

void MachineNode::setIpAddr(string ipAddr)
{
    this->ipAddr = ipAddr;
}

void setupCluster(int topoType)
{
    vector<HadoopHost> hostSet = getHostTopology();
    for(size_t i = 0; i < hostSet.size(); i++) {
        MachineNode n(hostSet[i].rackName, hostSet[i].hostName, hostSet[i].ipAddr);
        nodeSet.push_back(n);
    }
    setTopology(topoType, nodeSet);
    for(size_t j = 0; j < nodeSet.size(); j++) {
        cout<<"rackName = "<<nodeSet[j].getRackName()<<", hostName = "<<nodeSet[j].getHostName()<<", ipAddr = "<<nodeSet[j].getIpAddr()<<endl;
    }
}

vector<MachineNode> getClusterNodes()
{
    return nodeSet;
}

string findIPAddr4Host(string hostName)
{
    string ipAddr;
    size_t i;
    for(i = 0; i < nodeSet.size(); i++) {
        if (nodeSet[i].getHostName() == hostName) {
            ipAddr = nodeSet[i].getIpAddr();
            break;
        }
    }
    assert(i < nodeSet.size());
    return ipAddr;
}

string findHostName4IP(string hostIPAddr)
{
    string hostName;
    size_t i;
    for(i = 0; i < nodeSet.size(); i++) {
        if (nodeSet[i].getIpAddr() == hostIPAddr) {
            hostName = nodeSet[i].getHostName();
            break;
        }
    }
    assert(i < nodeSet.size());
    return hostName;
}
