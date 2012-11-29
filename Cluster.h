/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef CLUSTER_H
#define CLUSTER_H

#include <string>
using namespace std;

const int MaxMapSlots = 2;
const int MaxReduceSlots = 2;

class MachineNode {
public:
    MachineNode(string rackName, string hostName, string ipAddr);
    string getRackName();
    string getHostName();
    string getIpAddr();
    void setIpAddr(string ipAddr);
private:
    string rackName;
    string hostName;
    string ipAddr;
};

void setupCluster(int topoType);
vector<MachineNode> getClusterNodes();
string findIPAddr4Host(string hostName);
string findHostName4IP(string hostIPAddr);

#endif

