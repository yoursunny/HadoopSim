/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef CLUSTER_H
#define CLUSTER_H

#include <string>
#include <vector>

const int MaxMapSlots = 2;
const int MaxReduceSlots = 2;

class MachineNode {
public:
    MachineNode(std::string rackName, std::string hostName, std::string ipAddr);
    std::string getRackName();
    std::string getHostName();
    std::string getIpAddr();
    void setIpAddr(std::string ipAddr);
private:
    std::string rackName;
    std::string hostName;
    std::string ipAddr;
};

void setupCluster(int topoType);
std::vector<MachineNode> getClusterNodes();
std::string findIPAddr4Host(std::string hostName);
std::string findHostName4IP(std::string hostIPAddr);

#endif // CLUSTER_H
