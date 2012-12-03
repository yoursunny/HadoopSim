/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TOPOLOGYREADER_H
#define TOPOLOGYREADER_H

#include <vector>

typedef struct HadoopHost {
    std::string hostName;
    std::string rackName;
    std::string ipAddr;
}HadoopHost;

typedef struct HadoopRack {
    std::string rackName;
    std::vector<HadoopHost> hostSet;
}HadoopRack;

typedef struct ClusterTopo {
    std::string clusterName;
    std::vector<HadoopRack> rackSet;
}ClusterTopo;

void initTopologyReader(std::string topologyFile, bool debug);
std::vector<HadoopHost> getHostTopology();

#endif // TOPOLOGYREADER_H
