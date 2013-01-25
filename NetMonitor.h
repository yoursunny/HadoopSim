#ifndef NETMONITOR_H
#define NETMONITOR_H

#include <string>
#include <unordered_map>
#include "netsim/netsim.h"

void enableNetMonitor(std::unordered_map<HadoopNetSim::LinkId,ns3::Ptr<HadoopNetSim::Link>> netSimLinks, std::string debugDir);
void disableNetMonitor();

#endif // NETMONITOR_H
