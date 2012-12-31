/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef NETMONITOR_H
#define NETMONITOR_H

#include <string>
#include <unordered_map>
#include "netsim/netsim.h"

void enableNetMonitor(std::unordered_map<HadoopNetSim::LinkId,ns3::Ptr<HadoopNetSim::Link>> netSimLinks, std::string debugDir);
void disableNetMonitor();

#endif // NETMONITOR_H
