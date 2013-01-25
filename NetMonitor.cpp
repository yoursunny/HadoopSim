#include <assert.h>
#include <stdio.h>
#include "Cluster.h"
#include "NetMonitor.h"
using namespace HadoopNetSim;
using namespace std;

static FILE *netFile = NULL;
static unordered_map<LinkId,ns3::Ptr<Link>> netSimLinks;
static unordered_map<LinkId,ns3::Ptr<LinkStat>> linkStats;

void checkLinkStat(void)
{
    fprintf(netFile, "%lu\n", ns3::Simulator::Now().GetMilliSeconds());
    for(LinkId id = 1; id <= netSimLinks.size(); ++id) {
        ns3::Ptr<LinkStat> stat = getNetSim()->GetLinkStat(id);
        fprintf(netFile, "LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", stat->id(), stat->bandwidth_utilization(linkStats.at(id))*100, stat->queue_utilization()*100);
        linkStats[id] = stat;

        stat = getNetSim()->GetLinkStat(-id);
        fprintf(netFile, "LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", stat->id(), stat->bandwidth_utilization(linkStats.at(-id))*100, stat->queue_utilization()*100);
        linkStats[-id] = stat;
    }
    fprintf(netFile, "\n");
    printf("%lu\n", ns3::Simulator::Now().GetMilliSeconds());
    ns3::Simulator::Schedule(ns3::Seconds(3.0), &checkLinkStat);
}

void enableNetMonitor(unordered_map<LinkId,ns3::Ptr<Link>> links, string debugDir)
{
    netSimLinks = links;
    netFile = fopen((debugDir + "linkstat.txt").c_str(), "w");
    for(LinkId id = 1; id <= netSimLinks.size(); ++id) {
        linkStats[id] = getNetSim()->GetLinkStat(id);
        linkStats[-id] = getNetSim()->GetLinkStat(-id);
    }
    ns3::Simulator::Schedule(ns3::Seconds(3.0), &checkLinkStat);
}

void disableNetMonitor()
{
    if(netFile)
        fclose(netFile);
}
