/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include "Cluster.h"
#include "TaskTrackerStatus.h"
using namespace std;

bool TaskTrackerStatus::isLive()
{
    return isTrackerLive;
}

void TaskTrackerStatus::setLive(bool live)
{
    this->isTrackerLive = live;
}

long TaskTrackerStatus::getLastReportTime()
{
    return this->lastReportTime;
}

void TaskTrackerStatus::setLastReportTime(long reportTime)
{
    this->lastReportTime = reportTime;
}

long TaskTrackerStatus::getAvailMapSlots()
{
    return this->numAvailMapSlots;
}

void TaskTrackerStatus::setAvailMapSlots(long slot)
{
    assert(slot >= 0 && slot <= MaxMapSlots);
    this->numAvailMapSlots = slot;
}

long TaskTrackerStatus::getAvailReduceSlots()
{
    return this->numAvailReduceSlots;
}

void TaskTrackerStatus::setAvailReduceSlots(long slot)
{
    assert(slot >= 0 && slot <= MaxReduceSlots);
    this->numAvailReduceSlots = slot;
}
