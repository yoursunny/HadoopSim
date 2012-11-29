/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
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
    this->numAvailMapSlots = slot;
}

long TaskTrackerStatus::getAvailReduceSlots()
{
    return this->numAvailReduceSlots;
}

void TaskTrackerStatus::setAvailReduceSlots(long slot)
{
    this->numAvailReduceSlots = slot;
}

