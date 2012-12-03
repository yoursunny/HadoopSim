/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TASKTRACKERSTATUS_H
#define TASKTRACKERSTATUS_H

#include <string>

class TaskTrackerStatus {
public:
    TaskTrackerStatus(std::string trackerName): isTrackerLive(true), \
            lastReportTime(0), numAvailMapSlots(0), numAvailReduceSlots(0)
    {
        this->taskTrackerName = trackerName;
    }
    bool isLive();
    void setLive(bool live);
    long getLastReportTime();
    void setLastReportTime(long reportTime);
    long getAvailMapSlots();
    void setAvailMapSlots(long slot);
    long getAvailReduceSlots();
    void setAvailReduceSlots(long slot);
private:
    std::string taskTrackerName;
    bool isTrackerLive;
    long lastReportTime;
    long numAvailMapSlots;
    long numAvailReduceSlots;
};

#endif // TASKTRACKERSTATUS_H
