#ifndef TASKTRACKERSTATUS_H
#define TASKTRACKERSTATUS_H

#include <string>
#include "Task.h"

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
    std::vector<TaskStatus> getRunningMaps(void);
    void setRunningMaps(std::vector<TaskStatus> maps);
    std::vector<TaskStatus> getRunningReduces(void);
    void setRunningReduces(std::vector<TaskStatus> reduces);
private:
    std::string taskTrackerName;
    bool isTrackerLive;
    long lastReportTime;
    std::vector<TaskStatus> runningMaps;
    std::vector<TaskStatus> runningReduces;
    long numAvailMapSlots;
    long numAvailReduceSlots;
};

#endif // TASKTRACKERSTATUS_H
