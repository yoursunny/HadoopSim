/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TASKTRACKER_H
#define TASKTRACKER_H

#include <list>
#include <map>
#include <queue>
#include <string>
#include "HeartBeat.h"
#include "HEvent.h"
#include "Task.h"

class TaskTracker: public EventListener{
public:
    TaskTracker():usedMapSlots(0), usedReduceSlots(0), lastReportTime(0), pendingTaskActionID(0), pendingMapDataActionID(0) { }
    void setHostName(std::string hostName);
    std::string getHostName();
    long getUsedMapSlots();
    void setUsedMapSlots(long slot);
    long getUsedReduceSlots();
    void setUsedReduceSlots(long slot);
    std::list<TaskStatus> collectTaskStatus(long now);
    void sendHeartbeat(long evtTime);
    void handleHeartbeatResponse(HeartBeatResponse *response, long evtTime);
    void completeMapTask(long evtTime);
    void handleNewEvent(long timestamp, EvtType type);
    long getLastReportTime();
    void setLastReportTime(long reportTime);
    HeartBeatReport getReport();
    void addResponse(HeartBeatResponse response);
    HeartBeatResponse getResponse();
    TaskAction getPendingTaskAction(unsigned long taskActionID);
    std::map<std::string, Task> getRunningTasks();
    void addRunningTask(std::string taskID, Task task);
    MapDataAction getPendingMapDataAction(unsigned long mapDataActionID);
    void updateRunningTask(std::string taskID, Task task);
private:
    long usedMapSlots;
    long usedReduceSlots;
    std::map<std::string, Task> runningTasks;
    std::map<std::string, Task> killedTasks;
    std::map<std::string, Task> completedTasks;
    long lastReportTime;
    std::string hostName;
    std::queue<HeartBeatReport> reportQueue;
    std::queue<HeartBeatResponse> responseQueue;
    std::map<unsigned long, TaskAction> pendingTaskAction;
    unsigned long pendingTaskActionID;      // self-increasing only
    std::map<unsigned long, MapDataAction> pendingMapDataAction;
    unsigned long pendingMapDataActionID;   // self-increasing only
};

size_t reportArrive(std::string hostIPAddr);
void responseArrive(std::string hostIPAddr);
void dataArrive(unsigned long dataType, unsigned long dataRequestID, std::string hostIPAddr);
long initTaskTrackers(long startTime);
void killTaskTrackers();

#endif // TASKTRACKER_H
