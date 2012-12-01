/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TASKTRACKER_H
#define TASKTRACKER_H

#include <map>
#include <queue>
#include <list>
#include "Task.h"
#include "HEvent.h"
#include "HeartBeat.h"
using namespace std;

class TaskTracker: public EventListener{
public:
    TaskTracker():usedMapSlots(0), usedReduceSlots(0), lastReportTime(0), pendingTaskActionID(0), pendingMapDataActionID(0) { }
    void setHostName(string hostName);
    string getHostName();
    long getUsedMapSlots();
    void setUsedMapSlots(long slot);
    long getUsedReduceSlots();
    void setUsedReduceSlots(long slot);
    list<TaskStatus> collectTaskStatus(long now);
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
    map<string, Task> getRunningTasks();
    void addRunningTask(string taskID, Task task);
    MapDataAction getPendingMapDataAction(unsigned long mapDataActionID);
    void updateRunningTask(string taskID, Task task);
private:
    long usedMapSlots;
    long usedReduceSlots;
    map<string, Task> runningTasks;
    map<string, Task> killedTasks;
    map<string, Task> completedTasks;
    long lastReportTime;
    string hostName;
    queue<HeartBeatReport> reportQueue;
    queue<HeartBeatResponse> responseQueue;
    map<unsigned long, TaskAction> pendingTaskAction;
    unsigned long pendingTaskActionID;      // self-increasing only
    map<unsigned long, MapDataAction> pendingMapDataAction;
    unsigned long pendingMapDataActionID;   // self-increasing only
};

size_t reportArrive(string hostIPAddr);
void responseArrive(string hostIPAddr);
void dataArrive(unsigned long dataType, unsigned long dataRequestID, string hostIPAddr);
long initTaskTrackers(long startTime);
void killTaskTrackers();

#endif

