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
    TaskTracker():usedMapSlots(0), usedReduceSlots(0), lastReportTime(0) { }
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
};

size_t reportArrive(string hostIPAddr);
void responseArrive(string hostIPAddr);
long initTaskTrackers(long startTime);
void killTaskTrackers();

#endif

