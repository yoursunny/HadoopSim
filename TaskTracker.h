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
#include "Cluster.h"
#include "HeartBeat.h"
#include "HEvent.h"
#include "Task.h"

class TaskTracker: public EventListener{
public:
    TaskTracker():usedMapSlots(0), usedReduceSlots(0), lastReportTime(0) { }
    void setHostName(std::string hostName);
    const std::string getHostName() const;
    const long getUsedMapSlots() const;
    void setUsedMapSlots(long slot);
    const long getUsedReduceSlots() const;
    void setUsedReduceSlots(long slot);
    std::list<TaskStatus> collectTaskStatus(long now);

    // call back functions used by netsim
    void hbReport(ns3::Ptr<HadoopNetSim::MsgInfo> request_msg);
    void hbResponse(ns3::Ptr<HadoopNetSim::MsgInfo> request_msg);
    void dataRequest(ns3::Ptr<HadoopNetSim::MsgInfo> request_msg);
    void dataResponse(ns3::Ptr<HadoopNetSim::MsgInfo> request_msg);
    void sendHeartbeat(long evtTime);
    void handleHeartbeatResponse(HeartBeatResponse *response, long evtTime);
    void completeMapTask(long evtTime);
    void handleNewEvent(long timestamp, EvtType type);
    long getLastReportTime();
    void setLastReportTime(long reportTime);
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
    std::map<HadoopNetSim::MsgId, HeartBeatReport> reportMap;
    std::map<HadoopNetSim::MsgId, HeartBeatResponse> responseMap;
    std::map<HadoopNetSim::MsgId, TaskAction> pendingTaskAction;
    std::map<HadoopNetSim::MsgId, MapDataAction> pendingMapDataAction;
};

long initTaskTrackers(void);
void killTaskTrackers();

#endif // TASKTRACKER_H
