/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include "Task.h"
#include "Cluster.h"
#include "TaskTracker.h"
#include "EventQueue.h"
#include "JobTracker.h"
#include "ns3/Ns3.h"
using namespace std;

/* TaskTracker Variables */
static TaskTracker *taskTrackers;
static int numTaskTrackers = 0;
const long clusterStartupDuration = 100 * 1000;     //milliseconds
const long HEARTBEAT_INTERVAL_MIN = 3 * 1000;       //milliseconds
const int CLUSTER_INCREMENT = 100;
static long nextHeartbeatInterval = 0;

void TaskTracker::setHostName(string hostName)
{
    this->hostName = hostName;
}

string TaskTracker::getHostName()
{
    return this->hostName;
}

long TaskTracker::getUsedMapSlots()
{
    return this->usedMapSlots;
}

void TaskTracker::setUsedMapSlots(long slot)
{
    this->usedMapSlots = slot;
}

long TaskTracker::getUsedReduceSlots()
{
    return this->usedReduceSlots;
}

void TaskTracker::setUsedReduceSlots(long slot)
{
    this->usedReduceSlots = slot;
}

long TaskTracker::getLastReportTime()
{
    return this->lastReportTime;
}

void TaskTracker::setLastReportTime(long reportTime)
{
    this->lastReportTime = reportTime;
}

list<TaskStatus> TaskTracker::collectTaskStatus(long now)
{
    list<TaskStatus> allTaskStatus;
    map<string, Task>::iterator it;
    Task task;
    double EPSILON = 0.0001;
    for(it = runningTasks.begin(); it != runningTasks.end();) {
        cout<<"it->second.getTaskTrackerName() = "<<it->second.getTaskTrackerName()<<endl;
        cout<<"this->hostName = "<<this->hostName<<endl;
        assert(it->second.getTaskTrackerName() == this->hostName);
        TaskStatus status = it->second.getTaskStatus();

        // check if it is a running task
        double progress = 0.0;
        assert(status.runState == RUNNING);
        if (status.type == MAPTASK) {
            progress = ((double)(now - status.startTime)) / (status.finishTime - status.startTime);
        }
        else {
            Phase reducePhase = status.runPhase;
            switch(reducePhase) {
                case SHUFFLE:
                    progress = 0.0;
                    break;
                case SORT:
                    progress = 1.0 / 3;
                    break;
                case REDUCE:
                    progress = 2.0 / 3 + (((double) (now - status.startTime)) / (status.finishTime - status.startTime)) / 3.0;
                    break;
                default:
                    cout<<"Error in collectTaskStatus().\n";
                    break;
            }
        }

        if (progress < -EPSILON || progress > 1 + EPSILON) {
            cout<<"Error progress = "<<progress<<endl;
            cout<<"Error on task progress in collectTaskStatus().\n";
//            assert(0);
        }
        progress = max(min(1.0, progress), 0.0);
        task = it->second;
        task.setTaskStatus(progress, now);
        it->second = task;

        // insert into the list by the oder:  map tasks (list head) -----   reduct tasks (list tail)
        if (status.type == MAPTASK) {
            allTaskStatus.push_front(it->second.getTaskStatus());
        } else {
            allTaskStatus.push_back(it->second.getTaskStatus());
        }

        if (it->second.getTaskStatus().runState == SUCCEEDED) {
            if (it->second.getTaskStatus().type == MAPTASK) {
                assert(getUsedMapSlots() > 0 && getUsedMapSlots() < MaxMapSlots + 1);
                setUsedMapSlots(getUsedMapSlots() - 1);
            }
            else {
                assert(getUsedReduceSlots() > 0 && getUsedReduceSlots() < MaxReduceSlots + 2);
                setUsedReduceSlots(getUsedReduceSlots() - 1);
            }
            completedTasks.insert(pair<string, Task>(it->first, it->second));
            cout<<it->second.getTaskStatus().taskAttemptID<<" done on "<<it->second.getTaskStatus().taskTracker<<endl;
            runningTasks.erase(it++);
        }
        else
            it++;
    }
    return allTaskStatus;
}

TaskAction TaskTracker::getPendingTaskAction(unsigned long taskActionID)
{
    map<unsigned long, TaskAction>::iterator it;
    it = pendingTaskAction.find(taskActionID);
    assert(it != pendingTaskAction.end());

    TaskAction action = it->second;
    pendingTaskAction.erase(it);
    return action;
}

map<string, Task> TaskTracker::getRunningTasks()
{
    return this->runningTasks;
}

void TaskTracker::addRunningTask(string taskID, Task task)
{
    runningTasks.insert(pair<string, Task>(taskID, task));
}

MapDataAction TaskTracker::getPendingMapDataAction(unsigned long mapDataActionID)
{
    map<unsigned long, MapDataAction>::iterator it;
    it = pendingMapDataAction.find(mapDataActionID);
    assert(it != pendingMapDataAction.end());

    MapDataAction action = it->second;
    pendingMapDataAction.erase(it);
    return action;
}

void TaskTracker::updateRunningTask(string taskID, Task task)
{
    map<string, Task>::iterator taskIt = runningTasks.find(taskID);
    assert(taskIt != runningTasks.end());
    taskIt->second = task;
}

void dataArrive(unsigned long dataType, unsigned long dataRequestID, string hostIPAddr)
{
    string hostName = findHostName4IP(hostIPAddr);
    int i;
    for(i = 0; i < numTaskTrackers; i++) {
        if (taskTrackers[i].getHostName() == hostName)
            break;
    }
    assert(i < numTaskTrackers);

    if (dataType == MAPTASK) {
        TaskAction action = taskTrackers[i].getPendingTaskAction(dataRequestID);
        Task task;
        action.status.startTime = Simulator::Now().GetMilliSeconds();
        action.status.finishTime = Simulator::Now().GetMilliSeconds() + action.status.duration;
        task.updateTaskStatus(action.status);
        map<string, Task> runningTasks = taskTrackers[i].getRunningTasks();
        map<string, Task>::iterator taskIt = runningTasks.find(action.status.taskAttemptID);
        assert(taskIt == runningTasks.end());
        assert(task.getTaskStatus().type == MAPTASK);
        taskTrackers[i].setUsedMapSlots(taskTrackers[i].getUsedMapSlots() + 1);
        taskTrackers[i].addRunningTask(action.status.taskAttemptID, task);
    } else {
        MapDataAction action = taskTrackers[i].getPendingMapDataAction(dataRequestID);
        map<string, Task> runningTasks = taskTrackers[i].getRunningTasks();
        map<string, Task>::iterator taskIt = runningTasks.find(action.reduceTaskID);
        assert(taskIt != runningTasks.end());
        Task task = taskIt->second;
        TaskStatus status = task.getTaskStatus();
        status.mapDataCouter++;
        task.updateTaskStatus(status);
        taskTrackers[i].updateRunningTask(taskIt->first, task);
    }
}

void TaskTracker::handleHeartbeatResponse(HeartBeatResponse *response, long evtTime)
{
    assert(response->type == HBResponse);
    Task task;
    map<string, Task>::iterator taskIt;

    // handle TaskAction
    list<TaskAction>::iterator actionIt = response->taskActions.begin();
    while(actionIt != response->taskActions.end()) {
        assert(actionIt->type != NO_ACTION);
        switch(actionIt->type) {
            case LAUNCH_TASK:
                if (!actionIt->status.isRemote || actionIt->status.type == REDUCETASK) {    // todo modify for reduce task
                    actionIt->status.startTime = evtTime;
                    actionIt->status.finishTime = evtTime + actionIt->status.duration;
                    task.updateTaskStatus(actionIt->status);
                    taskIt = runningTasks.find(actionIt->status.taskAttemptID);
                    assert(taskIt == runningTasks.end());
                    if (task.getTaskStatus().type == MAPTASK)
                        this->usedMapSlots++;
                    else
                        this->usedReduceSlots++;
                    runningTasks.insert(pair<string, Task>(actionIt->status.taskAttemptID, task));
                } else {
                    pendingTaskAction.insert(pair<unsigned long, TaskAction>(pendingTaskActionID, *actionIt));
                    fetchRawData(findIPAddr4Host(actionIt->status.dataSource),
                                 findIPAddr4Host(this->hostName),
                                 actionIt->status.dataSize,
                                 MAPTASK, pendingTaskActionID, MilliSeconds(evtTime));
                    pendingTaskActionID++;
                }
                break;
            case KILL_TASK:
                taskIt = runningTasks.find(actionIt->status.taskAttemptID);
                assert(taskIt != runningTasks.end());
                actionIt->status.isSucceeded = false;
                actionIt->status.runState = KILLED;
                actionIt->status.finishTime = evtTime;
                task.updateTaskStatus(actionIt->status);
                runningTasks.erase(taskIt);
                if (task.getTaskStatus().type == MAPTASK)
                    this->usedMapSlots--;
                else
                    this->usedReduceSlots--;
                killedTasks.insert(pair<string, Task>(actionIt->status.taskAttemptID, task));
                break;
            case START_REDUCEPHASE:
                // todo consider data transfer time from all map tasks

                taskIt = runningTasks.find(actionIt->status.taskAttemptID);
                assert(taskIt != runningTasks.end() && actionIt->status.runPhase == REDUCE);
                task = taskIt->second;
                task.updateTaskStatus(actionIt->status);
                taskIt->second = task;
                break;
            default:
                cout<<"Error in handleHeartbeatResponse().\n";
                break;
        }
        actionIt++;
    }

    // handle MapDataAction
    for(size_t i = 0; i < response->mapDataActions.size(); i++) {
        taskIt = runningTasks.find(response->mapDataActions[i].reduceTaskID);
        assert(taskIt != runningTasks.end());
        pendingMapDataAction.insert(pair<unsigned long, MapDataAction>(pendingMapDataActionID, response->mapDataActions[i]));
        fetchMapData(findIPAddr4Host(response->mapDataActions[i].dataSource),
                     findIPAddr4Host(this->hostName),
                     response->mapDataActions[i].dataSize,
                     REDUCETASK, pendingMapDataActionID, MilliSeconds(evtTime));
        pendingMapDataActionID++;
    }
}

HeartBeatReport TaskTracker::getReport()
{
    assert(!reportQueue.empty());
    HeartBeatReport report = reportQueue.front();
    reportQueue.pop();
    return report;
}

void TaskTracker::addResponse(HeartBeatResponse response)
{
    responseQueue.push(response);
}

HeartBeatResponse TaskTracker::getResponse()
{
    assert(!responseQueue.empty());
    HeartBeatResponse response = responseQueue.front();
    responseQueue.pop();
    return response;
}

size_t reportArrive(string hostIPAddr)
{
    string hostName = findHostName4IP(hostIPAddr);
    int i;
    for(i = 0; i < numTaskTrackers; i++) {
        if (taskTrackers[i].getHostName() == hostName)
            break;
    }
    assert(i < numTaskTrackers);

    HeartBeatReport report = taskTrackers[i].getReport();
    dumpHeartBeatReport(report);
    JobTracker *jobTracker = getJobTracker();
    HeartBeatResponse response = jobTracker->processHeartbeat(report, Simulator::Now().GetMilliSeconds());
    taskTrackers[i].addResponse(response);
    return getHBResponseSize(response);
}

void responseArrive(string hostIPAddr)
{
    string hostName = findHostName4IP(hostIPAddr);
    int i;
    for(i = 0; i < numTaskTrackers; i++) {
        if (taskTrackers[i].getHostName() == hostName)
            break;
    }
    assert(i < numTaskTrackers);

    HeartBeatResponse response = taskTrackers[i].getResponse();
    dumpHeartBeatResponse(response);
    taskTrackers[i].handleHeartbeatResponse(&response, Simulator::Now().GetMilliSeconds());

    // add next HeartBeat event from this task tracker to the EventQueue
    long timeStamp = Simulator::Now().GetMilliSeconds() + nextHeartbeatInterval;
    HEvent evt(&taskTrackers[i], EVT_HBReport, timeStamp);
    Simulator::Schedule(Seconds((double)timeStamp/1000.0), &hadoopEventCallback, evt);
}

void TaskTracker::sendHeartbeat(long evtTime)
{
    cout<<getHostName()<<" sendHeartbeat "<<evtTime<<endl;
    HeartBeatReport report;
    report.type = HBReport;
    report.hostName = this->hostName;
    report.taskStatus = collectTaskStatus(evtTime);
    report.numAvailMapSlots = MaxMapSlots - getUsedMapSlots();
    report.numAvailReduceSlots = MaxReduceSlots - getUsedReduceSlots();
    reportQueue.push(report);
    transferHeartBeat(findIPAddr4Host(getHostName()), getHBReportSize(report), MilliSeconds(evtTime));
    setLastReportTime(evtTime);
}

void TaskTracker::completeMapTask(long evtTime)
{
    cout<<getHostName()<<" completeTask "<<evtTime<<endl;
}

void TaskTracker::handleNewEvent(long timestamp, EvtType type)
{
//    HEvent evt = getEventFromEvtQueue();
    switch(type) {
        case EVT_HBReport:
            sendHeartbeat(timestamp);
            break;
//        case EVT_MapTaskDone:
//            completeMapTask(evt.getTimeStamp());
//            break;
        default:
            cout<<"Unhandled Event for TaskTracker\n";
    }
}

void dumpTaskTrackers()
{
    for(int i = 0; i < numTaskTrackers; i++) {
        cout<<"No. "<<i<<", Name="<<taskTrackers[i].getHostName()<<", ";
        cout<<"usedMapSlots="<<taskTrackers[i].getUsedMapSlots()<<", ";
        cout<<"usedReduceSlots="<<taskTrackers[i].getUsedReduceSlots()<<endl;
    }
}

/* Start task trackers, and return time stamp by which the entire cluster is booted up */
long initTaskTrackers(long startTime)
{
    vector<MachineNode> nodes = getClusterNodes();
    numTaskTrackers = (int)nodes.size();
    nextHeartbeatInterval = max((long)(1000 * ceil((double)numTaskTrackers / CLUSTER_INCREMENT)), HEARTBEAT_INTERVAL_MIN);

    taskTrackers = new TaskTracker[numTaskTrackers];
    int index = 0;
    for(vector<MachineNode>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        taskTrackers[index].setHostName(it->getHostName());

        // add first HeartBeat event from all task trackers to the EventQueue
        long timeStamp = startTime + rand() % clusterStartupDuration;
        HEvent evt(&taskTrackers[index], EVT_HBReport, timeStamp);
        Simulator::Schedule(Seconds((double)timeStamp/1000.0), &hadoopEventCallback, evt);
//        addEventToEvtQueue(evt);
        index++;
    }
    return startTime + clusterStartupDuration + nextHeartbeatInterval;
}

void killTaskTrackers()
{
    delete []taskTrackers;
}

