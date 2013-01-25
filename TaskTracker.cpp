#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include "EventQueue.h"
#include "JobTracker.h"
#include "TaskTracker.h"
using namespace HadoopNetSim;
using namespace std;

/* TaskTracker Variables */
static TaskTracker *taskTrackers;
static int numTaskTrackers = 0;
static const long clusterStartupDuration = 100 * 1000;     //milliseconds
static const long HEARTBEAT_INTERVAL_MIN = 3 * 1000;       //milliseconds
static const int CLUSTER_INCREMENT = 100;
static long nextHeartbeatInterval = 0;
// For some unknown reason, if a message is smaller than TCP segment size, TCP
// retransmission may not work. Therefore, all messages should be at least 9000 octets.
static const size_t DataRequestMsgSize = 9000;             // bytes

void TaskTracker::setHostName(string hostName)
{
    this->hostName = hostName;
}

const string TaskTracker::getHostName() const
{
    return this->hostName;
}

const long TaskTracker::getUsedMapSlots() const
{
    return this->usedMapSlots;
}

void TaskTracker::setUsedMapSlots(long slot)
{
    this->usedMapSlots = slot;
}

const long TaskTracker::getUsedReduceSlots() const
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
        //cout<<"it->second.getTaskTrackerName() = "<<it->second.getTaskTrackerName()<<endl;
        //cout<<"this->hostName = "<<this->hostName<<endl;
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
                assert(getUsedReduceSlots() > 0 && getUsedReduceSlots() < MaxReduceSlots + 1);
                setUsedReduceSlots(getUsedReduceSlots() - 1);
            }
            completedTasks.insert(pair<string, Task>(it->first, it->second));
            cout<<it->second.getTaskStatus().taskAttemptID<<" done "<<(it->second.getTaskStatus().isRemote ? "REMOTELY" : "LOCALLY")<<" on "<<it->second.getTaskStatus().taskTracker<<endl;
            runningTasks.erase(it++);
        }
        else
            it++;
    }
    return allTaskStatus;
}

void TaskTracker::addRunningTask(string taskID, Task task)
{
    runningTasks.insert(pair<string, Task>(taskID, task));
}

void TaskTracker::updateRunningTask(string taskID, Task task)
{
    map<string, Task>::iterator taskIt = runningTasks.find(taskID);
    assert(taskIt != runningTasks.end());
    taskIt->second = task;
}

void TaskTracker::hbReport(ns3::Ptr<MsgInfo> request_msg)
{
    assert(request_msg->type() == kMTNameRequest);
    assert(request_msg->src().compare(this->hostName) == 0);
    assert(request_msg->dst().compare(getJobTrackerName()) == 0);
    assert(reportMap.find(request_msg->id()) != reportMap.end());
    HeartBeatReport report = reportMap[request_msg->id()];
    reportMap.erase(request_msg->id());

//  dumpHeartBeatReport(report);
    JobTracker *jobTracker = getJobTracker();
    HeartBeatResponse response = jobTracker->processHeartbeat(report, ns3::Simulator::Now().GetMilliSeconds());

    MsgId id;
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);
    id = netsim->NameResponse(request_msg->dst(), request_msg->src(), getHBResponseSize(response), ns3::MakeCallback(&TaskTracker::hbResponse, this), this);
    assert(id != MsgId_invalid);
    assert(reportMap.find(id) == reportMap.end());
    responseMap[id] = response;
}

void TaskTracker::hbResponse(ns3::Ptr<MsgInfo> response_msg)
{
    assert(response_msg->type() == kMTNameResponse);
    assert(response_msg->src().compare(getJobTrackerName()) == 0);
    assert(response_msg->dst().compare(this->hostName) == 0);
    assert(responseMap.find(response_msg->id()) != responseMap.end());
    HeartBeatResponse response = responseMap[response_msg->id()];
    responseMap.erase(response_msg->id());

//  dumpHeartBeatResponse(response);
    handleHeartbeatResponse(&response, ns3::Simulator::Now().GetMilliSeconds());

    // add next HeartBeat event from this task tracker to the EventQueue
    HEvent evt(this, EVT_HBReport);
    ns3::Simulator::Schedule(ns3::Seconds((double)nextHeartbeatInterval/1000.0), &hadoopEventCallback, evt);
}

void TaskTracker::dataRequest(ns3::Ptr<MsgInfo> request_msg)
{
    assert(request_msg->type() == kMTDataRequest);
    assert(request_msg->src().compare(this->hostName) == 0);
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);

    if (pendingTaskAction.find(request_msg->id()) != pendingTaskAction.end()) {
        TaskAction action = pendingTaskAction[request_msg->id()];
        pendingTaskAction.erase(request_msg->id());
        assert(request_msg->dst().compare(action.status.dataSource) == 0);
        MsgId id = netsim->DataResponse(request_msg->id(), request_msg->dst(),
                                         request_msg->src(), action.status.dataSize,
                                         ns3::MakeCallback(&TaskTracker::dataResponse, this), this);
        assert(id != MsgId_invalid);
        assert(pendingTaskAction.find(id) == pendingTaskAction.end());
        pendingTaskAction[id] = action;
    } else if (pendingMapDataAction.find(request_msg->id()) != pendingMapDataAction.end()) {
        MapDataAction action = pendingMapDataAction[request_msg->id()];
        pendingMapDataAction.erase(request_msg->id());
        assert(request_msg->dst().compare(action.dataSource) == 0);
        MsgId id = netsim->DataResponse(request_msg->id(), request_msg->dst(),
                                         request_msg->src(), action.dataSize,
                                         ns3::MakeCallback(&TaskTracker::dataResponse, this), this);
        assert(id != MsgId_invalid);
        assert(pendingMapDataAction.find(id) == pendingMapDataAction.end());
        pendingMapDataAction[id] = action;
    } else {
        cout<<"Error in TaskTracker::dataRequest.\n";
        exit(1);
    }
}

void TaskTracker::dataResponse(ns3::Ptr<MsgInfo> response_msg)
{
    assert(response_msg->type() == kMTDataResponse);
    assert(response_msg->dst().compare(this->hostName) == 0);
    Task task;

    if (pendingTaskAction.find(response_msg->id()) != pendingTaskAction.end()) {
        TaskAction action = pendingTaskAction[response_msg->id()];
        pendingTaskAction.erase(response_msg->id());
        assert(action.status.type == MAPTASK);
        assert(response_msg->src().compare(action.status.dataSource) == 0);

        action.status.finishTime = ns3::Simulator::Now().GetMilliSeconds() + action.status.duration;
        task.updateTaskStatus(action.status);

        map<string, Task>::iterator taskIt = runningTasks.find(action.status.taskAttemptID);
        assert(taskIt == runningTasks.end());
        assert(task.getTaskStatus().type == MAPTASK);
        addRunningTask(action.status.taskAttemptID, task);
    } else if (pendingMapDataAction.find(response_msg->id()) != pendingMapDataAction.end()) {
        MapDataAction action = pendingMapDataAction[response_msg->id()];
        pendingMapDataAction.erase(response_msg->id());
        assert(response_msg->src().compare(action.dataSource) == 0);

        map<string, Task>::iterator taskIt = runningTasks.find(action.reduceTaskID);
        assert(taskIt != runningTasks.end());
        Task task = taskIt->second;
        TaskStatus status = task.getTaskStatus();
        status.finishTime = ns3::Simulator::Now().GetMilliSeconds() + status.duration;		// the last arrived MapData set the finishTime of this reduce task
        status.mapDataCouter++;
	    //cout<<"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ mapdata = "<<status.mapDataCouter<<endl;
        task.updateTaskStatus(status);
        updateRunningTask(taskIt->first, task);
    } else {
        cout<<"Error in TaskTracker::dataResponse.\n";
        exit(1);
    }
}

void TaskTracker::sendHeartbeat(long evtTime)
{
    //cout<<getHostName()<<" sendHeartbeat "<<evtTime<<endl;

    HeartBeatReport report;
    report.type = HBReport;
    report.hostName = this->hostName;
    report.taskStatus = collectTaskStatus(evtTime);
    report.numAvailMapSlots = MaxMapSlots - getUsedMapSlots();
    report.numAvailReduceSlots = MaxReduceSlots - getUsedReduceSlots();

    MsgId id;
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);
    id = netsim->NameRequest(this->hostName, getJobTrackerName(), getHBReportSize(report), ns3::MakeCallback(&TaskTracker::hbReport, this), this);
    assert(id != MsgId_invalid);
    assert(reportMap.find(id) == reportMap.end());

    reportMap[id] = report;
    setLastReportTime(evtTime);
}

void TaskTracker::makeDataRequest(MapDataAction action)
{
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);
    MsgId id = netsim->DataRequest(this->hostName, action.dataSource, DataRequestMsgSize, ns3::MakeCallback(&TaskTracker::dataRequest, this), this);
    assert(id != MsgId_invalid);
    assert(pendingMapDataAction.find(id) == pendingMapDataAction.end());
    pendingMapDataAction[id] = action;
}

void TaskTracker::handleHeartbeatResponse(HeartBeatResponse *response, long evtTime)
{
    assert(response->type == HBResponse);
    Task task;
    map<string, Task>::iterator taskIt;
    MsgId id;
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);

    // handle TaskAction
    list<TaskAction>::iterator actionIt = response->taskActions.begin();
    while(actionIt != response->taskActions.end()) {
        assert(actionIt->type != NO_ACTION);
        switch(actionIt->type) {
            case LAUNCH_TASK:
                if (!actionIt->status.isRemote || actionIt->status.type == REDUCETASK) {    // local Map task , Reduce task
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
                } else {                                                                    // remote Map task
                    TaskAction action = *actionIt;
                    assert(action.status.type == MAPTASK);
                    assert(action.status.isRemote == true);
                    action.status.startTime = evtTime;
                    this->usedMapSlots++;
                    //cout<<"remote map tasks, host = "<<this->hostName<<", data source =  "<<action.status.dataSource<<endl;
                    id = netsim->DataRequest(this->hostName, action.status.dataSource, DataRequestMsgSize, ns3::MakeCallback(&TaskTracker::dataRequest, this), this);
                    assert(id != MsgId_invalid);
                    assert(pendingTaskAction.find(id) == pendingTaskAction.end());
                    pendingTaskAction[id] = action;
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
                taskIt = runningTasks.find(actionIt->status.taskAttemptID);
                assert(taskIt != runningTasks.end() && actionIt->status.runPhase == REDUCE);
                task = taskIt->second;
                task.updateTaskStatus(actionIt->status);
                taskIt->second = task;
                //cout<<"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ reduce start\n";
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

        if (this->hostName.compare(response->mapDataActions[i].dataSource) == 0) {      // on the same node
            Task task = taskIt->second;
            TaskStatus status = task.getTaskStatus();
            status.mapDataCouter++;
            //cout<<"############################### mapdata = "<<status.mapDataCouter<<endl;
            //cout<<"^^^^^reduce tasks, host = "<<this->hostName<<", data source =  "<<response->mapDataActions[i].dataSource<<endl;
            task.updateTaskStatus(status);
            updateRunningTask(taskIt->first, task);
            continue;
        }

        assert(response->mapDataActions[i].dataSize > 0);
        //cout<<"reduce tasks, host = "<<this->hostName<<", data source =  "<<response->mapDataActions[i].dataSource<<", datasize ="<<response->mapDataActions[i].dataSize<<endl;
        //ns3::Simulator::Schedule(ns3::Seconds(0.01 * i), &TaskTracker::makeDataRequest, this, response->mapDataActions[i]);
        makeDataRequest(response->mapDataActions[i]);
    }
}

void TaskTracker::handleNewEvent(EvtType type)
{
    long timestamp = ns3::Simulator::Now().GetMilliSeconds();

    switch(type) {
        case EVT_HBReport:
            sendHeartbeat(timestamp);
            break;
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

/* Start task trackers, and return relative time stamp by which the entire cluster is booted up */
long initTaskTrackers(void)
{
    vector<MachineNode> nodes = getClusterSlaveNodes();
    numTaskTrackers = (int)nodes.size();
    nextHeartbeatInterval = max((long)(1000 * ceil((double)numTaskTrackers / CLUSTER_INCREMENT)), HEARTBEAT_INTERVAL_MIN);

    taskTrackers = new TaskTracker[numTaskTrackers];
    int index = 0;
    for(vector<MachineNode>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        taskTrackers[index].setHostName(it->getHostName());

        // add first HeartBeat event from all task trackers to the EventQueue
        long timeStamp = rand() % clusterStartupDuration;
        HEvent evt(&taskTrackers[index], EVT_HBReport);
        ns3::Simulator::Schedule(ns3::Seconds((double)timeStamp/1000.0), &hadoopEventCallback, evt);
        index++;
    }
    return clusterStartupDuration + nextHeartbeatInterval;
}

void killTaskTrackers()
{
    delete []taskTrackers;
}
