#include <assert.h>
#include <iostream>
#include "Task.h"
using namespace std;

Task::Task(string jobID, string taskID, Type type, bool isRemote, long startTime, long duration, long inputSize, long outputSize)
{
    this->status.jobID = jobID;
    this->status.taskAttemptID = taskID;
    this->status.type = type;
    this->status.isRemote = isRemote;
    this->status.isSucceeded = false;
    this->status.dataSize = inputSize;
    this->status.mapDataCouter = 0;
    this->status.progress = 0.0;
    assert(type == MAPTASK || type == REDUCETASK);
    if (type == MAPTASK)
        this->status.runPhase = MAP;
    else
        this->status.runPhase = SHUFFLE;
    this->status.runState = RUNNING;
    this->status.startTime = startTime;
    this->status.finishTime = startTime + duration; // pre-set finishtime, but it varies in simulation runtime.
    this->status.duration = duration;
    this->status.outputSize = outputSize;
}

void Task::setTaskTrackerName(string taskTracker)
{
    this->status.taskTracker = taskTracker;
}

string Task::getTaskTrackerName()
{
    return this->status.taskTracker;
}

TaskStatus Task::getTaskStatus()
{
    return this->status;
}

bool Task::isSucceeded()
{
    return this->status.isSucceeded;
}

void Task::updateTaskStatus(bool isRemote, string trackerName, string dataSource)
{
    this->status.taskTracker = trackerName;
    this->status.isRemote = isRemote;
    this->status.dataSource = dataSource;
}

void Task::updateTaskStatus(TaskStatus status)
{
    this->status = status;
}

void Task::setTaskStatus(double progress, long now)
{
    double EPSILON = 0.000001;
    this->status.progress = progress;
    // if task is complete
    if (progress - 1.0 >= -EPSILON && progress - 1.0 <= EPSILON) {
        //assert(now >= this->status.finishTime);
        this->status.runState = SUCCEEDED;
        this->status.isSucceeded = true;
    }
}

void dumpTaskStatus(TaskStatus &status)
{
    cout<<"taskTracker = "<<status.taskTracker<<endl;
    cout<<"jobID = "<<status.jobID<<endl;
    cout<<"taskAttemptID = "<<status.taskAttemptID<<endl;
    cout<<"type = "<<status.type<<endl;
    cout<<"isRemote = "<<status.isRemote<<endl;
    cout<<"isSucceeded = "<<status.isSucceeded<<endl;
    cout<<"dataSource = "<<status.dataSource<<endl;
    cout<<"dataSize = "<<status.dataSize<<endl;
    cout<<"mapDataCouter = "<<status.mapDataCouter<<endl;
    cout<<"progress = "<<status.progress<<endl;
    cout<<"runPhase = "<<status.runPhase<<endl;
    cout<<"runState = "<<status.runState<<endl;
    cout<<"startTime = "<<status.startTime<<endl;
    cout<<"finishTime = "<<status.finishTime<<endl;
    cout<<"duration = "<<status.duration<<endl;
    cout<<"outputSize = "<<status.outputSize<<endl;
}

void dumpTaskAction(TaskAction &action)
{
    cout<<"ActionType = "<<action.type<<endl;
    dumpTaskStatus(action.status);
}
