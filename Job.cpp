/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <iostream>
#include "Job.h"
#include "JobTracker.h"
using namespace std;

Job::Job(string jobID, int numMap, int numReduce, long submitTime)
{
    this->jobID = jobID;
    this->state = JOBRUNNING;
    this->numMapDataSource = numMap;
    this->numMap = numMap;
    this->numReduce = numReduce;
    this->submitTime = submitTime;
    this->completedMapsForReduceStart = (long)(numMap * 0.05);
    //cout<<"completedMapsForReduceStart = "<<completedMapsForReduceStart<<endl;
}

void Job::initMapTasks(vector<TaskStory> mapTasks)
{
    assert(this->numMap == (long)mapTasks.size());
    vector<TaskStory>::iterator taskStoryIt = mapTasks.begin();
    size_t i;
    while(taskStoryIt != mapTasks.end()) {
        for(i = 0; i < taskStoryIt->attempts.size(); i++) {
            if (taskStoryIt->attempts[i].result.compare("SUCCESS") == 0)
                break;
        }
        //assert(taskStoryIt->attempts[i].startTime >= taskStoryIt->startTime);
        //cout<<taskStoryIt->taskID<<endl;
        //assert(taskStoryIt->attempts[i].finishTime <= taskStoryIt->finishTime);
        long mapInputBytes = taskStoryIt->attempts[i].mapInputBytes;
        if (mapInputBytes < 0)
            mapInputBytes = 0;
        Task task(this->jobID, taskStoryIt->taskID, MAPTASK, false,
                    0, taskStoryIt->attempts[i].finishTime - taskStoryIt->attempts[i].startTime,
                    mapInputBytes, taskStoryIt->attempts[i].mapOutputBytes);
        waitingMaps.insert(pair<string, Task>(taskStoryIt->taskID, task));
        // update block mapping space
        vector<string> dataNodes;
        for(i = 0; i < taskStoryIt->preferredLocations.size(); i++) {
            dataNodes.push_back(taskStoryIt->preferredLocations[i].hostName);
        }
        Split split(taskStoryIt->taskID, dataNodes);
        splitSpace.insert(pair<string, Split>(taskStoryIt->taskID, split));

        // update global mapping
        JobTracker *jobTracker = getJobTracker();
        jobTracker->updateBlockNodeMapping(taskStoryIt->taskID, dataNodes);

        taskStoryIt++;
    }
}

void Job::initReduceTasks(vector<TaskStory> reduceTasks)
{
    assert(this->numReduce == (long)reduceTasks.size());
    vector<TaskStory>::iterator taskStoryIt = reduceTasks.begin();
    size_t i;
    while(taskStoryIt != reduceTasks.end()) {
        for(i = 0; i < taskStoryIt->attempts.size(); i++) {
            if (taskStoryIt->attempts[i].result.compare("SUCCESS") == 0)
                break;
        }
        //assert(taskStoryIt->attempts[i].startTime >= taskStoryIt->startTime);
        assert(taskStoryIt->attempts[i].finishTime <= taskStoryIt->finishTime);
        Task task(this->jobID, taskStoryIt->taskID, REDUCETASK, true,
                    0, taskStoryIt->attempts[i].finishTime - taskStoryIt->attempts[i].shuffleFinished,
                    0, 0);
        waitingReduces.insert(pair<string, Task>(taskStoryIt->taskID, task));
        taskStoryIt++;
    }
}

JobState Job::getState()
{
    return this->state;
}

void Job::setState(JobState state)
{
    this->state = state;
}

bool Job::isSucceeded()
{
    return this->state == JOBSUCCEEDED;
}

bool Job::isAllMapsDone()
{
    return this->numMap == (long)this->completedMaps.size();
}

bool Job::isAllReducesDone()
{
    return this->numReduce == (long)this->completedReduces.size();
}

bool Job::isWaitingMapLeft()
{
    return !this->waitingMaps.empty();
}

bool Job::isWaitingReduceLeft()
{
    return !this->waitingReduces.empty();
}

ActionType Job::updateTaskStatus(TaskStatus &taskStatus)
{
    map<string, Task>::iterator taskIt;
    Task task;

    if (taskStatus.type == MAPTASK) {
        // check if this task is already in the completedMaps. Yes, kill this task.
        taskIt = completedMaps.find(taskStatus.taskAttemptID);
        if (taskIt != completedMaps.end()) {

            return KILL_TASK;
        } else {
            if (taskStatus.isRemote) {
                taskIt = remoteRunningMaps.find(taskStatus.taskAttemptID);
                assert(taskIt != remoteRunningMaps.end());
            } else {
                taskIt = localRunningMaps.find(taskStatus.taskAttemptID);
                assert(taskIt != localRunningMaps.end());
            }
            task = taskIt->second;
            task.updateTaskStatus(taskStatus);
            taskIt->second = task;

            // if Succeeded, move the task to completedMaps
            if (taskIt->second.isSucceeded()) {
                completedMaps.insert(pair<string, Task>(taskIt->first, taskIt->second));
                if (taskStatus.isRemote) {
                    remoteRunningMaps.erase(taskIt);
                } else {
                    localRunningMaps.erase(taskIt);
                }

                // consider about Map tasks only generating data
                if (isAllMapsDone() && isAllReducesDone()) {
                    setState(JOBSUCCEEDED);
                    return NO_ACTION;
                }

                if (task.getTaskStatus().outputSize > 0)
                    return FETCH_MAPDATA;
                else
                    this->numMapDataSource--;   // reduce Map Data Sources
            }
        }
    } else {
        // check if this task is already in the completedReduces. Yes, kill this task.
        taskIt = completedReduces.find(taskStatus.taskAttemptID);
        if (taskIt != completedReduces.end()) {

            return KILL_TASK;
        } else {
            taskIt = runningReduces.find(taskStatus.taskAttemptID);
            assert(taskIt != runningReduces.end());
            task = taskIt->second;
            task.updateTaskStatus(taskStatus);
            taskIt->second = task;

            // if Succeeded, move the task to completedReduces
            if (taskIt->second.isSucceeded()) {
                completedReduces.insert(pair<string, Task>(taskIt->first, taskIt->second));
                runningReduces.erase(taskIt);
                if (isAllReducesDone()) {   // if all reduce tasks are done, then the entire job will be done
                    setState(JOBSUCCEEDED);
                }
            } else {
		cout<<"+++++++++++++++++++++++++taskStatus.mapDataCouter = "<<taskStatus.mapDataCouter<<endl;	
		cout<<"=========================this->numMapDataSource = "<<this->numMapDataSource<<endl;
                if (isAllMapsDone() && taskStatus.runPhase == SHUFFLE && taskStatus.mapDataCouter == this->numMapDataSource) {
                    taskStatus.runPhase = REDUCE;
                    return START_REDUCEPHASE;
                }
            }
        }
    }
    return NO_ACTION;
}

void Job::moveWaitingMapToRunning(bool isRemote, string trackerName, string dataSource, string taskID)
{
    map<string, Task>::iterator taskIt;
    Task task;
    taskIt = waitingMaps.find(taskID);
    assert(taskIt != waitingMaps.end());

    task = taskIt->second;
    task.updateTaskStatus(isRemote, trackerName, dataSource);
    taskIt->second = task;
    if (isRemote) {
        remoteRunningMaps.insert(pair<string, Task>(taskID, taskIt->second));
    } else {
        localRunningMaps.insert(pair<string, Task>(taskID, taskIt->second));
    }
    waitingMaps.erase(taskIt);
}

void Job::moveWaitingReduceToRunning(string trackerName, string taskID)
{
    map<string, Task>::iterator taskIt;
    Task task;
    taskIt = waitingReduces.find(taskID);
    assert(taskIt != waitingReduces.end());

    task = taskIt->second;
    task.updateTaskStatus(true, trackerName, "ALL");
    taskIt->second = task;
    runningReduces.insert(pair<string, Task>(taskID, taskIt->second));
    waitingReduces.erase(taskIt);
}

void Job::setStartTime(long startTime)
{
    this->startTime = startTime;
}

void Job::setEndTime(long endTime)
{
    this->endTime = endTime;
}

map<string, Task> Job::getWaitingMaps()
{
    return this->waitingMaps;
}

map<string, Task> Job::getWaitingReduces()
{
    return this->waitingReduces;
}

map<string, Task> Job::getCompletedMaps()
{
    return this->completedMaps;
}

map<string, Task> Job::getCompletedReduces()
{
    return this->completedReduces;
}

map<string, Task> Job::getRunningReduces()
{
    return this->runningReduces;
}

bool Job::canScheduleReduce()
{
    return completedMaps.size() >= (size_t)completedMapsForReduceStart;
}

long Job::getStarTime()
{
    return this->startTime;
}

long Job::getEndTime()
{
    return this->endTime;
}

string Job::getJobID()
{
    return this->jobID;
}

bool Job::isFirstMap()
{
    return remoteRunningMaps.empty() && localRunningMaps.empty() && completedMaps.empty() && killedMaps.empty();
}

long Job::getNumReduce()
{
    return this->numReduce;
}
