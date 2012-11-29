/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOB_H
#define JOB_H

#include <map>
#include <list>
#include <vector>
#include "Split.h"
#include "Task.h"
#include "JobTaskStory.h"

// what state is the job in?
typedef enum JobState {
    JOBRUNNING,
    JOBSUCCEEDED,
    JOBFAILED,
    JOBPREP,
    JOBKILLED
}JobState;

class Job {
public:
    Job(string jobID, int numMap, int numReduce, long submitTime);
    void initMapTasks(vector<TaskStory> mapTasks);
    void initReduceTasks(vector<TaskStory> reduceTasks);
    JobState getState();
    void setState(JobState state);
    bool isSucceeded();
    bool isAllMapsDone();
    bool isAllReducesDone();
    bool isWaitingMapLeft();
    bool isWaitingReduceLeft();
    ActionType updateTaskStatus(TaskStatus &taskStatus);
    void moveWaitingMapToRunning(bool isRemote, string trackerName, string taskID);
    void moveWaitingReduceToRunning(string trackerName, string taskID);
    void setStartTime(long startTime);
    void setEndTime(long endTime);
    map<string, Task> getWaitingMaps();
    map<string, Task> getWaitingReduces();
    map<string, Task> getCompletedMaps();
    map<string, Task> getCompletedReduces();
    bool canScheduleReduce();
    long getStarTime();
    long getEndTime();
    string getJobID();
    bool isFirstMap();
private:
    string jobID;
    JobState state;
    map<string, Split> splitSpace;      // block location space
    long numMap;
    long numReduce;
    long submitTime;
    long startTime;
    long endTime;
    long completedMapsForReduceStart;

    map<string, Task> waitingMaps;
    map<string, Task> remoteRunningMaps;
    map<string, Task> localRunningMaps;
    map<string, Task> killedMaps;
    map<string, Task> completedMaps;

    map<string, Task> waitingReduces;
    map<string, Task> runningReduces;
    map<string, Task> killedReduces;
    map<string, Task> completedReduces;
};

#endif

