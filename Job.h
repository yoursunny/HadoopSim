#ifndef JOB_H
#define JOB_H

#include <map>
#include <string>
#include <vector>
#include "JobTaskStory.h"
#include "Split.h"
#include "Task.h"

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
    Job() { }
    Job(std::string jobID, int numMap, int numReduce, long submitTime);
    void initMapTasks(std::vector<TaskStory> mapTasks, long scaledMapCPUTime, long customMapNum);
    void initReduceTasks(std::vector<TaskStory> reduceTasks, long scaledDownRatioForReduce, long customReduceNum);
    JobState getState();
    void setState(JobState state);
    bool isSucceeded();
    bool isAllMapsDone();
    bool isAllReducesDone();
    bool isWaitingMapLeft();
    bool isWaitingReduceLeft();
    ActionType updateTaskStatus(TaskStatus &taskStatus);
    void moveWaitingMapToRunning(bool isRemote, std::string trackerName, std::string dataSource, std::string taskID);
    void moveWaitingReduceToRunning(std::string trackerName, std::string taskID);
    void setStartTime(long startTime);
    void setEndTime(long endTime);
    std::map<std::string, Task> getWaitingMaps();
    std::map<std::string, Task> getWaitingReduces();
    std::map<std::string, Task> getCompletedMaps();
    std::map<std::string, Task> getCompletedReduces();
    std::map<std::string, Task> getRemoteRunningMaps();
    std::map<std::string, Task> getLocalRunningMaps();
    std::map<std::string, Task> getRunningReduces();
    bool canScheduleReduce();
    long getSubmitTime();
    long getStarTime();
    long getEndTime();
    std::string getJobID();
    bool isFirstMap();
    long getNumMap();
    long getNumReduce();
    void removeMapDataSource();     // reduce num by one
private:
    std::string jobID;
    JobState state;
    std::map<std::string, Split> splitSpace;      // block location space
    long numMapDataSource;
    long numMap;
    long numReduce;
    long submitTime;
    long startTime;
    long endTime;
    long completedMapsForReduceStart;
    // map tasks
    std::map<std::string, Task> waitingMaps;
    std::map<std::string, Task> remoteRunningMaps;
    std::map<std::string, Task> localRunningMaps;
    std::map<std::string, Task> killedMaps;
    std::map<std::string, Task> completedMaps;
    // reduce tasks
    std::map<std::string, Task> waitingReduces;
    std::map<std::string, Task> runningReduces;
    std::map<std::string, Task> killedReduces;
    std::map<std::string, Task> completedReduces;
};

#endif // JOB_H
