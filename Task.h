/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef Task_H
#define Task_H

#include <list>
#include <string>
using namespace std;

// enumeration for reporting current phase of a task.
typedef enum Phase {
    STARTING,
    MAP,
    SHUFFLE,
    SORT,
    REDUCE,
    CLEANUP
}Phase;

// what state is the task in?
typedef enum State {
    RUNNING,
    SUCCEEDED,
    FAILED,
    UNASSIGNED,
    KILLED,
    COMMIT_PENDING,
    FAILED_UNCLEAN,
    KILLED_UNCLEAN
}State;

typedef enum Type {
    MAPTASK,
    REDUCETASK
}Type;

typedef enum ActionType {
    NO_ACTION,
    LAUNCH_TASK,
    KILL_TASK,
    START_REDUCEPHASE,
}ActionType;

typedef struct TaskStatus {
    string taskTracker;
    string jobID;
    string taskAttemptID;
    Type type;
    bool isRemote;
    bool isSucceeded;
    string dataSource;
    long dataSize;   // bytes
    double progress;
    Phase runPhase;
    State runState;
    long startTime;
    long finishTime;
    long duration;
    long outputSize;
}TaskStatus;

typedef struct TaskAction {
    ActionType type;
    TaskStatus status;
}TaskAction;

class Task {
public:
    Task() { }
    Task(string jobID, string taskID, Type type, bool isRemote, long startTime, long duration, long outputSize);
    void setTaskTrackerName(string taskTracker);
    string getTaskTrackerName();
    TaskStatus getTaskStatus();
    bool isSucceeded();
//    void updateTaskStatus(list<TaskAction>::iterator action);
//    void updateTaskStatus(list<TaskStatus>::iterator status);
    void updateTaskStatus(bool isRemote, string trackerName);
    void updateTaskStatus(TaskStatus status);
    void setTaskStatus(double progress, long now);
private:
    TaskStatus status;
};

void dumpTaskStatus(TaskStatus &status);
void dumpTaskAction(TaskAction &action);

#endif

