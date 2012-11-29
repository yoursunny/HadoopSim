/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBSTORY_H
#define JOBSTORY_H

#include <vector>
using namespace std;

typedef struct Ranking {
    double relativeRanking;
    long datum;
}Ranking;

typedef struct CDF {
    long maximum;
    long minimum;
    vector<Ranking> rankings;
    long numberValues;
}CDF;

typedef struct Location {
    string rack;
    string hostName;
}Location;

typedef struct Attempt {
    Location location;
    string hostName;
    long startTime;
    string result;
    long finishTime;
    string attemptID;
    long shuffleFinished;
    long sortFinished;
    long hdfsBytesRead;
    long hdfsBytesWritten;
    long fileBytesRead;
    long fileBytesWritten;
    long mapInputRecords;
    long mapOutputBytes;
    long mapOutputRecords;
    long combineInputRecords;
    long reduceInputGroups;
    long reduceInputRecords;
    long reduceShuffleBytes;
    long reduceOutputRecords;
    long spilledRecords;
    long mapInputBytes;
}Attempt;

typedef struct TaskStory {
    long startTime;
    vector<Attempt> attempts;
    long finishTime;
    vector<Location> preferredLocations;
    string taskType;
    string taskStatus;
    string taskID;
    long inputBytes;
    long inputRecords;
    long outputBytes;
    long outputRecords;
}TaskStory;

typedef struct JobStory {
    string priority;
    string jobID;
    string user;
    string jobName;
    vector<TaskStory> mapTasks;
    long finishTime;
    vector<TaskStory> reduceTasks;
    long submitTime;
    long launchTime;
    long totalMaps;
    long totalReduces;
    vector<TaskStory> otherTasks;
    long computonsPerMapInputByte;
    long computonsPerMapOutputByte;
    long computonsPerReduceInputByte;
    long computonsPerReduceOutputByte;
    long heapMegabytes;
    string outcome;
    string jobtype;
    string directDependantJobs;
    vector<CDF> successfulMapAttemptCDFs;
    vector<CDF> failedMapAttemptCDFs;
    vector<CDF> successfulReduceAttemptCDF;
    vector<CDF> failedReduceAttemptCDF;
    vector<double> mapperTriesToSucceed;
    double failedMapperFraction;
    long relativeTime;
    string queuetype;
    long clusterMapMB;
    long clusterReduceMB;
    long jobMapMB;
    long jobReduceMB;
}JobStory;

#endif
