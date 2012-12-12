/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBSTORY_H
#define JOBSTORY_H

#include <string>
#include <vector>

typedef struct Ranking {
    long datum;
    double relativeRanking;
}Ranking;

typedef struct CDF {
    long minimum;
    long maximum;
    long numberValues;
    std::vector<Ranking> rankings;
}CDF;

typedef struct Location {
    std::string rack;
    std::string hostName;
}Location;

typedef struct Attempt {
    Location location;
    std::string hostName;
    long startTime;
    std::string result;
    long finishTime;
    std::string attemptID;
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
    std::vector<Attempt> attempts;
    long finishTime;
    std::vector<Location> preferredLocations;
    std::string taskType;
    std::string taskStatus;
    std::string taskID;
    long inputBytes;
    long inputRecords;
    long outputBytes;
    long outputRecords;
}TaskStory;

typedef struct JobStory {
    std::string priority;
    std::string jobID;
    std::string user;
    std::string jobName;
    long computonsPerMapInputByte;
    long computonsPerMapOutputByte;
    long computonsPerReduceInputByte;
    long computonsPerReduceOutputByte;
    long heapMegabytes;
    std::string outcome;
    std::string jobtype;
    std::string directDependantJobs;
    std::vector<CDF> successfulMapAttemptCDFs;
    std::vector<CDF> failedMapAttemptCDFs;
    std::vector<CDF> successfulReduceAttemptCDF;
    std::vector<CDF> failedReduceAttemptCDF;
    std::vector<double> mapperTriesToSucceed;
    double failedMapperFraction;
    long relativeTime;
    std::string queuetype;
    long clusterMapMB;
    long clusterReduceMB;
    long jobMapMB;
    long jobReduceMB;
    std::vector<TaskStory> mapTasks;
    long finishTime;
    std::vector<TaskStory> reduceTasks;
    long submitTime;
    long launchTime;
    long totalMaps;
    long totalReduces;
    std::vector<TaskStory> otherTasks;
}JobStory;

#endif // JOBSTORY_H
