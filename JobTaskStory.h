#ifndef JOBSTORY_H
#define JOBSTORY_H

#include <string>
#include <vector>

typedef struct Ranking {
    long datum;
    double relativeRanking;
}Ranking;

typedef struct CDF {
    long maximum;
    long minimum;
    long numberValues;
    std::vector<Ranking> rankings;
}CDF;

typedef struct Location {
    std::string rack;
    std::string hostName;
}Location;

typedef struct Attempt {
    std::string attemptID;
    long combineInputRecords;
    long fileBytesRead;
    long fileBytesWritten;
    long finishTime;
    long hdfsBytesRead;
    long hdfsBytesWritten;
    std::string hostName;
    Location location;
    long mapInputBytes;
    long mapInputRecords;
    long mapOutputBytes;
    long mapOutputRecords;
    long reduceInputGroups;
    long reduceInputRecords;
    long reduceOutputRecords;
    long reduceShuffleBytes;
    std::string result;
    long shuffleFinished;
    long sortFinished;
    long spilledRecords;
    long startTime;
}Attempt;

typedef struct TaskStory {
    std::vector<Attempt> attempts;
    long finishTime;
    long inputBytes;
    long inputRecords;
    long outputBytes;
    long outputRecords;
    std::vector<Location> preferredLocations;
    long startTime;
    std::string taskID;
    std::string taskStatus;
    std::string taskType;
}TaskStory;

typedef struct JobStory {
    long clusterMapMB;
    long clusterReduceMB;
    long computonsPerMapInputByte;
    long computonsPerMapOutputByte;
    long computonsPerReduceInputByte;
    long computonsPerReduceOutputByte;
    std::string directDependantJobs;
    std::vector<CDF> failedMapAttemptCDFs;
    double failedMapperFraction;
    std::vector<CDF> failedReduceAttemptCDF;
    long finishTime;
    long heapMegabytes;
    std::string jobID;
    long jobMapMB;
    std::string jobName;
    long jobReduceMB;
    std::string jobtype;
    long launchTime;
    std::vector<TaskStory> mapTasks;
    std::vector<double> mapperTriesToSucceed;
    std::vector<TaskStory> otherTasks;
    std::string outcome;
    std::string priority;
    std::string queuetype;
    std::vector<TaskStory> reduceTasks;
    long relativeTime;
    long submitTime;
    std::vector<CDF> successfulMapAttemptCDFs;
    std::vector<CDF> successfulReduceAttemptCDF;
    long totalMaps;
    long totalReduces;
    std::string user;
}JobStory;

#endif // JOBSTORY_H
