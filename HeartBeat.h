/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef HEARTBEAT_H
#define HEARTBEAT_H

#include <list>
#include "Task.h"
using namespace std;

typedef enum HeartBeatType {
    HBReport = 0,
    HBResponse = 1
}HeartBeatType;

typedef struct HeartBeatReport {
    HeartBeatType type;
    string hostName;
    list<TaskStatus> taskStatus;        // map tasks (list head) -----   reduct tasks (list tail)
    long numAvailMapSlots;
    long numAvailReduceSlots;
}HeartBeatReport;

typedef struct HeartBeatResponse {
    HeartBeatType type;
    list<TaskAction> taskActions;
}HeartBeatResponse;

size_t getHBReportSize(HeartBeatReport &report);
size_t getHBResponseSize(HeartBeatResponse &response);
void dumpHeartBeatReport(HeartBeatReport &report);
void dumpHeartBeatResponse(HeartBeatResponse &response);

#endif

