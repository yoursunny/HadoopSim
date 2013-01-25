#ifndef HEARTBEAT_H
#define HEARTBEAT_H

#include <list>
#include <vector>
#include "Task.h"

typedef enum HeartBeatType {
    HBReport = 0,
    HBResponse = 1
}HeartBeatType;

typedef struct HeartBeatReport {
    HeartBeatType type;
    std::string hostName;
    std::list<TaskStatus> taskStatus;        // map tasks (list head) -----   reduct tasks (list tail)
    long numAvailMapSlots;
    long numAvailReduceSlots;
}HeartBeatReport;

typedef struct HeartBeatResponse {
    HeartBeatType type;
    std::list<TaskAction> taskActions;
    std::vector<MapDataAction> mapDataActions;
}HeartBeatResponse;

size_t getHBReportSize(HeartBeatReport &report);
size_t getHBResponseSize(HeartBeatResponse &response);
void dumpHeartBeatReport(HeartBeatReport &report);
void dumpHeartBeatResponse(HeartBeatResponse &response);

#endif // HEARTBEAT_H
