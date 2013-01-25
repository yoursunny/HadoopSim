#include <iostream>
#include "HeartBeat.h"
using namespace std;

size_t getHBReportSize(HeartBeatReport &report)
{
    size_t size = 0;
    size += sizeof(report.type) + sizeof(report.hostName);
    size += report.taskStatus.size() * sizeof(TaskStatus);
    size += sizeof(long) * 2;
    return size;
}

size_t getHBResponseSize(HeartBeatResponse &response)
{
    size_t size = 0;
    size += sizeof(response.type);
    size += response.taskActions.size() * sizeof(TaskAction);
    size += response.mapDataActions.size() * sizeof(MapDataAction);
    return size;
}

void dumpHeartBeatReport(HeartBeatReport &report)
{
    cout<<"******HeartBeatReport******\n";
    cout<<"type = "<<report.type<<endl;
    cout<<"hostName = "<<report.hostName<<endl;
    int i = 0;
    for(list<TaskStatus>::iterator it = report.taskStatus.begin(); it != report.taskStatus.end(); it++) {
        cout<<i<<":\n";
        dumpTaskStatus(*it);
        cout<<"-----------\n";
        i++;
    }
    cout<<"numAvailMapSlots = "<<report.numAvailMapSlots<<endl;
    cout<<"numAvailReduceSlots = "<<report.numAvailReduceSlots<<endl<<endl;
}

void dumpHeartBeatResponse(HeartBeatResponse &response)
{
    cout<<"******HeartBeatResponse******\n";
    cout<<"type = "<<response.type<<endl;
    int i = 0;
    for(list<TaskAction>::iterator it = response.taskActions.begin(); it != response.taskActions.end(); it++) {
        cout<<i<<":\n";
        dumpTaskAction(*it);
        cout<<"-----------\n";
        i++;
    }

    if (response.mapDataActions.empty()) {
        cout<<endl;
        return;
    }
    cout<<"MapDataActions = "<<endl;
    for(size_t j = 0; j < response.mapDataActions.size(); j++) {
        cout<<"\t"<<j<<":\n";
        cout<<"\treduceTaskID = "<<response.mapDataActions[j].reduceTaskID<<endl;
        cout<<"\tdataSource = "<<response.mapDataActions[j].dataSource<<endl;
        cout<<"\tdataSize = "<<response.mapDataActions[j].dataSize<<endl;
    }
    cout<<endl;
}
