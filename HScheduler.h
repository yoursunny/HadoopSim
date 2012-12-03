/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef HSCHEDULER_H
#define HSCHEDULER_H

#include <list>
#include <string>
#include "Task.h"

class HScheduler {
public:
    HScheduler(std::string name): schedName(name) { }
    std::string getSchedName() { return schedName; }
    virtual std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now) = 0;
private:
    std::string schedName;
};

class DataLocalityScheduler: public HScheduler {
public:
    DataLocalityScheduler(std::string name): HScheduler(name) { }
    std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

class FIFOScheduler: public HScheduler {
public:
    FIFOScheduler(std::string name): HScheduler(name) { }
    std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

#endif // HSCHEDULER_H
