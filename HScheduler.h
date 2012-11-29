/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef HSCHEDULER_H
#define HSCHEDULER_H

#include <string>
#include <list>
#include "Task.h"
using namespace std;

class HScheduler {
public:
    HScheduler(string name): schedName(name) { }
    string getSchedName() { return schedName; }
    virtual list<TaskAction> assignTasks(string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now) = 0;
private:
    string schedName;
};

class DataLocalityScheduler: public HScheduler {
public:
    DataLocalityScheduler(string name): HScheduler(name) { }
    list<TaskAction> assignTasks(string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

class FIFOScheduler: public HScheduler {
public:
    FIFOScheduler(string name): HScheduler(name) { }
    list<TaskAction> assignTasks(string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

#endif
