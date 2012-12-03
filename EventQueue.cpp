/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <limits.h>
#include "EventQueue.h"
#include "JobClient.h"
#include "TaskTracker.h"
#include "TraceReader.h"
#include "ns3/Ns3.h"
using namespace ns3;
using namespace std;

/* Sim Variables */
static long simTime = 0;    //milliseconds
static long finishTime = LONG_MAX;  //6934100;

void hadoopEventCallback(HEvent evt)
{
    simTime = evt.getTimeStamp();
    if (simTime >= finishTime) {
        Simulator::Stop();
    } else {
        // Dispatch and deal with the event
        evt.getListener()->handleNewEvent(simTime, evt.getType());
        if (isAllJobsDone())
            Simulator::Stop();
    }
}
