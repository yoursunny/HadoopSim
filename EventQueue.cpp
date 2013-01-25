#include <limits.h>
#include "EventQueue.h"
#include "JobClient.h"
#include "TaskTracker.h"
#include "TraceReader.h"
using namespace std;

/* Sim Variables */
static long simTime = 0;    //The timestamp of event (milliseconds)
static long finishTime = LONG_MAX;  //6934100;

void hadoopEventCallback(HEvent evt)
{
    simTime = ns3::Simulator::Now().GetMilliSeconds();
    if (simTime >= finishTime) {
        ns3::Simulator::Stop();
    } else {
        // Dispatch and deal with the event
        evt.getListener()->handleNewEvent(evt.getType());
        if (isAllJobsDone()) {
            cout<<"Hadoop Sim endTime = "<<simTime<<endl;
            ns3::Simulator::Stop();
        }
    }
}
