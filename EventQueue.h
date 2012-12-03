/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef EVENTQUEUE_H
#define EVENTQUEUE_H

#include "HEvent.h"

void hadoopEventCallback(HEvent evt);

#endif // EVENTQUEUE_H
