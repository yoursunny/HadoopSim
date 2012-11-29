/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef STATUS_H
#define STATUS_H

class Status {
public:
    enum RuntimeStatus {
        NotStarted = 1,
        Running = 2,
        Pending = 3,
        Completed = 4
    };

    enum ErrorCode {
        TooFewParameters = -1,
        WrongParameters = -2,
        NonExistentFile = -3
    };
};

#endif

