# Hadoop simulator

This program requires ns-3.16. This directory should be placed in `ns-allinone-3.16/ns-3.16/examples/HadoopSim`.
ns-3 needs a patch to compile in C++ 11. Run `./ns3patch.sh` once to apply the patch.

## Running

Run HadoopSim
`./waf --run "HadoopSim 0 0 0 examples/HadoopSim/bench-trace/star.nettopo examples/HadoopSim/bench-trace/bayes/Trace 11 0"`

Run HadoopSim with debug output
`./waf --run "HadoopSim 0 0 0 examples/HadoopSim/bench-trace/star.nettopo examples/HadoopSim/bench-trace/bayes/Trace 11 1 examples/HadoopSim/debug-output/"`

Debug HadoopSim using GDB
`./waf --run "HadoopSim" --command-template="gdb --args %s 0 0 0 examples/HadoopSim/bench-trace/star.nettopo examples/HadoopSim/bench-trace/bayes/Trace 11 0"`

Run NetSim unittest
`./waf --run HadoopNetSimUnitTest`

