# Hadoop simulator

This program requires ns-3.15.
This directory should be placed in `ns-3.15/examples/HadoopSim`.

How to run HadoopSim? Below is an example.
Enter `ns-3.15/examples/HadoopSim` directory, run command:
./waf --run "HadoopSim 0 0 /home/hduser/ns3/ns-allinone-3.15/ns-3.15/examples/HadoopSim-NS3/topo.json /home/hduser/ns3/ns-allinone-3.15/ns-3.15/examples/HadoopSim-NS3/Trace 2 1"

How to run GDB to debug HadoopSim? Below is an example.
Enter `ns-3.15/examples/HadoopSim` directory, run command:
./waf --run "HadoopSim" --command-template="gdb --args %s"
