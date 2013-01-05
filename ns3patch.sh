#!/bin/bash
if [ -f .ns3patched ]
then
	echo ns-3 is already patched
	echo delete .ns3patched to redo patching
	exit
fi

# HadoopNetSim requires C++11
# ns-3.16 needs to be fixed to compile in C++11
# the only change necessary is replacing std::make_pair<A,B>() with std::make_pair()

cd ../../
./waf clean
patch -p1 < examples/HadoopSim/ns-3.16_c11.patch
#CXXFLAGS='--std=gnu++0x -g -I../examples/HadoopSim' ./waf configure --enable-examples --with-openflow=../openflow
CXXFLAGS='--std=gnu++0x -g -I../examples/HadoopSim' ./waf configure --enable-examples
./waf
cd examples/HadoopSim
touch .ns3patched

