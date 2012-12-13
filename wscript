def build(bld):
  netsim_source = {
    'json/block_allocator.cpp',
    'json/json.cpp',
    'netsim/topology.cc',
    'netsim/msginfo.cc',
    'netsim/msgtransport.cc',
    'netsim/nameclient.cc',
    'netsim/nameserver.cc',
    'netsim/dataclient.cc',
    'netsim/dataserver.cc',
    'netsim/netsim.cc'
  }
  netsim_dependency = {
    'point-to-point',
    'internet',
    'applications'
  }
  
  obj = bld.create_ns3_program('HadoopSim', ['core', 'point-to-point', 'csma', 'internet', 'config-store', 'tools', 'applications'])
  obj.source = ['HadoopSim.cpp',
    'json/block_allocator.cpp',
    'json/json.cpp',
    'Cluster.cpp',
    'TopologyReader.cpp',
    'HEvent.cpp',
    'EventQueue.cpp',
    'HeartBeat.cpp',
    'JobTracker.cpp',
    'TaskTracker.cpp',
    'Split.cpp',
    'TaskTrackerStatus.cpp',
    'JobClient.cpp',
    'TraceReader.cpp',
    'TraceAnalyzer.cpp',
    'TraceLogger.cpp',
    'ResultGenerator.cpp',
    'DataLocalityScheduler.cpp',
    'FIFOScheduler.cpp',
    'Task.cpp',
    'Job.cpp',
    'ns3/Ns3Topo.cpp',
    'ns3/Ns3NameNode.cpp',
    'ns3/Ns3DataNode.cpp']

  obj = bld.create_ns3_program('HadoopNetSimUnitTest', netsim_dependency)
  obj.source = netsim_source | {
    'gtest/gtest_main.cc',
    'gtest/gtest.cc',
    'netsim/topology_test.cc',
    'netsim/msgtransport_test.cc',
    'netsim/netsim_test.cc'
  }

