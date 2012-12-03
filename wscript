def build(bld):
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

  obj = bld.create_ns3_program('HadoopNetSimUnitTest', ['csma','internet','applications'])
  obj.source = [
    'gtest/gtest_main.cc',
    'gtest/gtest.cc',
    'json/block_allocator.cpp',
    'json/json.cpp',
    'netsim/topology.cc',
    'netsim/topology_test.cc'
  ]

