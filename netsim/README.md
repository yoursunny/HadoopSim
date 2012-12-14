# network simulation for HadoopSim

## steps to use

1. generate .nettopo from HiBench topology using a `topogen/` script
1. load .nettopo with `Topology::Load`
1. build topology in ns3 with `NetSim::BuildTopology`
1. install apps in ns3 nodes with `NetSim::InstallApps`
1. register the `NetSim::set_ready_cb` callback;
   transmit functions are available after this callback
1. start ns3 with `ns3::Simulator::Run`
1. transmit messages with `NetSim::NameRequest` `NetSim::NameResponse` `NetSim::DataRequest` `NetSim::DataResponse`, register a callback when a message arrives at destination
1. read link statistics from `NetSim::GetLinkStat`
1. stop ns3 with `ns3::Simulator::Stop`
1. clean up with `ns3::Simulator::Destroy`


