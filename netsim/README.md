# network simulation for HadoopSim

## steps to use

1. generate .nettopo from HiBench topology using a `topogen/` script
2. load .nettopo with `Topology::Load`
3. build topology in ns3 with `NetSim::BuildTopology`
4. register the `NetSim::set_ready_cb` callback;
   transmit functions are available after this callback
5. start ns3 with `ns3::Simulator::Run`
6. transmit messages with `NetSim::NameRequest` `NetSim::NameResponse` `NetSim::DataRequest` `NetSim::DataResponse`, register a callback when a message arrives at destination
7. read link statistics from `NetSim::GetLinkStat`
8. clean up with `ns3::Simulator::Destroy`


