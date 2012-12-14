# Network Topology

## .nettopo file format

	{
	  "version":1,
	  "type":"star"
	  "nodes":{
		"host1":{
		  "type":"host",
		  "ip":"192.168.0.1",
		  "devices":["eth0"]
		},
		"host2":{
		  "type":"host",
		  "ip":"192.168.0.2",
		  "devices":["eth0"]
		},
		"sw1":{
		  "type":"switch",
		  "devices":["e0/0","e0/1"]
		}
	  },
	  "links":{
		"1":{"node1":"sw1","port1":"e0/0","node2":"host1","port2":"eth0","type":"eth1G"},
		"2":{"node1":"sw1","port1":"e0/1","node2":"host2","port2":"eth0","type":"eth1G"}
	  }
	}

## topology generators

Star topology
`topogen.php topo.json star.nettopo star`

Data Center rack-row topology
`topogen.php topo.json rackrow-6-4.nettopo rackrow 6 4`
(6 hosts per rack with 1 top-of-rack switch, 4 racks per row with 2 end-of-row switches, 2 core switches)

PHP 5.4 is required. HOWTO [install PHP 5.4 on Ubuntu 12.04](https://launchpad.net/~ondrej/+archive/php5)

