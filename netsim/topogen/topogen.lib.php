<?php
class TopoGen {
	private $last_ip;
	private $last_linkid;
	public $type;
	public $nodes;
	public $links;

	function __construct($topotype) {
		$this->last_ip = ip2long('10.0.0.0');
		$this->last_linkid = 0;
		$this->type = $topotype;
		$this->nodes = array();
		$this->links = array();
	}
	public function ToJSON() {
		return array(
			'version'=>1,
			'type'=>$this->type,
			'nodes'=>$this->nodes,
			'links'=>$this->links
		);
	}

	public function AddHost($name) {
		if (array_key_exists($name,$this->nodes)) die(sprintf("TopoGen::AddHost duplicate %s\n",$name));
		$this->nodes[$name] = array(
			'type'=>'host',
			'ip'=>long2ip(++$this->last_ip),
			'devices'=>array()
		);
	}
	public function AddSwitch($name) {
		if (array_key_exists($name,$this->nodes)) die(sprintf("TopoGen::AddSwitch duplicate %s\n",$name));
		$this->nodes[$name] = array(
			'type'=>'switch',
			'devices'=>array()
		);
	}
	protected function SelectLinkType($node1,$node2) {
		if ($node1['type']=='switch' && $node2['type']=='switch') return 'eth10G';
		else return 'eth1G';
	}
	private function AddDevice(&$node) {
		$n = count($node['devices']);
		$ifname = sprintf('eth%d',$n);
		$node['devices'][] = $ifname;
		return $ifname;
	}
	public function AddLink($end1,$end2) {
		if (!array_key_exists($end1,$this->nodes) || !array_key_exists($end2,$this->nodes))  die(sprintf("TopoGen::AddLink missing %s %s\n",$end1,$end2));
		$link = array(
			'node1'=>$end1,
			'port1'=>$this->AddDevice($this->nodes[$end1]),
			'node2'=>$end2,
			'port2'=>$this->AddDevice($this->nodes[$end2]),
			'type'=>$this->SelectLinkType($this->nodes[$end1],$this->nodes[$end2])
		);
		$this->links[++$this->last_linkid] = $link;
	}
};

?>
