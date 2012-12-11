<?php
/*
rack-row topology

<nhost_rack> hosts connect to 1 top-of-rack switch
<nrack_row> top-of-rack switches connect to 2 end-of-row switches
all end-of-row switches connect to 2 core switches

*/

function topogen_rackrow($g,$hostlist,$args) {
	if (count($args)!=2) {
		die("rackrow args: ... nhost_rack nrack_row\n");
	}
	$nhost_rack = intval($args[0]);
	$nrack_row = intval($args[1]);
	if ($nhost_rack<1 || $nrack_row<1) die("rackrow args: must >0");
	$g->AddSwitch('coreA');
	$g->AddSwitch('coreB');
	$nrack = ceil(count($hostlist) / $nhost_rack);
	$nrow = ceil($nrack / $nrack_row);
	for ($i=0;$i<$nrow;++$i) {
		foreach (array(sprintf('row%dA',$i),sprintf('row%dB',$i)) as $swname) {
			$g->AddSwitch($swname);
			$g->AddLink('coreA',$swname);
			$g->AddLink('coreB',$swname);
		}
	}
	for ($i=0;$i<$nrack;++$i) {
		$row = floor($i/$nrack_row);
		$swname = sprintf('rack%d',$i);
		$g->AddSwitch($swname);
		$g->AddLink(sprintf('row%dA',$row),$swname);
		$g->AddLink(sprintf('row%dB',$row),$swname);
	}
	foreach ($hostlist as $i=>$hostname) {
		$rack = floor($i/$nhost_rack);
		$g->AddHost($hostname);
		$g->AddLink(sprintf('rack%d',$rack),$hostname);
	}
}

?>
