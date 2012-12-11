<?php

//extract an array of host names from cluster topo
function clustertopo_hosts($json) {
	$a = array();
	if (is_array($json['children'])) {
		foreach ($json['children'] as $child) {
			$a = array_merge($a, clustertopo_hosts($child));
		}
	} else {
		$a[] = $json['name'];
	}
	return $a;
}

?>
