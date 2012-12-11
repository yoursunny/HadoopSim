<?php

function topogen_star($g,$hostlist,$args) {
	$g->AddSwitch('hub');
	foreach ($hostlist as $hostname) {
		$g->AddHost($hostname);
		$g->AddLink('hub',$hostname);
	}
}

?>
