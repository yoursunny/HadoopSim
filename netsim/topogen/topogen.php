<?php
//php5 topogen.php input-clustertopo.json output.nettopo topotype args
if (count($argv)<4) {
	die("Usage: topogen.php input-clustertopo.json output.nettopo topotype [args]\n");
}

require_once 'topogen.lib.php';
require_once 'clustertopo.func.php';

$input_file = $argv[1];
$input_json = json_decode(file_get_contents($input_file),TRUE);
$output_file = $argv[2];
$topotype = $argv[3];
if (!ctype_alnum($topotype)) die("topotype invalid\n");
require $topotype.'.topo.php';
$topotype_f = 'topogen_'.$topotype;

$g = new TopoGen($topotype);
$hostlist = clustertopo_hosts($input_json);
$args = array_slice($argv,4);
$topotype_f($g,$hostlist,$args);

$output_json = $g->toJSON();
file_put_contents($output_file,json_encode($output_json,JSON_PRETTY_PRINT));
?>
