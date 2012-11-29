/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <stdio.h>
#include <assert.h>
#include <iostream>
#include "json/json.h"
#include "TopologyReader.h"
using namespace std;

/* TopologyReader Variables */
static ClusterTopo clusterTopo;
static vector<HadoopHost> hostTopo;

#define IDENT(n) for (int i = 0; i < n; ++i) printf("    ")
#define Next(n) ((n) = ((n)->next_sibling))

vector<HadoopHost> parseHost(json_value *value, string rackName)
{
    assert(value);
    vector<HadoopHost> hostSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        HadoopHost host;
        json_value *child = it->first_child;
        host.hostName.assign(child->string_value); Next(child);
        host.rackName.assign(rackName);
        hostSet.push_back(host);
        hostTopo.push_back(host);
    }
    return hostSet;
}

vector<HadoopRack> parseRack(json_value *value)
{
    assert(value);
    vector<HadoopRack> rackSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        HadoopRack rack;
        json_value *child = it->first_child;
        rack.rackName.assign(child->string_value); Next(child);
        rack.hostSet = parseHost(child, rack.rackName);
        rackSet.push_back(rack);
    }
    return rackSet;
}

void parseTopo(json_value *value)
{
    json_value *it = value->first_child;
    clusterTopo.clusterName.assign(it->string_value); Next(it);
    clusterTopo.rackSet = parseRack(it);
}

int parseTopoJSON(char *source)
{
	char *errorPos = 0;
	char *errorDesc = 0;
	int errorLine = 0;
	block_allocator allocator(1 << 10);

	json_value *root = json_parse(source, &errorPos, &errorDesc, &errorLine, &allocator);
	if (root) {
		parseTopo(root);
	} else {
	    printf("Error at line %d: %s\n%s\n\n", errorLine, errorDesc, errorPos);
	    return -1;
	}
	return 0;
}

void dumpClusterTopo(int ident)
{
    cout<<"clusterName = "<<clusterTopo.clusterName<<endl;
    cout<<"rackSet"<<endl;
    for(size_t i = 0; i < clusterTopo.rackSet.size(); i++) {
        HadoopRack rack = clusterTopo.rackSet[i];
        IDENT(ident + 1); cout<<"rackName = "<<rack.rackName<<endl;
        IDENT(ident + 1); cout<<"hostSet"<<endl;
        for(size_t j = 0; j < rack.hostSet.size(); j++) {
            IDENT(ident + 2); cout<<"hostName = "<<rack.hostSet[j].hostName<<endl;
            IDENT(ident + 2); cout<<"rackName = "<<rack.hostSet[j].rackName<<endl;
            IDENT(ident + 2); cout<<"ipAddr = "<<rack.hostSet[j].ipAddr<<endl;
        }
    }
}

void initTopologyReader(string topologyFile, bool debug)
{
    FILE *fp = fopen(topologyFile.c_str(), "rb");
    if (fp) {
    	fseek(fp, 0, SEEK_END);
    	long size = ftell(fp);
    	fseek(fp, 0, SEEK_SET);
    	vector<char> buffer(size + 1);
    	size_t result = fread(&buffer[0], 1, size, fp);
    	assert(result == (size_t)size);
    	fclose(fp);
        parseTopoJSON(&buffer[0]);
    }
    else {
        cout<<"Topo file '"<<topologyFile<<"' loaded error.\n";
    }

    if (debug)
        dumpClusterTopo(0);
}

vector<HadoopHost> getHostTopology()
{
    return hostTopo;
}
