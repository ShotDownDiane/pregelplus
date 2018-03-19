#include "pregel_app_sssp.h"

int main(int argc, char* argv[]){
	init_workers();
	batch=1;
	pregel_hashmin("/test", "/toyOutput", true);
	worker_finalize();
	return 0;
}
