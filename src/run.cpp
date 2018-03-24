#include "pregel_app_sssp.h"

int main(int argc, char* argv[]){
	init_workers();
	batch=1;
	init();
	pregel_hashmin("/test2", "/toyOutput", true);
	worker_finalize();
	return 0;
}
