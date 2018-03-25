#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "basic/Worker.h"
#include "stdio.h"
using namespace std;
typedef unsigned long int uint64;
//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

struct CCValue_pregel {
	int level;
	vector<VertexID> in_neighbor;
	vector<VertexID> out_neighbor;
	vector<VertexID> in_label;
	vector<VertexID> out_label;
	vector<uint64> used_in;
	vector<uint64> used_out;
	vector<uint64> min_pathOut_level;
	vector<uint64> min_pathIn_level;
	void init() {
//		for(int i=0;i<50;i++){
//			used_in[i]=0;
//			used_out[i]=0;
//			min_pathIn_level[i]=0;
//			min_pathOut_level[i]=0;
//		}
		int i=used_in.size();
		if(i==0){
			used_in.push_back(0);
			used_out.push_back(0);
			min_pathOut_level.push_back(0);
			min_pathIn_level.push_back(0);
		}else{
			for(int j=0;j<i;j++){
				used_in[j]=0;
				used_out[j]=0;
				min_pathOut_level[j]=0;
				min_pathIn_level[j]=0;
			}
		}
		if(Batch_Size[batch-1]/64>i){
			used_in.push_back(0);
			used_out.push_back(0);
			min_pathOut_level.push_back(0);
			min_pathIn_level.push_back(0);
		}
	}
};
struct my_Message {
	int id;
	int level;
	bool min_level;
	bool fwdORbwd; //ture -> forward and false -> backward
};
struct respond_message {
	vector<VertexID> in;
	vector<VertexID> out;
	respond_message& operator =(const respond_message& b) {
		in = b.in;
		out = b.out;
		return *this;
	}
};

ibinstream & operator<<(ibinstream & m, const CCValue_pregel & v) {
	m << v.level;
	m << v.in_neighbor;
	m << v.out_neighbor;
	m << v.in_label;
	m << v.out_label;
	return m;
}

obinstream & operator>>(obinstream & m, CCValue_pregel & v) {
	m >> v.level;
	m >> v.in_neighbor;
	m >> v.out_neighbor;
	m >> v.in_label;
	m >> v.out_label;
	return m;
}
ibinstream & operator<<(ibinstream & m, const my_Message & v) {
	m << v.level;
	m << v.min_level;
	m << v.fwdORbwd;
	return m;
}

obinstream & operator>>(obinstream & m, my_Message & v) {
	m >> v.level;
	m >> v.min_level;
	m >> v.fwdORbwd;
	return m;
}
ibinstream & operator<<(ibinstream & m, const respond_message & v) {
	m << v.in;
	m << v.out;
	return m;
}

obinstream & operator>>(obinstream & m, respond_message & v) {
	m >> v.in;
	m >> v.out;
	return m;
}
bool set_intersection(vector<VertexID>& a,vector<VertexID>& b){
	int size_a=a.size();
	int size_b=b.size();
	if(size_a+size_b>get_vnum())return false;
	int size_1=0;
	int size_2=0;
	while(size_1<size_a&&size_2<size_b){
		if(a[size_1]<b[size_2]){
			size_1++;
		}else if(a[size_1]>b[size_2]){
			size_2++;
		}else{
			return false;
		}
	}
	return true;
}
//====================================
class CCVertex_pregel: public Vertex<VertexID, CCValue_pregel, my_Message> {
public:
	void broadcast(my_Message msg) {
		if (msg.fwdORbwd) {
			vector<VertexID> & nbs = value().out_neighbor;
			for (int i = 0; i < nbs.size(); i++) {
				send_message(nbs[i], msg);
			}
		} else {
			vector<VertexID> & nbs = value().in_neighbor;
			for (int i = 0; i < nbs.size(); i++) {
				send_message(nbs[i], msg);
			}
		}
	}
	virtual void compute(MessageContainer & messages) {
		if (step_num() == 1 || init_bit) {
			value().init(); //init min path level
			if (value().level>=begin_num&&value().level<begin_num+Batch_Size[batch-1]) {
			my_Message a;
			a.id=id;
			a.level = value().level;
			a.min_level = 0;//0 represent don't have a smaller one
			a.fwdORbwd = 1;//same with the eage direction
			broadcast(a);
			a.fwdORbwd = 0;//different from the eage direction
			broadcast(a);
		}
		vote_to_halt();
	} else {
		if (value().level < begin_num ){
		    vote_to_halt();
			return;
		}// if it is the last batch's level,return immediately
		int size = messages.size();
		for (int i = 0; i < size; i++) {
			my_Message a = messages[i];
			if (a.fwdORbwd && (value().min_pathIn_level[a.level/64]&(1<<(hashbacket(a.level)-(a.level/64)*64))))
			{//exclude pruning
				continue; //has been handled
			}
			if((!a.fwdORbwd) && (value().min_pathOut_level[a.level/64]&(1<<(hashbacket(a.level)-(a.level/64)*64)))){
				//exclude pruning
				continue;//has been handled
			}
			if (a.level < value().level) {// the source vertext's level is highter than mine
				if (!a.min_level) {//it doesn't exist a vertex highter than it
					if (a.fwdORbwd) {//same direction
						if(value().used_in[a.level/64]&1<<(hashbacket(a.level)-(a.level/64)*64))
							continue;//has been push in
//						vector<VertexID> temp;
						//intersection should be (a.id|a.out)&(id|in_label)?
//						set_intersection(mir[hashbacket(a.level)].out.begin(),
//								mir[hashbacket(a.level)].out.end(),
//								value().in_label.begin(),
//								value().in_label.end(),
//								inserter(temp, temp.begin()));
						if (set_intersection(mir[hashbacket(a.level)].out,value().in_label)) {//if empty push in
//							value().in_label.push_back(a.id);later handle the label,there only update the used arrays
							//include pruning here, if used_in is 1, no need broadcast anymore. no more than twice bfs
							value().used_in[a.level/64]|=(1<<(hashbacket(a.level)-(a.level/64)*64));
						}else{
								//pruning process goes here
							continue;
						}
					} else if(!a.fwdORbwd){
						if(value().used_out[a.level/64]&(1<<(hashbacket(a.level)-(a.level/64)*64)))
							continue;
//						vector<VertexID> temp;
						//intersection should be (a.id|a.out)&(id|in_label)?
//						set_intersection(mir[hashbacket(a.level)].in.begin(),
//								mir[hashbacket(a.level)].in.end(),
//								value().out_label.begin(),
//								value().out_label.end(),
//								inserter(temp, temp.begin()));
						if (set_intersection(mir[hashbacket(a.level)].in,value().out_label)) {
//							value().out_label.push_back(a.id);
							//include pruning here, if used_in is 1, no need broadcast anymore. no more than twice bfs
							value().used_out[a.level/64]|=(1<<(hashbacket(a.level)-(a.level/64)*64));
						}else{
							continue;					//pruning process goes here
						}
					}
					broadcast(a);
				} else if (a.min_level) {
//					vector<VertexID>::iterator Iter;
					if (a.fwdORbwd) {
						value().min_pathIn_level[a.level/64]|=1<<(hashbacket(a.level)-(a.level/64)*64);
						if(value().used_in[a.level/64]&(1<<(hashbacket(a.level)-(a.level/64)*64)))
							value().used_in[a.level/64]&=~(1<<(hashbacket(a.level)-(a.level/64)*64));//set 0
//						for (Iter = value().in_label.begin();
//								Iter != value().in_label.end(); Iter++) {
//							if (*Iter == a.level) {
//								value().in_label.erase(Iter);
//								break;
//							}
						} else {
						value().min_pathOut_level[a.level/64]|=1<<(hashbacket(a.level)-(a.level/64)*64); // log that there is a smaller one between this two vertexes
						if(value().used_in[a.level/64]&(1<<(hashbacket(a.level)-(a.level/64)*64))){
							value().used_out[a.level/64]&=~(1<<(hashbacket(a.level)-(a.level/64)*64));// set 0
//							for (Iter = value().out_label.begin();
//									Iter != value().out_label.end(); Iter++) {
//								if (*Iter == a.level) {
//									value().out_label.erase(Iter);
//									break;
//								}
//							}
						}
					}
					broadcast(a);
				}
			}else {
				a.min_level = 1; // there is a smaller  level in one path.
				broadcast(a);
			}
		}
		vote_to_halt();
	}
}
	void postbatch_compute(){
		//the update process of label_in and label_out goes here

	}
};

class CCWorker_pregel: public Worker<CCVertex_pregel> {
	char buf[100];

public:
//C version
	virtual CCVertex_pregel* toVertex(char* line) {
		char * pch;
		pch = strtok(line, "\t");
		CCVertex_pregel* v = new CCVertex_pregel;
		v->id = atoi(pch)-1;
//		printf("%d	", v->id);
		pch = strtok(NULL, " ");
		v->value().level = atoi(pch)-1;
//		printf("%d	", v->value().level);
		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//			printf("%d	", atoi(pch));
			v->value().out_neighbor.push_back(atoi(pch)-1);
		}
		pch = strtok(NULL, " ");
		num = atoi(pch);
//		printf("%d	", num);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//			printf("%d	", atoi(pch));
			v->value().in_neighbor.push_back(atoi(pch)-1);
		}
//		printf("\n");
		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
		int j = sprintf(buf, "%d\n", v->id);
//		printf("%d\n\t", v->id);
		j += sprintf(buf + j, "in_label:");
//		printf("in_label:");
//		sort(v->value().in_label.begin(), v->value().in_label.end());
//		vector<VertexID>::iterator iter = unique(v->value().in_label.begin(),
//				v->value().in_label.end());
//		v->value().in_label.erase(iter, v->value().in_label.end());
		vector<VertexID>::iterator iter;
		for (iter = v->value().in_label.begin();
				iter != v->value().in_label.end(); iter++) {
			j += sprintf(buf + j, "%d ", *iter);
//			printf("%d ", *iter);
		}
		j += sprintf(buf + j, "\n");
		j += sprintf(buf + j, "out_label:");
//		printf("/n/t out_label:");
//		sort(v->value().out_label.begin(), v->value().out_label.end());
//		iter = unique(v->value().out_label.begin(), v->value().out_label.end());
//		v->value().out_label.erase(iter, v->value().out_label.end());
		for (iter = v->value().out_label.begin();
				iter != v->value().out_label.end(); iter++) {
			j += sprintf(buf + j, "%d ", *iter);
//			printf("%d ", *iter);
		}
		j += sprintf(buf + j, "\n");
		printf(buf);
//		sprintf(buf, "/n");
//		printf("/n");
		writer.write(buf);
	}
}
;

//class CCCombiner_pregel: public Combiner<my_Message> {
//public:
//	virtual void combine(my_Message & old, const my_Message & new_msg) {
//		if ((old.id==new_msg.id)&&(new_msg.fwdORbwd=old.fwdORbwd)&&(new_msg.min_level))
//			old.min_level=1;
//	}
//};

void pregel_hashmin(string in_path, string out_path, bool use_combiner) {
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	CCWorker_pregel worker;
	worker.run(param);
}
