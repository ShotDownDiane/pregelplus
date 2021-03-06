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
	vector<VertexID> in_respone;
	vector<VertexID> out_respone;
	uint64 used_in;
	uint64 used_out;
	uint64 min_pathOut_level;
	uint64 min_pathIn_level;
	void init() {
//		for(int i=0;i<50;i++){
//			used_in[i]=0;
//			used_out[i]=0;
//			min_pathIn_level[i]=0;
//			min_pathOut_level[i]=0;
//		}
		used_in=0;
		used_out=0;
		min_pathOut_level=0;
		min_pathIn_level=0;
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
	m << v.id;
	m << v.level;
	m << v.min_level;
	m << v.fwdORbwd;
	return m;
}

obinstream & operator>>(obinstream & m, my_Message & v) {
	m >> v.id;
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
			if (value().level>= (batch - 1) * BATCH_SIZE&& value().level <= batch * BATCH_SIZE-1) {
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
		if (value().level < (batch - 1) * BATCH_SIZE ){
		    vote_to_halt();
			return;
		}// if it is the last batch's level,return immediately
		int size = messages.size();
		for (int i = 0; i < size; i++) {
			my_Message a = messages[i];
			if (a.fwdORbwd && (value().min_pathIn_level&(1<<(hashbacket(a.id)))))
			{
				continue; //has been handled
			}
			if((!a.fwdORbwd) && (value().min_pathOut_level&(1<<(hashbacket(a.id))))){
				continue;//has been handled
			}
			if (a.level < value().level) {// the source vertext's level is highter than mine
				if (!a.min_level) {//it doesn't exist a vertex highter than it
					if (a.fwdORbwd) {//same direction
//						if(value().used_in&1<<(hashbacket(a.id)))
//							continue;//has been push in
						vector<VertexID> temp;
						set_intersection(mir[hashbacket(a.id)].out.begin(),
								mir[hashbacket(a.id)].out.end(),
								value().in_label.begin(),
								value().in_label.end(),
								inserter(temp, temp.begin()));
						if (temp.size() == 0) {//if empty push in
//							value().in_label.push_back(a.id);later handle the label,there only update the used arrays
							value().used_in|=(1<<(hashbacket(a.id)));
						}
					} else if(!a.fwdORbwd){
//						if(value().used_out&(1<<(hashbacket(a.id))))
//							continue;
						vector<VertexID> temp;
						set_intersection(mir[hashbacket(a.id)].in.begin(),
								mir[hashbacket(a.id)].in.end(),
								value().out_label.begin(),
								value().out_label.end(),
								inserter(temp, temp.begin()));
						if (temp.size() == 0) {
//							value().out_label.push_back(a.id);
							value().used_out|=(1<<(hashbacket(a.id)));
						}
					}
					broadcast(a);
				} else if (a.min_level) {
//					vector<VertexID>::iterator Iter;
					if (a.fwdORbwd) {
						value().min_pathIn_level|=1<<(hashbacket(a.id));
						if(value().used_in&(1<<hashbacket(a.id)))
							value().used_in&=~(1<<(hashbacket(a.id)));//set 0
//						for (Iter = value().in_label.begin();
//								Iter != value().in_label.end(); Iter++) {
//							if (*Iter == a.level) {
//								value().in_label.erase(Iter);
//								break;
//							}
						} else {
						value().min_pathOut_level|=1<<(hashbacket(a.id)); // log that there is a smaller one between this two vertexes
						if(value().used_in&(1<<hashbacket(a.id))){
							value().used_out&=~(1<<(hashbacket(a.id)));// set 0
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
		sort(v->value().in_label.begin(), v->value().in_label.end());
		vector<VertexID>::iterator iter = unique(v->value().in_label.begin(),
				v->value().in_label.end());
		v->value().in_label.erase(iter, v->value().in_label.end());
		for (iter = v->value().in_label.begin();
				iter != v->value().in_label.end(); iter++) {
			j += sprintf(buf + j, "%d ", *iter);
//			printf("%d ", *iter);
		}
		j += sprintf(buf + j, "\n");
		j += sprintf(buf + j, "out_label:");
//		printf("/n/t out_label:");
		sort(v->value().out_label.begin(), v->value().out_label.end());
		iter = unique(v->value().out_label.begin(), v->value().out_label.end());
		v->value().out_label.erase(iter, v->value().out_label.end());
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
