#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "basic/Worker.h"
using namespace std;

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
	bool used_in[BATCH_SIZE];
	bool used_out[BATCH_SIZE];
	bool min_pathOut_level[BATCH_SIZE];
	bool min_pathIn_level[BATCH_SIZE];
	void init() {
		memset(min_pathIn_level, 0, sizeof(min_pathIn_level));
		memset(min_pathOut_level, 0, sizeof(min_pathOut_level));
		memset(used_in, 0, sizeof(used_in));
		memset(used_out, 0, sizeof(used_out));
	}
};
struct my_Message {
	int id;
	int level;
	int min_level;
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
				cout <<msg.fwdORbwd <<msg.id<<nbs[i]<< endl;
			}
		} else {
			vector<VertexID> & nbs = value().in_neighbor;
			for (int i = 0; i < nbs.size(); i++) {
				send_message(nbs[i], msg);
				cout <<msg.fwdORbwd <<msg.id<<nbs[i] << endl;
			}
		}
	}
	virtual void compute(MessageContainer & messages) {
		if (step_num() == 1 || init_bit) {
			if (value().level>= (batch - 1) * BATCH_SIZE+ 1&& value().level <= batch * BATCH_SIZE) {
			my_Message a;
			value().init(); //init min path level
			a.id=id;
			a.level = value().level;
			a.min_level = 0;//0 represent don't have a smaller one
			a.fwdORbwd = 1;
			broadcast(a);
			a.fwdORbwd = 0;
			broadcast(a);
		}
		vote_to_halt();
	} else {
		if (value().level < (batch - 1) * BATCH_SIZE + 1){
		    vote_to_halt();
			return;
		}// if it is the last batch's level,return immediately
		int size = messages.size();
		vector<VertexID> temp;
		for (int i = 0; i < size; i++) {
			my_Message a = messages[i];
			if ((a.fwdORbwd && value().min_pathIn_level[a.id])
					|| ((!a.fwdORbwd) && value().min_pathOut_level[a.id])){
				broadcast(a);
				continue; //has be used
			}
			if (a.level < value().level) {
				if (!a.min_level) {
					if (a.fwdORbwd) {
						set_union(mir[a.id - (batch-1) * BATCH_SIZE].out.begin(),
								mir[a.id - (batch-1) * BATCH_SIZE].out.end(),
								value().in_label.begin(),
								value().in_label.end(),
								inserter(temp, temp.begin()));
						if (temp.size() == 0) {
							value().in_label.push_back(a.id);
							value().used_in[a.id]=1;
						}
						temp.clear();
					} else if(!a.fwdORbwd){
						set_union(mir[a.id - (batch-1) * BATCH_SIZE].in.begin(),
								mir[a.id - (batch-1) * BATCH_SIZE].in.end(),
								value().out_label.begin(),
								value().out_label.end(),
								inserter(temp, temp.begin()));
						if (temp.size() == 0) {
							value().out_label.push_back(a.id);
							value().used_out[a.id]=1;
						}
						temp.clear();
					}
					broadcast(a);
				} else if (a.min_level) {
					vector<VertexID>::iterator Iter;
					if (a.fwdORbwd) {
						value().min_pathIn_level[a.id] = 1;
						if(value().used_in[a.id])
						for (Iter = value().in_label.begin();
								Iter != value().in_label.end(); Iter++) {
							if (*Iter == a.level) {
								value().in_label.erase(Iter);
								break;
							}
						}
					} else {
						value().min_pathOut_level[a.id] = 1; // log that there is a smaller one between this two vertexes
						if(value().used_out[a.id])
						for (Iter = value().out_label.begin();
								Iter != value().out_label.end(); Iter++) {
							if (*Iter == a.level) {
								value().out_label.erase(Iter);
								break;
							}
						}
					}
					broadcast(a);
				}
			} else {
				a.min_level = 1; // there is a smaller  level in one path.
				broadcast(a);
			}
		}
		cout << "***" << endl;
		vote_to_halt();
		cout << "***" << endl;
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
//		if (old > new_msg)
//			old = new_msg;
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
