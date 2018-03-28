#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "basic/Worker.h"
#include "stdio.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

struct CCValue_pregel {
	int level;
	vector<VertexID> in_neighbor;
	vector<VertexID> out_neighbor;
	vector<VertexID> in_label;
	vector<VertexID> out_label;
	vector<bitcontainer> used_in; //to be added to inlabel, set to 1, else 0
	vector<bitcontainer> used_out;
	vector<bitcontainer> min_pathOut_level; //from i to this point don't have a lower level than i.level ,set 0;
	vector<bitcontainer> min_pathIn_level;
	void init() {
		vector<bitcontainer> init_vec;
		init_vec.resize(Batch_Size[batch - 1] / 64 + 1, 0ul);
		used_in = init_vec;
		used_out = init_vec;
		min_pathOut_level = init_vec;
		min_pathIn_level = init_vec;
	}
};
struct my_Message {
	int level; //source vertex level
	bool min_level; //true: contains lower level than source vertex in path
	bool fwdORbwd; //ture -> forward and false -> backward
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

bool set_intersection(vector<VertexID>& a, vector<VertexID>& b) {
	//if contains same element, return false
	int size_a = a.size();
	int size_b = b.size();
	if (size_a + size_b > get_vnum())
		return false;

	int size_1 = 0;
	int size_2 = 0;
	while (size_1 < size_a && size_2 < size_b) {
		if (a[size_1] < b[size_2]) {
			size_1++;
		} else if (a[size_1] > b[size_2]) {
			size_2++;
		} else {
			return false; //contain same element
		}
	}
	return true;
}
//====================================
class CCVertex_pregel: public Vertex<VertexID, CCValue_pregel, my_Message> {
public:
	void broadcast(my_Message &msg) {
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
			if (value().level >= begin_num
					&& value().level < begin_num + Batch_Size[batch - 1]) {
				my_Message msg;
				msg.level = value().level;
//			cout << value().level << endl;
				msg.min_level = 0; //0 represent don't have a smaller one
				msg.fwdORbwd = 1; //same with the eage direction
				broadcast(msg);
				msg.fwdORbwd = 0; //different from the eage direction
				broadcast(msg);
			}
			vote_to_halt();
		} else {
			if (value().level < begin_num) {
				vote_to_halt();
				return;
			} // if it is the last batch's level,return immediately
			int size = messages.size();
			for (int i = 0; i < size; i++) {
				my_Message &a = messages[i];
				int new_id = hashbacket(a.level);
				if (a.fwdORbwd && getbit(value().min_pathIn_level, new_id)) { //exclude pruning
					continue; //has been handled
				}
				if ((!a.fwdORbwd)
						&& getbit(value().min_pathOut_level, new_id)) {
					//exclude pruning
					continue;//has been handled
				}
				if (a.level < value().level) { // the source vertext's level is highter than mine
					if (!a.min_level) { //it doesn't exist a vertex highter than it
						if (a.fwdORbwd) { //same direction
							if (getbit(value().used_in, new_id))
								continue; //has been push in
//						vector<VertexID> temp;
								//intersection should be (a.id|a.out)&(id|in_label)?
//						set_intersection(mir[hashbacket(a.level)].out.begin(),
//								mir[hashbacket(a.level)].out.end(),
//								value().in_label.begin(),
//								value().in_label.end(),
//								inserter(temp, temp.begin()));
							if (set_intersection(mir[new_id].out,
									value().in_label)) { //if empty push in
//							value().in_label.push_back(a.id);later handle the label,there only update the used arrays
									//include pruning here, if used_in is 1, no need broadcast anymore. no more than twice bfs
								setbit(value().used_in, new_id);
							} else {
								//pruning process goes here
								continue;
							}
						} else if (!a.fwdORbwd) {
							if (getbit(value().used_out, new_id))
								continue;
//						vector<VertexID> temp;
							//intersection should be (a.id|a.out)&(id|in_label)?
//						set_intersection(mir[hashbacket(a.level)].in.begin(),
//								mir[hashbacket(a.level)].in.end(),
//								value().out_label.begin(),
//								value().out_label.end(),
//								inserter(temp, temp.begin()));
							if (set_intersection(mir[new_id].in,
									value().out_label)) {
//							value().out_label.push_back(a.id);
								//include pruning here, if used_in is 1, no need broadcast anymore. no more than twice bfs
								setbit(value().used_out, new_id);
							} else {
								continue; //pruning process goes here
							}
						}
						broadcast(a);
					} else if (a.min_level) {
//					vector<VertexID>::iterator Iter;
						if (a.fwdORbwd) {
							setbit(value().min_pathIn_level, new_id);
//						if(value().used_in[new_id/64]&(1<<(new_id%64)))
							unsetbit(value().used_in, new_id); //set 0
//						for (Iter = value().in_label.begin();
//								Iter != value().in_label.end(); Iter++) {
//							if (*Iter == a.level) {
//								value().in_label.erase(Iter);
//								break;
//							}
						} else {
							setbit(value().min_pathOut_level, new_id); // log that there is a smaller one between this two vertexes
//						if(value().used_in[new_id/64]&(1<<(new_id%64))){
							unsetbit(value().used_out, new_id); // set 0
//							for (Iter = value().out_label.begin();
//									Iter != value().out_label.end(); Iter++) {
//								if (*Iter == a.level) {
//									value().out_label.erase(Iter);
//									break;
//								}
//							}
//						}
						}
						broadcast(a);
					}
				} else if (a.level == value().level) {
					if (a.min_level == 1) {
						if (a.fwdORbwd) {
							setbit(value().min_pathIn_level, new_id);
						} else {
							setbit(value().min_pathOut_level, new_id);
						}
						broadcast(a);
					}

				} else {
					a.min_level = 1; // there is a smaller  level in one path.
					broadcast(a);
				}
			}
			vote_to_halt();
		}
	}
	void postbatch_compute() {
		if (value().level >= begin_num) {
			for (int j = 0; j < Batch_Size[batch - 2]; j++) {
				if (getbit(value().used_in, j)) {
					value().in_label.push_back(mir[j].id);
				}
				if (getbit(value().used_out, j)) {
					value().out_label.push_back(mir[j].id);
				}
			}
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
		v->id = atoi(pch) - 1;
//              int j = sprintf(buf, "%d ", v->id);
		pch = strtok(NULL, " ");
		v->value().level = atoi(pch) - 1;
//              j += sprintf(buf + j, "%d ",v->value().level);
		pch = strtok(NULL, " ");
		int num = atoi(pch);
//              j += sprintf(buf + j, "%d ",num);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//                      j += sprintf(buf + j, "%d ",atoi(pch)-1);
			v->value().out_neighbor.push_back(atoi(pch) - 1);
		}
		pch = strtok(NULL, " ");
		num = atoi(pch);
//              j += sprintf(buf + j, "%d ",num);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//                      j += sprintf(buf + j, "%d ",atoi(pch)-1);
			v->value().in_neighbor.push_back(atoi(pch) - 1);
		}
//              j += sprintf(buf + j, "\n");
//              printf(buf);
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
