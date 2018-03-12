#include "basic/pregel-dev.h"
using namespace std;

//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v \t min_vertexID(v's connected component)

struct CCValue_pregel {
	int level;
	vector<VertexID> in_neighbor;
	vector<VertexID> out_neighbor;
	vector<VertexID> in_label;
	vector<VertexID> out_label;
	bool used[9];
};
struct my_Message {
	int id;
	int level;
	int min_level;
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
		if (step_num() == 1) {
			my_Message a;
			a.id = id;
			a.level = a.min_level = value().level;
			a.fwdORbwd = 1;
			broadcast(a);
			a.fwdORbwd = 0;
			broadcast(a);
			vote_to_halt();
		} else {
			for (int i = 0; i < messages.size(); i++) {
				my_Message a = messages[i];
				if (a.level > value().level) {
					continue;
				}
				if (a.level < value().level) {
					if (a.fwdORbwd)
						value().in_label.push_back(a.id);
					else
						value().out_label.push_back(a.id);
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
		v->id = atoi(pch);
//		printf("%d	", v->id);
		pch = strtok(NULL, " ");
		v->value().level = atoi(pch);
//		printf("%d	", v->value().level);
		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//			printf("%d	", atoi(pch));
			v->value().out_neighbor.push_back(atoi(pch));
		}
		pch = strtok(NULL, " ");
		num = atoi(pch);
//		printf("%d	", num);
		for (int i = 0; i < num; i++) {
			pch = strtok(NULL, " ");
//			printf("%d	", atoi(pch));
			v->value().in_neighbor.push_back(atoi(pch));
		}
//		printf("\n");
		return v;
	}

	virtual void toline(CCVertex_pregel* v, BufferedWriter & writer) {
		int j=sprintf(buf, "%d\n", v->id);
//		printf("%d\n\t", v->id);
		j+=sprintf(buf+j, "in_label:");
//		printf("in_label:");
		sort(v->value().in_label.begin(), v->value().in_label.end());
		vector<VertexID>::iterator iter = unique(v->value().in_label.begin(),
				v->value().in_label.end());
		v->value().in_label.erase(iter, v->value().in_label.end());
		for (iter = v->value().in_label.begin();
				iter != v->value().in_label.end(); iter++) {
			j+=sprintf(buf+j, "%d ", *iter);
//			printf("%d ", *iter);
		}
		j+=sprintf(buf+j, "\n");
		j+=sprintf(buf+j, "out_label:");
//		printf("/n/t out_label:");
		sort(v->value().out_label.begin(), v->value().out_label.end());
		iter = unique(v->value().out_label.begin(),
				v->value().out_label.end());
		v->value().out_label.erase(iter, v->value().out_label.end());
		for (iter = v->value().out_label.begin();
				iter != v->value().out_label.end(); iter++) {
			j+=sprintf(buf+j, "%d ", *iter);
//			printf("%d ", *iter);
		}
		j+=sprintf(buf+j, "\n");
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
