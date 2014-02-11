#include <string>
#include <vector>
#include <sstream>
#include <exception>
#include <graphlab.hpp>
#define THRESHOLD 0.05


typedef int labelType;
typedef float labelCountType;

struct my_vertex {
	std::map<labelType, labelCountType> labels;
	int labelCount;
	my_vertex(){ 
		labelCount = 0;
	}
	
	my_vertex(labelType id)
	{
		labels[id] = 1;
		labelCount = 1;
	}
	
	void removeExtraLabels()
	{
		std::map<labelType, labelCountType>::iterator it, next;
		
		for( it = next = labels.begin(); it != labels.end() ; it = next )
		{
			++next;
			it->second /= labelCount;
			if(it->second < THRESHOLD)
				labels.erase(it);
		}
	}
	
	void save(graphlab::oarchive& oarc) const 
	{
		oarc << labelCount;
		oarc << labels;
	}
	
	void load(graphlab::iarchive& iarc) {
		iarc >> labelCount;
		iarc >> labels;
	} 
};

struct gatherType 
{
	std::string value;
	
	gatherType() { value=""; }
	gatherType(std::string str) { value = str; }
	
	gatherType& operator+=(const gatherType& other)
	{
		value += " " + other.value;
		return *this;
	}
	
	void save(graphlab::oarchive& oarc) const {
		oarc << value;
	}
	
	void load(graphlab::iarchive& iarc) {
		iarc >> value;
	} 
};

typedef graphlab::distributed_graph<my_vertex , graphlab::empty> graph_type;


bool line_parser(graph_type& graph, const std::string& filename, const std::string& textline) {
	if (textline.empty()) return true;
	std::stringstream strm(textline);
	graphlab::vertex_id_type source, target;
	strm >> source;
	strm >> target;
	graph.add_vertex(source , my_vertex(source));
	graph.add_vertex(target , my_vertex(target));
	graph.add_edge(source, target);
	return true;
}


inline std::string labelToStr (labelType number){
	std::ostringstream buff;
	buff<<number;
	return buff.str();
}

inline labelType speakerRule(std::map<labelType, labelCountType> *Labels)
{
	labelType max;
	labelCountType max_count;
	std::map<labelType, labelCountType>::iterator ii=Labels->begin();
	max = ii->first;
	max_count = ii->second;
	
	for( ; ii!=Labels->end(); ++ii)
	{
		if(ii->second > max_count)
		{
			max_count = ii->second;
			max = ii->first;
		}
	}
	return max;
}

inline labelType listenerRule(std::map<labelType,labelCountType> *labelList)
{
	labelType max;
	labelCountType max_count;
	std::map<labelType, labelCountType>::iterator ii=labelList->begin();
	max = ii->first;
	max_count = ii->second;
	
	for( ii++; ii!=labelList->end(); ++ii)
	{
		if(ii->second > max_count)
		{
			max_count = ii->second;
			max = ii->first;
		}
	}
	return max;
}

inline std::map<labelType,labelCountType> *parseLabelString(std::string labels)
{
	std::map<labelType,labelCountType> *temp = new std::map<labelType,labelCountType>;
	std::stringstream strm(labels);
	labelType label;
	while(1)
	{
		strm >> label;
		if(strm.fail()) break;
		try
		{
			(temp->at(label))++;
		}
		catch(std::exception& e)
		{
			(*temp)[label] = 1;
		}
	}
	return temp;
}

class slpa_program : public graphlab::ivertex_program<graph_type,gatherType>, public graphlab::IS_POD_TYPE {
public:
	edge_dir_type gather_edges(icontext_type& context, const vertex_type& vertex) const {
		return graphlab::OUT_EDGES;
	}
  
	gather_type gather(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
		gatherType ret;
		std::map<labelType, labelCountType> labelsTemp = edge.target().data().labels;
		labelType label = speakerRule(&labelsTemp);
		ret.value = labelToStr(label);
		return ret;
	}
  
	void apply(icontext_type& context, vertex_type& vertex, const gather_type& total) {
		if(!total.value.empty())
		{
			std::map<labelType, labelCountType> *temp = parseLabelString(total.value);
			labelType label = listenerRule(temp);	
			try
			{
				(vertex.data().labels.at(label))++;
			}
			catch(std::exception& e)
			{
				vertex.data().labels[label] = 1;
			}
			vertex.data().labelCount++;
		}
	}
  
	edge_dir_type scatter_edges(icontext_type& context, const vertex_type& vertex) const {
		return graphlab::OUT_EDGES;
	}
	
	void scatter(icontext_type& context, const vertex_type& vertex, edge_type& edge) const {
		context.signal(edge.target());
	}
};


class graph_writer {
public:
  std::string save_vertex(graph_type::vertex_type v)
  {
    std::stringstream strm;
	std::map<labelType, labelCountType>::iterator it;
	
	strm << v.id();
	
	if(v.data().labels.empty())
		std::cout << "empty" << std::endl;
	
	for( it = v.data().labels.begin(); it != v.data().labels.end() ; it++ )
		strm << " " << it->first;
	
	strm << "\n";
	
    return strm.str();
  }
  
  std::string save_edge(graph_type::edge_type e) { return ""; }
};


void set_vertex_value(graphlab::omni_engine<slpa_program>::icontext_type& context, graph_type::vertex_type vertex) {
	vertex.data().removeExtraLabels();
}

int main(int argc, char** argv) {
	graphlab::mpi_tools::init(argc, argv);
	graphlab::distributed_control dc;
	std::string graph_dir;
	float threshold = 0.1;
	std::string out_dir = "";
	std::string exec_type = "synchronous";
	
	graphlab::command_line_options clopts("Community Detection using Graphlab");
	clopts.attach_option("graph", graph_dir, "The graph file. Required ");
	clopts.add_positional("graph");
	clopts.attach_option("output", out_dir, "The Output directory.");
	clopts.attach_option("r", threshold , "The threshold for removing extra labels.");
	//clopts.attach_option("engine", exec_type, "The engine type synchronous or asynchronous");
	
	if(!clopts.parse(argc, argv)) {
		dc.cout() << "Error in parsing command line arguments." << std::endl;
		return EXIT_FAILURE;
	}

	if (graph_dir == "") {
		dc.cout() << "Graph not specified. Cannot continue";
		return EXIT_FAILURE;
	}
	
	graph_type graph(dc);
	graph.load(graph_dir , line_parser);
	graph.finalize();

	graphlab::omni_engine<slpa_program> engine(dc, graph, "sync", clopts);
	engine.signal_all();
	engine.start();
	engine.transform_vertices(set_vertex_value);

	graph.save(out_dir + "/output" , graph_writer() , false , true, false);

	graphlab::mpi_tools::finalize();
}

