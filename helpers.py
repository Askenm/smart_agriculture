import networkx as nx
import matplotlib.pyplot as plt
import pickle
import json

def plot_bipartite_graph(B,resize = False):
    if resize:
    # Calculate betweenness centrality
        centrality = nx.degree_centrality(B)
        # Normalize the values for node size
        centrality_values = [v * 1000 for v in centrality.values()]
    else:
        centrality_values = [300]*B.number_of_nodes()
    # Separate the nodes by partition
    left_nodes = {n for n, d in B.nodes(data=True) if d['bipartite']==0}
    right_nodes = set(B) - left_nodes

    # Assign colors based on the partition
    color_map = []
    for node in B:
        if node in left_nodes:
            color_map.append('#e17055')
        else: 
            color_map.append('#00b894')

    # Draw the graph
    plt.style.use('fivethirtyeight')
    plt.figure(figsize=[16,9])
    pos = nx.bipartite_layout(B, left_nodes)
    nx.draw(B, pos, node_color=color_map, with_labels=True, font_weight='bold',node_size = centrality_values)

    plt.show()




def assign_levels(G):
    """Assign levels to each node based on the longest path from a root."""
    levels = {}
    for node in nx.topological_sort(G):
        if G.in_degree(node) == 0:
            # Root node
            levels[node] = 0
        else:
            # Non-root node: one more than max level of predecessors
            levels[node] = max(levels[predecessor] for predecessor in G.predecessors(node)) + 1
    return levels

def draw_job_DAG(G):
    levels = assign_levels(G)

    # Horizontal position based on levels, vertical position to spread out nodes
    pos = {node: (level, -index) for index, (node, level) in enumerate(sorted(levels.items(), key=lambda x: x[1]))}

    # Draw the graph
    plt.style.use('fivethirtyeight')
    plt.figure(figsize=[16,9])
    nx.draw(G, pos, with_labels=True, arrows=True, node_size=700, node_color='lightblue', font_size=10, font_weight='bold', arrowstyle='->', arrowsize=12)
    plt.show()





def save_graphs_to_pickle(graph_dict,filename, path='./data/fake_data/'):
    graph_dict = graph_dict.copy()
    for key, graph in graph_dict.items():
        with open(f'{path}graphs/{key}.pkl', 'wb') as file:
            pickle.dump(graph, file)
        graph_dict[key] = f'{path}graphs/{key}.pkl'

    dump_dict_to_json(graph_dict, f'{path}{filename}')


def load_graphs_from_pickle(dict_file_name='machine_processes_dag.json', path='./data/fake_data/'):
    with open(path+dict_file_name, 'r') as json_file:
        graph_dict = json.load(json_file)
    for key, graph_path in graph_dict.items():
        with open(graph_path, 'rb') as file:
            graph_dict[key] = pickle.load(file)
    return graph_dict


def load_dict_from_json(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)


def load_all_graphs(path='./data/fake_data/'):

    machine_processes_dag = load_graphs_from_pickle(path=path)
    order_job_map = load_dict_from_json(f'{path}order_job_map.json')
    resource_process_map = load_dict_from_json(f'{path}resource_process_map.json')

    return order_job_map,machine_processes_dag,resource_process_map





def create_bipartite_graph_from_dict(dictionary):
    G = nx.DiGraph()
    G.add_nodes_from(dictionary.keys(), bipartite=0)
    G.add_nodes_from(set(j for i in dictionary.values() for j in i), bipartite=1)
    G.add_edges_from([(k, v) for k, vs in dictionary.items() for v in vs])


    return G





