"""
Graph builder module - Creates graph visualizations from schema and relationships
"""

import json
import yaml
import networkx as nx
from pyvis.network import Network
from typing import Dict, Any, Optional


def build_graph(schema: Dict[str, Any], relationships_file: str, 
                visualize: bool = True, output_json_path: str = "schema_graph.json"):
    """
    Build a graph from schema and relationships, optionally creating visualization.
    
    Args:
        schema: Database schema dictionary
        relationships_file: Path to YAML file with relationship information
        visualize: Whether to create HTML visualization
        output_json_path: Path to save JSON graph data
    """
    # Load relationships
    with open(relationships_file, 'r') as f:
        relationships = yaml.safe_load(f)
    
    # Create NetworkX directed graph
    G = nx.DiGraph()
    
    # Add nodes for each table
    for table in schema['tables']:
        table_name = table['name']
        G.add_node(table_name, 
                   columns=len(table['columns']),
                   label=table_name)
    
    # Add edges for foreign key relationships
    for table_name, table_rels in relationships.items():
        for fk in table_rels.get('foreign_keys', []):
            ref_table = fk['references'].split('.')[0]
            G.add_edge(table_name, ref_table, 
                      label=fk['column'],
                      confidence=fk['confidence'])
    
    # Save graph data as JSON
    graph_data = nx.readwrite.json_graph.node_link_data(G)
    with open(output_json_path, 'w') as f:
        json.dump(graph_data, f, indent=2)
    
    # Create visualization if requested
    if visualize:
        html_filename = output_json_path.replace('.json', '.html')
        if 'postgres' in output_json_path:
            html_filename = 'postgres_schema_graph.html'
        elif 'mysql' in output_json_path:
            html_filename = 'mysql_schema_graph.html'
        elif 'mssql' in output_json_path:
            html_filename = 'mssql_schema_graph.html'
        else:
            html_filename = 'schema_graph.html'
        
        try:
            _create_pyvis_visualization(G, html_filename)
        except Exception as e:
            print(f"Warning: Pyvis visualization failed ({e}), creating fallback HTML")
            _create_fallback_html(G, html_filename)


def _create_pyvis_visualization(G: nx.DiGraph, output_file: str):
    """Create interactive visualization using Pyvis."""
    net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black")
    
    # Add nodes
    for node in G.nodes():
        net.add_node(node, label=node, title=f"Table: {node}")
    
    # Add edges
    for edge in G.edges(data=True):
        source, target, data = edge
        label = data.get('label', '')
        title = f"Foreign Key: {label}"
        net.add_edge(source, target, label=label, title=title)
    
    # Configure physics
    net.set_options("""
    {
        "physics": {
            "enabled": true,
            "stabilization": {"iterations": 100}
        }
    }
    """)
    
    net.show(output_file)


def _create_fallback_html(G: nx.DiGraph, output_file: str):
    """Create fallback HTML visualization using vis.js."""
    # Prepare nodes and edges for vis.js
    nodes = []
    edges = []
    
    for i, node in enumerate(G.nodes()):
        nodes.append({
            "id": i,
            "label": node,
            "title": f"Table: {node}"
        })
    
    node_map = {node: i for i, node in enumerate(G.nodes())}
    
    for edge in G.edges(data=True):
        source, target, data = edge
        edges.append({
            "from": node_map[source],
            "to": node_map[target],
            "label": data.get('label', ''),
            "arrows": "to"
        })
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Database Schema Relationships</title>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style type="text/css">
            #mynetworkid {{
                width: 100%;
                height: 600px;
                border: 1px solid lightgray;
            }}
        </style>
    </head>
    <body>
        <h1>Database Schema Relationships</h1>
        <div id="mynetworkid"></div>

        <script type="text/javascript">
            var nodes = new vis.DataSet({json.dumps(nodes)});
            var edges = new vis.DataSet({json.dumps(edges)});
            var container = document.getElementById('mynetworkid');
            var data = {{
                nodes: nodes,
                edges: edges
            }};
            var options = {{
                physics: {{
                    enabled: true,
                    stabilization: {{iterations: 100}}
                }},
                edges: {{
                    arrows: {{
                        to: {{enabled: true, scaleFactor: 1}}
                    }}
                }}
            }};
            var network = new vis.Network(container, data, options);
        </script>
    </body>
    </html>
    """
    
    with open(output_file, 'w') as f:
        f.write(html_content) 