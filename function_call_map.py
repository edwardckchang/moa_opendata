
from pycallgraph2 import PyCallGraph
from pycallgraph2.output import GraphvizOutput
from pycallgraph2.pycallgraph import Config
import datetime
import os

def draw_fuction_call_map(func: callable, output_type: str = 'dot'):
    now = datetime.datetime.now()
    if not output_type:
        output_type = 'dot'
    output_file_path = os.path.join("function_call_map", "main_graph" + now.strftime("%Y%m%d%H%M%S") + '.' + output_type)
    
    config = Config(
        include = ["data_moa_gov_tw.*"]
    )
    graphviz_output = GraphvizOutput(
        output_file=output_file_path,
        output_type=output_type,  # 明確指定輸出類型為 dot
        config=config
    )
    with PyCallGraph(output=graphviz_output):
        func()