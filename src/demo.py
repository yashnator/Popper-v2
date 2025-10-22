from .popper import Node, Edge, RootNode
import time


def process_item(x):
    x["value"] *= 2
    return x


r = RootNode("Root")
a = Node("A", lambda x: x)
b = Node("B", process_item)
c = Node("C", lambda x: x)
nodes = [r, a, b, c]

edge0 = Edge(r, a)
edge1 = Edge(a, b)
edge2 = Edge(b, c)

edges = [edge0, edge1, edge2]

r.add_out_edge(edge0)
a.add_in_edge(edge0)
a.add_out_edge(edge1)
b.add_in_edge(edge1)
b.add_out_edge(edge2)
c.add_in_edge(edge2)

edge1.add_item({"value": 10})
edge1.add_item({"value": 20})

for n in nodes:
    n.start()
time.sleep(0.5)
for n in nodes:
    n.stop()

print(edge2.items)
