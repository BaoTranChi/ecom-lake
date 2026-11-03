"""
Kiểm chứng công thức PageRank BSP dùng trong jobs/transforms.py::pagerank_bsp
bằng cách so với networkx.pagerank trên đồ thị ngẫu nhiên.

  make validate-pagerank
Kết quả ghi output/graph/pagerank_validation.json
"""
import json
import os

import networkx as nx

OUT = os.environ.get("OUTPUT_DIR", "/app/output")


def pagerank_bsp(graph: nx.Graph, iterations: int = 30, reset: float = 0.15) -> dict:
    """Cùng công thức với bản Spark: rank = reset + (1-reset) * Σ rank(u)/outdeg(u)."""
    nodes = list(graph)
    outdeg = {n: graph.degree(n) for n in nodes}
    rank = {n: 1.0 for n in nodes}
    for _ in range(iterations):
        msg = {n: 0.0 for n in nodes}
        for u, v in graph.edges():                 # cạnh vô hướng = 2 cạnh có hướng
            msg[v] += rank[u] / outdeg[u]
            msg[u] += rank[v] / outdeg[v]
        rank = {n: reset + (1 - reset) * msg[n] for n in nodes}
    return rank


def main():
    results = []
    for n, m, seed in [(200, 800, 3), (1000, 5000, 7), (2000, 6000, 11)]:
        g = nx.gnm_random_graph(n, m, seed=seed)
        g.remove_nodes_from([x for x in list(g) if g.degree(x) == 0])
        bsp = pagerank_bsp(g)
        ref = nx.pagerank(g, alpha=0.85)
        N = g.number_of_nodes()
        diff = max(abs(bsp[x] - ref[x] * N) for x in g)
        top_b = set(sorted(bsp, key=bsp.get, reverse=True)[:20])
        top_r = set(sorted(ref, key=ref.get, reverse=True)[:20])
        r = {"nodes": N, "edges": g.number_of_edges(), "sum_rank": round(sum(bsp.values()), 4),
             "max_abs_diff_vs_networkx_xN": round(diff, 6), "top20_overlap": len(top_b & top_r)}
        results.append(r)
        print(r)
    os.makedirs(os.path.join(OUT, "graph"), exist_ok=True)
    with open(os.path.join(OUT, "graph", "pagerank_validation.json"), "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
