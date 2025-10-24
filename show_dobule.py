import json
import networkx as nx
import plotly.graph_objects as go
from collections import defaultdict


def visualize_mesh_tsn(json_file, output_html="tsn_mesh_550_dual.html"):
    # 1. 读取JSON文件
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    nodes_data = data["nodes"]
    links_data = data["links"]

    # ===== 新增：验证端点连接情况 =====
    print("\n" + "=" * 60)
    print("端点连接验证")
    print("=" * 60)

    endpoint_connections = defaultdict(list)
    for link in links_data:
        source = link["source"]
        target = link["target"]
        if source.startswith("EP"):
            endpoint_connections[source].append(target)
        elif target.startswith("EP"):
            endpoint_connections[target].append(source)

    # 统计
    total_eps = sum(1 for n in nodes_data if n['type'] == 'endpoint')
    dual_connected = sum(1 for conns in endpoint_connections.values() if len(conns) == 2)
    single_connected = sum(1 for conns in endpoint_connections.values() if len(conns) == 1)

    print(f"\n总端点数: {total_eps}")
    print(f"双连接端点: {dual_connected} ({dual_connected / total_eps * 100:.1f}%)")
    print(f"单连接端点: {single_connected}")
    print(f"未连接端点: {total_eps - len(endpoint_connections)}")

    # 显示前10个端点的连接详情
    print(f"\n前10个端点详情:")
    eps = sorted([n["id"] for n in nodes_data if n["type"] == "endpoint"])
    for ep in eps[:10]:
        conns = endpoint_connections.get(ep, [])
        status = "✓" if len(conns) == 2 else "✗"
        print(f"  {status} {ep}: {len(conns)}条连接 -> {conns}")

    print("=" * 60 + "\n")
    # ===== 验证结束 =====

    # 2. 创建NetworkX图
    G = nx.Graph()

    # 添加节点
    for node in nodes_data:
        G.add_node(node["id"], node_type=node["type"])

    # 添加边（链路）
    for link in links_data:
        bandwidth = link.get("bandwidth_mbps", link.get("bandwidth", 1000))
        latency = link.get("latency_ms", 0.001)

        G.add_edge(link["source"], link["target"],
                   bandwidth=bandwidth,
                   latency=latency)

    # 3. 布局：spring_layout适合Mesh大图
    pos = nx.spring_layout(G, k=0.5, iterations=100, seed=42)

    # 4. 准备Plotly边
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    # 5. 准备节点（改进：悬停显示连接信息）
    node_x = []
    node_y = []
    node_color = []
    node_text = []
    node_hover = []  # 新增：悬停信息

    for node in G.nodes(data=True):
        x, y = pos[node[0]]
        node_x.append(x)
        node_y.append(y)
        node_id = node[0]

        if node[1]['node_type'] == 'switch':
            node_color.append('blue')
            node_text.append(node_id)
            degree = G.degree(node_id)
            node_hover.append(f"{node_id}<br>类型: 交换机<br>连接数: {degree}")
        else:
            # 端点根据连接数着色
            conns = endpoint_connections.get(node_id, [])
            num_conns = len(conns)

            if num_conns == 2:
                node_color.append('green')  # 双连接：绿色
            elif num_conns == 1:
                node_color.append('orange')  # 单连接：橙色
            else:
                node_color.append('red')  # 未连接：红色

            node_text.append(node_id)
            node_hover.append(f"{node_id}<br>类型: 端点<br>连接数: {num_conns}<br>连接到: {', '.join(conns)}")

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=node_text,
        textposition="top center",
        hoverinfo='text',
        hovertext=node_hover,  # 使用详细的悬停信息
        marker=dict(
            color=node_color,
            size=10,
            line_width=1))

    # 6. 创建Figure
    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title=f'550-Node TSN Mesh Topology<br><sub>双连接端点: {dual_connected}/{total_eps} ({dual_connected / total_eps * 100:.1f}%)</sub>',
                        title_x=0.5,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=60),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    # 7. 保存为HTML
    fig.write_html(output_html)
    print(f"✓ 可视化HTML已生成: {output_html}")
    print("用浏览器打开即可交互查看拓扑")
    print("\n图例:")
    print("  🔵 蓝色 = 交换机")
    print("  🟢 绿色 = 双连接端点（正常）")
    print("  🟠 橙色 = 单连接端点（警告）")
    print("  🔴 红色 = 未连接端点（错误）")
    print("  💡 鼠标悬停节点可查看详细连接信息\n")


if __name__ == "__main__":
    visualize_mesh_tsn("tsn_mesh_550_dual.json")