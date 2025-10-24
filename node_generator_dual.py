"""
TSN网络拓扑生成器（
生成500节点TSN网络拓扑连接关系，每个端点连接到2个交换机
所有交换机端口带宽统一为1000Mbps
"""

import json
import random


class TSNTopologyGenerator:
    """TSN网络拓扑生成器"""

    def __init__(self):
        # 网络规模配置
        self.TOTAL_SWITCHES = 250
        self.TOTAL_ENDPOINTS = 300
        self.PORTS_PER_SWITCH = 12  # 增加到12个端口，确保有足够端口容量

        # 连接配置
        self.SWITCH_CONNECTIONS_RANGE = (1, 5)
        self.EP_CONNECTIONS = 2

        # 带宽配置 (Mbps)
        self.SWITCH_PORT_BANDWIDTH = 1000  # 交换机端口统一为1000Mbps
        self.ENDPOINT_PORT_BANDWIDTH_RANGE = (100, 1000)  # 端点端口：100-1000Mbps之间随机

    def generate_mesh_topology(self, filename="tsn_mesh_550_dual.json"):
        """生成TSN网络拓扑连接关系"""
        nodes = []
        links = []
        link_id_counter = 1

        print(f"开始生成TSN拓扑...")
        print(f"- 交换机数量: {self.TOTAL_SWITCHES}")
        print(f"- 端点数量: {self.TOTAL_ENDPOINTS}")
        print(f"- 每个交换机端口数: {self.PORTS_PER_SWITCH}")
        print(f"- 每个端点连接数: {self.EP_CONNECTIONS}")
        print(f"- 交换机端口带宽: {self.SWITCH_PORT_BANDWIDTH} Mbps")
        print(f"- 端点端口带宽: {self.ENDPOINT_PORT_BANDWIDTH_RANGE[0]}-{self.ENDPOINT_PORT_BANDWIDTH_RANGE[1]} Mbps (连续随机)")
        print(f"- 总节点数: {self.TOTAL_SWITCHES + self.TOTAL_ENDPOINTS}")
        for i in range(self.TOTAL_SWITCHES):
            node_id = f"SW{i + 1:03d}"
            nodes.append({
                "id": node_id,
                "type": "switch",
                "ports": [f"p{j + 1}" for j in range(self.PORTS_PER_SWITCH)]
            })

        # 2. 创建端点节点
        for i in range(self.TOTAL_ENDPOINTS):
            node_id = f"EP{i + 1:03d}"
            nodes.append({
                "id": node_id,
                "type": "endpoint",
                "ports": ["p1", "p2"]
            })

        switch_nodes = [n for n in nodes if n["type"] == "switch"]
        endpoint_nodes = [n for n in nodes if n["type"] == "endpoint"]

        # 用于跟踪端口使用情况
        port_usage = {node["id"]: set() for node in nodes}

        # 3. 创建交换机间的连接
        print("创建交换机间连接...")
        for sw in switch_nodes:
            others = [n for n in switch_nodes if n["id"] != sw["id"]]
            num_connections = random.randint(*self.SWITCH_CONNECTIONS_RANGE)
            targets = random.sample(others, min(num_connections, len(others)))

            for target in targets:
                # 检查是否已存在连接
                existing_link = any(
                    (link["source"] == sw["id"] and link["target"] == target["id"]) or
                    (link["source"] == target["id"] and link["target"] == sw["id"])
                    for link in links
                )

                if not existing_link:
                    # 分配可用端口
                    available_ports_sw = [p for p in sw["ports"] if p not in port_usage[sw["id"]]]
                    available_ports_target = [p for p in target["ports"] if p not in port_usage[target["id"]]]

                    if available_ports_sw and available_ports_target:
                        sw_port = random.choice(available_ports_sw)
                        target_port = random.choice(available_ports_target)

                        port_usage[sw["id"]].add(sw_port)
                        port_usage[target["id"]].add(target_port)

                        links.append({
                            "link_id": f"L{link_id_counter:04d}",
                            "source": sw["id"],
                            "target": target["id"],
                            "source_port": sw_port,
                            "target_port": target_port,
                            "bandwidth_mbps": self.SWITCH_PORT_BANDWIDTH
                        })
                        link_id_counter += 1

        # 4. 连接端点到交换机
        print("连接端点到交换机（双连接模式）...")
        fully_connected_eps = 0
        partially_connected_eps = 0
        isolated_eps = 0

        for i, ep in enumerate(endpoint_nodes):
            connections_made = 0
            ep_ports = ["p1", "p2"]
            connected_switches = []

            # 选择2个不同的交换机
            primary_sw_idx = i % len(switch_nodes)
            secondary_sw_idx = (i + len(switch_nodes) // 2) % len(switch_nodes)

            if primary_sw_idx == secondary_sw_idx:
                secondary_sw_idx = (secondary_sw_idx + 1) % len(switch_nodes)

            target_switches = [switch_nodes[primary_sw_idx], switch_nodes[secondary_sw_idx]]

            # 尝试连接到预定的2个交换机
            for j, sw in enumerate(target_switches):
                available_ports_sw = [p for p in sw["ports"] if p not in port_usage[sw["id"]]]

                if available_ports_sw:
                    sw_port = random.choice(available_ports_sw)
                    port_usage[sw["id"]].add(sw_port)
                    port_usage[ep["id"]].add(ep_ports[j])

                    links.append({
                        "link_id": f"L{link_id_counter:04d}",
                        "source": ep["id"],
                        "target": sw["id"],
                        "source_port": ep_ports[j],
                        "target_port": sw_port,
                        "bandwidth_mbps": random.randint(*self.ENDPOINT_PORT_BANDWIDTH_RANGE)
                    })
                    link_id_counter += 1
                    connections_made += 1
                    connected_switches.append(sw["id"])

            # 如果连接数不足2个，遍历所有交换机寻找可用端口
            attempts = 0
            while connections_made < 2 and attempts < len(switch_nodes):
                for alt_sw in switch_nodes:
                    if connections_made >= 2:
                        break

                    if alt_sw["id"] in connected_switches:
                        continue

                    alt_ports = [p for p in alt_sw["ports"] if p not in port_usage[alt_sw["id"]]]
                    if alt_ports:
                        sw_port = random.choice(alt_ports)
                        port_usage[alt_sw["id"]].add(sw_port)

                        ep_port = ep_ports[connections_made]
                        port_usage[ep["id"]].add(ep_port)

                        links.append({
                            "link_id": f"L{link_id_counter:04d}",
                            "source": ep["id"],
                            "target": alt_sw["id"],
                            "source_port": ep_port,
                            "target_port": sw_port,
                            "bandwidth_mbps": random.randint(*self.ENDPOINT_PORT_BANDWIDTH_RANGE)
                        })
                        link_id_counter += 1
                        connections_made += 1
                        connected_switches.append(alt_sw["id"])

                attempts += 1

            # 统计连接情况
            if connections_made == 2:
                fully_connected_eps += 1
            elif connections_made == 1:
                partially_connected_eps += 1
                print(f"警告: EP {ep['id']} 只连接到1个交换机（部分冗余）")
            else:
                isolated_eps += 1
                print(f"错误: EP {ep['id']} 未能连接到任何交换机")

        print(f"- 完全连接的端点（2个SW）: {fully_connected_eps}/{len(endpoint_nodes)}")
        print(f"- 部分连接的端点（1个SW）: {partially_connected_eps}/{len(endpoint_nodes)}")
        if isolated_eps > 0:
            print(f"- 错误：存在孤立端点: {isolated_eps} - 请增加交换机数量或每交换机端口数")
        else:
            print(f"- 所有端点均已连接，无孤立节点")

        # 5. 保存拓扑文件
        topology = {
            "metadata": {
                "description": "TSN network topology (>500 nodes)",
                "total_nodes": len(nodes),
                "total_switches": self.TOTAL_SWITCHES,
                "total_endpoints": self.TOTAL_ENDPOINTS,
                "total_links": len(links),
                "switch_port_bandwidth_mbps": self.SWITCH_PORT_BANDWIDTH,
                "endpoint_port_bandwidth_mbps": f"{self.ENDPOINT_PORT_BANDWIDTH_RANGE[0]}-{self.ENDPOINT_PORT_BANDWIDTH_RANGE[1]}",
                "generated_by": "TSNTopologyGenerator v5.0",
                "ep_connection_rule": "Each EP connects to exactly 2 different SWs for redundancy",
                "fully_connected_endpoints": fully_connected_eps,
                "partially_connected_endpoints": partially_connected_eps,
                "isolated_endpoints": isolated_eps
            },
            "nodes": nodes,
            "links": links
        }

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(topology, f, indent=2, ensure_ascii=False)

        print(f"\n拓扑文件已生成: {filename}")
        print(f"- 节点总数: {len(nodes)} (交换机: {self.TOTAL_SWITCHES}, 端点: {self.TOTAL_ENDPOINTS})")
        print(f"- 链路总数: {len(links)}")
        print(f"- 交换机端口带宽: {self.SWITCH_PORT_BANDWIDTH} Mbps")
        print(f"- 端点端口带宽: {self.ENDPOINT_PORT_BANDWIDTH_RANGE[0]}-{self.ENDPOINT_PORT_BANDWIDTH_RANGE[1]} Mbps (连续随机)")

        return topology


if __name__ == "__main__":
    generator = TSNTopologyGenerator()
    topology = generator.generate_mesh_topology("tsn_mesh_550_dual.json")
    print("\nTSN拓扑生成完成！")