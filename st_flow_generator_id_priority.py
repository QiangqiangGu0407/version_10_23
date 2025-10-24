"""
TSN ST流信息生成器
生成ST流信息：源节点、目的节点、数据流大小、数据帧大小、通信周期、优先级
优先级直接等于流ID（1-1000）
"""

import json
import random


class IDPriorityTSNFlowGenerator:
    """优先级=流ID的TSN流信息生成器"""

    def __init__(self):
        # ST流数量配置
        self.TOTAL_ST_FLOWS = 1000

        # 航空数据流类型配置（按比例分配）
        # 注意：优先级不在这里设置，而是直接使用流ID
        self.FLOW_TYPES = {
            # 类型名称: (比例, 数据流大小范围(字节), 数据帧大小范围(字节), 周期范围(us))
            "flight_control": (0.15, (128, 512), (64, 256), (1000, 5000)),  # 飞控（15%）
            "sensor_high_freq": (0.20, (200, 1000), (100, 500), (1000, 10000)),  # 高频传感器（20%）
            "sensor_normal": (0.25, (500, 2000), (200, 1000), (10000, 50000)),  # 普通传感器（25%）
            "large_data": (0.15, (10000, 50000), (1000, 1518), (1000, 10000)),  # 大数据流（15%）
            "config_management": (0.10, (5000, 20000), (500, 1518), (10000, 50000)),  # 配置管理（10%）
            "health_monitoring": (0.10, (1000, 5000), (500, 1518), (20000, 50000)),  # 健康监测（10%）
            "video_stream": (0.05, (100000, 300000), (1518, 1518), (10000, 30000)),  # 视频流（5%）
        }

    def _select_flow_type(self):
        """根据比例随机选择流类型"""
        rand = random.random()
        cumulative_prob = 0

        for flow_type, (prob, *_) in self.FLOW_TYPES.items():
            cumulative_prob += prob
            if rand < cumulative_prob:
                return flow_type

        # 默认返回传感器类型
        return "sensor_normal"

    def _generate_flow_params(self, flow_type):
        """根据流类型生成参数"""
        _, total_size_range, frame_size_range, period_range = self.FLOW_TYPES[flow_type]

        # 数据帧大小（字节，限制在64-1518）
        frame_size = random.randint(*frame_size_range)
        frame_size = max(64, min(1518, frame_size))  # 确保在范围内

        # 数据流总大小（字节）- 必须大于等于帧大小
        total_size_min = max(total_size_range[0], frame_size)  # 至少等于帧大小
        total_size_max = total_size_range[1]
        total_size = random.randint(total_size_min, total_size_max)

        # 周期（微秒）
        period = random.randint(*period_range)

        # 计算需要的帧数
        import math
        num_frames = math.ceil(total_size / frame_size)

        return {
            "total_size": total_size,
            "frame_size": frame_size,
            "period_us": period,
            "num_frames": num_frames,
            "flow_type": flow_type
        }


    def load_topology(self, filename="tsn_mesh_550_dual.json"):
        """加载拓扑文件"""
        try:
            with open(filename, "r", encoding="utf-8") as f:
                topology = json.load(f)
            return topology["nodes"], topology.get("links", [])
        except FileNotFoundError:
            print(f"错误: 找不到拓扑文件 {filename}")
            return None, None
        except Exception as e:
            print(f"错误: 读取拓扑文件失败 - {e}")
            return None, None

    def generate_st_flows(self,
                          topology_filename="tsn_mesh_550_dual.json",
                          output_filename="tsn_st_flows_id_priority.json"):
        """生成ST流信息（优先级=流ID）"""

        # 加载拓扑
        nodes, links = self.load_topology(topology_filename)
        if nodes is None or links is None:
            return None

        print(f"开始生成{self.TOTAL_ST_FLOWS}个TSN ST流（优先级=流ID）...")
        print(f"拓扑信息: {len(nodes)} 个节点")

        # 提取端点节点
        endpoint_nodes = [node["id"] for node in nodes if node["type"] == "endpoint"]
        print(f"可用端点: {len(endpoint_nodes)} 个")

        if len(endpoint_nodes) < 2:
            print("错误: 端点数量不足（至少需要2个）")
            return None

        st_flows = []
        flow_type_count = {ft: 0 for ft in self.FLOW_TYPES.keys()}

        for flow_id in range(1, self.TOTAL_ST_FLOWS + 1):
            # 选择流类型
            flow_type = self._select_flow_type()
            flow_type_count[flow_type] += 1

            # 生成流参数
            params = self._generate_flow_params(flow_type)

            # 源节点：从端点中随机选择
            source_node = random.choice(endpoint_nodes)

            # 目的节点：从端点中随机选择（排除源节点）
            available_destinations = [ep for ep in endpoint_nodes if ep != source_node]
            destination_node = random.choice(available_destinations)

            # 优先级 = 流ID（1-1000，数字越小优先级越高）
            priority = flow_id

            # 构建ST流数据
            st_flow = {
                "flow_id": f"ST_{flow_id:04d}",
                "flow_type": params["flow_type"],
                "source_node": source_node,
                "destination_node": destination_node,

                # 数据流参数
                "total_size_bytes": params["total_size"],
                "frame_size_bytes": params["frame_size"],
                "num_frames": params["num_frames"],

                # 时间参数
                "period_us": params["period_us"],
                "period_ms": params["period_us"] / 1000,  # 同时提供ms单位

                # 延迟约束（默认为周期的90%）
                "deadline_us": int(params["period_us"] * 0.9),

                # 优先级 = 流ID
                "priority": priority
            }

            st_flows.append(st_flow)

        # 保存ST流信息
        st_flow_data = {
            "metadata": {
                "description": "TSN ST flows where priority equals flow ID",
                "total_st_flows": len(st_flows),
                "flow_types": flow_type_count,
                "frame_size_range_bytes": (64, 1518),
                "period_range_us": (1000, 50000),
                "priority_range": (1, self.TOTAL_ST_FLOWS),
                "priority_description": f"Priority = Flow ID (1 to {self.TOTAL_ST_FLOWS}, smaller number = higher priority)",
                "topology_file": topology_filename,
                "generated_by": "IDPriorityTSNFlowGenerator v1.0"
            },
            "st_flows": st_flows
        }

        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(st_flow_data, f, indent=2, ensure_ascii=False)

        # 打印统计信息
        self._print_statistics(st_flows, flow_type_count)

        print(f"\n✓ ST流信息已生成: {output_filename}")
        return st_flow_data

    def _generate_description(self, flow_type, total_size, num_frames, priority):
        """生成流的描述"""
        type_descriptions = {
            "flight_control": "飞行控制指令",
            "sensor_high_freq": "高频传感器数据",
            "sensor_normal": "普通传感器数据",
            "large_data": "大数据传输",
            "config_management": "配置管理数据",
            "health_monitoring": "健康监测数据",
            "video_stream": "视频流数据"
        }

        desc = type_descriptions.get(flow_type, "未知类型")
        if num_frames > 1:
            desc += f" (多帧传输: {num_frames}帧)"

        return desc

    def _print_statistics(self, st_flows, flow_type_count):
        """打印统计信息"""
        print("\n" + "=" * 70)
        print("流生成统计（优先级=流ID模式）")
        print("=" * 70)

        # 流类型分布
        print("\n流类型分布:")
        for flow_type, count in sorted(flow_type_count.items(), key=lambda x: -x[1]):
            percentage = count / len(st_flows) * 100
            print(f"  {flow_type:20s}: {count:4d} ({percentage:5.1f}%)")

        # 优先级分布说明
        print("\n优先级分布:")
        print(f"  优先级范围: 1-{len(st_flows)} (1=最高优先级, {len(st_flows)}=最低优先级)")
        print(f"  分配规则: 优先级 = 流ID")
        print(f"  示例:")
        print(f"    ST_0001 → 优先级 1 (最高)")
        print(f"    ST_0500 → 优先级 500 (中等)")
        print(f"    ST_1000 → 优先级 1000 (最低)")

        # 数据大小统计
        total_sizes = [flow["total_size_bytes"] for flow in st_flows]
        frame_sizes = [flow["frame_size_bytes"] for flow in st_flows]
        num_frames_list = [flow["num_frames"] for flow in st_flows]

        print(f"\n数据流大小统计:")
        print(f"  最小: {min(total_sizes)} 字节")
        print(f"  最大: {max(total_sizes)} 字节")
        print(f"  平均: {sum(total_sizes)//len(total_sizes)} 字节")

        print(f"\n数据帧大小统计:")
        print(f"  最小: {min(frame_sizes)} 字节")
        print(f"  最大: {max(frame_sizes)} 字节")
        print(f"  平均: {sum(frame_sizes)//len(frame_sizes)} 字节")

        print(f"\n帧数统计:")
        print(f"  单帧流: {sum(1 for n in num_frames_list if n == 1)} ({sum(1 for n in num_frames_list if n == 1)/len(st_flows)*100:.1f}%)")
        print(f"  多帧流: {sum(1 for n in num_frames_list if n > 1)} ({sum(1 for n in num_frames_list if n > 1)/len(st_flows)*100:.1f}%)")
        print(f"  最大帧数: {max(num_frames_list)}")
        print(f"  平均帧数: {sum(num_frames_list)//len(num_frames_list)}")

        # 周期统计
        periods = [flow["period_us"] for flow in st_flows]
        print(f"\n周期统计:")
        print(f"  最小: {min(periods)} us ({min(periods)/1000:.1f} ms)")
        print(f"  最大: {max(periods)} us ({max(periods)/1000:.1f} ms)")
        print(f"  平均: {sum(periods)//len(periods)} us ({sum(periods)//len(periods)/1000:.1f} ms)")


if __name__ == "__main__":
    generator = IDPriorityTSNFlowGenerator()

    # 可调整参数
    # generator.TOTAL_ST_FLOWS = 1500  # 流数量

    st_flows = generator.generate_st_flows(
        topology_filename="tsn_mesh_550_dual.json",
        output_filename="tsn_st_flows_id_priority.json"
    )

    if st_flows:
        print("\n✓ ST流信息生成完成！")
    else:
        print("\n✗ ST流信息生成失败！")
