#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大规模TSN网络流量调度器 - 多帧调度
数据流分解为多个数据帧进行调度
每个帧独立分配时隙
接收和发送端口都进行时隙分配
支持周期性流的完整调度
所有时延单位：微秒(us)

重调度机制：
对于失败率 < 50% 的流，对失败帧进行重调度
 重试时通过增加时间偏移来寻找新的可用时隙
"""

import heapq
import json
import time
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
import logging
from threading import Lock


# 配置参数


TOPOLOGY_FILE = "tsn_mesh_550_dual.json"
FLOWS_FILE = "tsn_st_flows_id_priority.json"

# 网络时序参数（微秒）
DEFAULT_FRAME_SIZE = 64
FRAME_GAP_US = 50.0
OVERLAP_TOLERANCE = 0.1
FORWARDING_DELAY_FACTOR = 0.8

# 调度策略参数
PERIOD_DELAY_FACTOR = 0.95  # 帧截止时间 = 周期 × 95%，留出5%安全余量

# 重调度参数
MAX_FRAME_RETRY_ATTEMPTS = 5  # 失败帧的最大重试次数
STREAM_FAILURE_THRESHOLD = 0.5  # 流失败率阈值，超过50%则放弃该流
TIME_OFFSET_RETRY_STEP = 10.0  # 重试时的时间偏移（微秒）

# 性能要求参数
MIN_NODES_REQUIREMENT = 500
MIN_STREAMS_REQUIREMENT = 1000
MAX_SCHEDULING_TIME = 3600
MAX_LATENCY_FACTOR = 0.8

# 单位转换
MS_TO_US = 1000
US_TO_MS = 0.001

# 其他参数
SLOT_SEARCH_TOLERANCE = 0.05
RECENT_STREAMS_COUNT = 5
FAILED_STREAMS_DISPLAY_LIMIT = 10
LINK_UTIL_WEIGHT = 100
MAX_PATH_LENGTH_FACTOR = 2
URGENCY_PRIORITY_WEIGHT = 1000000

# 光纤参数
LIGHT_SPEED_IN_FIBER = 2e8  # m/s
LINK_LENGTH_M = 100  # 米

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# 工具函数


def get_port_transmission_time(frame_size: int, port_id: str, topology) -> float:
    """计算帧在端口的传输时间（微秒）"""
    port = topology.ports.get(port_id)
    if not port:
        bandwidth_bps = 1_000_000_000  # 端口不存在时使用默认带宽1Gbps
    else:
        bandwidth_bps = port.bandwidth * 1_000_000  # 将Mbps转换为bps

    trans_time_us = (frame_size * 8) / bandwidth_bps * 1_000_000  # 帧大小(字节)×8转比特，除以带宽，乘以1000000转微秒
    return trans_time_us


def get_port_occupation_time(frame_size: int, port_id: str, topology) -> float:
    """计算端口完整占用时间 = 传输时间 + 50us间隔（微秒）"""
    trans_time = get_port_transmission_time(frame_size, port_id, topology)
    return trans_time + FRAME_GAP_US  # 加上帧间隙时间


def lookup_switching_delay(frame_size_bytes: int) -> float:
    """根据帧大小查找交换机内部转发延时（微秒）

    这是交换机转发延时的实际计算方式，根据帧大小动态确定。
    返回值范围：3.0 - 20.0 微秒
    """
    if 64 <= frame_size_bytes < 95:
        return 3.0
    elif 95 <= frame_size_bytes < 128:
        return 3.5
    elif 128 <= frame_size_bytes < 173:
        return 4.0
    elif 173 <= frame_size_bytes < 256:
        return 5.0
    elif 256 <= frame_size_bytes < 316:
        return 5.5
    elif 316 <= frame_size_bytes < 376:
        return 6.0
    elif 376 <= frame_size_bytes < 436:
        return 7.0
    elif 436 <= frame_size_bytes < 512:
        return 9.0
    elif 512 <= frame_size_bytes < 612:
        return 10.0
    elif 612 <= frame_size_bytes < 712:
        return 11.0
    elif 712 <= frame_size_bytes < 812:
        return 12.0
    elif 812 <= frame_size_bytes < 912:
        return 13.0
    elif 912 <= frame_size_bytes < 1112:
        return 15.0
    elif 1112 <= frame_size_bytes < 1308:
        return 18.0
    elif 1308 <= frame_size_bytes <= 1518:
        return 20.0
    else:
        return 20.0


def extract_port_info(port_id: str) -> Dict:
    """
    从端口ID提取详细信息
    例如: SW1_p0_to_SW2_p1
    """
    try:
        parts = port_id.split('_to_')  # 按 '_to_' 分割端口ID
        if len(parts) != 2:
            return {'raw_id': port_id}  # 格式不正确，返回原始ID

        # 解析源端口: SW1_p0
        src_parts = parts[0].rsplit('_p', 1)  # 从右侧按 '_p' 分割
        src_node = src_parts[0]  # 源节点名称
        src_port_num = int(src_parts[1]) if len(src_parts) > 1 else -1  # 源端口号

        # 解析目标端口: SW2_p1
        dst_parts = parts[1].rsplit('_p', 1)  # 从右侧按 '_p' 分割
        dst_node = dst_parts[0]  # 目标节点名称
        dst_port_num = int(dst_parts[1]) if len(dst_parts) > 1 else -1  # 目标端口号

        return {
            'node': src_node,
            'port_num': src_port_num,
            'connected_node': dst_node,
            'connected_port_num': dst_port_num,
            'raw_id': port_id
        }
    except:
        return {'raw_id': port_id}  # 解析失败，返回原始ID



# 数据类

class Priority:
    """优先级类"""

    def __init__(self, value: int):
        self.value = value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __eq__(self, other):
        return self.value == other.value

    def __ne__(self, other):
        return self.value != other.value

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return f"Priority({self.value})"


@dataclass
class Frame:
    """数据帧"""
    frame_id: str
    flow_id: str
    frame_index: int
    size_bytes: int
    period_start_us: float  # 周期开始时间
    deadline_us: float  # 截止时间
    status: str = "PENDING"  # PENDING/SCHEDULED/FAILED
    schedule: List[Dict] = field(default_factory=list)
    e2e_latency_us: float = 0.0
    final_arrival_us: float = 0.0


@dataclass
class TSNStream:
    """T数据流（包含多个帧）"""
    stream_id: str
    src_node: str
    dst_nodes: List[str]
    period_us: float  # 周期(us)
    frame_size: int  # 单帧大小(bytes)
    total_size_bytes: int  # 总数据大小
    num_frames: int  # 帧数量
    priority: Priority
    redundancy: int = 1
    max_latency: float = 0

    # 帧列表
    frames: List[Frame] = field(default_factory=list)

    # 调度状态
    scheduled_frames: int = 0
    failed_frames: int = 0

    def __post_init__(self):
        if self.max_latency == 0:
            self.max_latency = self.period_us / 2  # 默认最大延时为周期的一半

        # 初始化帧列表
        if not self.frames:
            for i in range(self.num_frames):  # 为每个帧创建Frame对象
                frame = Frame(
                    frame_id=f"{self.stream_id}_F{i}",  # 帧ID = 流ID + 帧索引
                    flow_id=self.stream_id,
                    frame_index=i,  # 帧在流中的序号
                    size_bytes=self.frame_size,
                    period_start_us=i * self.period_us,  # 第i帧的起始时间 = i × 周期
                    deadline_us=(i + 1) * self.period_us * PERIOD_DELAY_FACTOR  # 截止时间 = (i+1) × 周期 × 0.95
                )
                self.frames.append(frame)

    @property
    def all_frames_scheduled(self) -> bool:
        """所有帧是否都已调度"""
        return self.scheduled_frames == self.num_frames

    @property
    def success_rate(self) -> float:
        """帧调度成功率"""
        return self.scheduled_frames / self.num_frames if self.num_frames > 0 else 0


@dataclass
class NetworkPort:
    port_id: str
    node_id: str
    connected_node: str
    connected_port: str
    bandwidth: int  # Mbps
    current_util: float = 0.0
    port_number: int = -1  # 端口号
    connected_port_number: int = -1  # 连接的对端端口号


@dataclass
class NetworkLink:
    src: str
    dst: str
    src_port: str
    dst_port: str
    bandwidth: int  # Mbps
    prop_delay: float  # 传播延时(us)
    current_util: float = 0.0


@dataclass
class NetworkNode:
    node_id: str
    forward_delay: float  # 转发延时(us)
    is_switch: bool = True
    ports: Dict[str, NetworkPort] = field(default_factory=dict)


@dataclass
class PathSegment:
    """路径段信息"""
    src_node: str
    dst_node: str
    src_port: str
    dst_port: str
    link: NetworkLink
    segment_index: int


@dataclass
class DetailedPath:
    """详细路径信息"""
    path_nodes: List[str]
    path_segments: List[PathSegment]
    total_delay: float
    path_id: str


@dataclass
class ScheduleEntry:
    """调度条目"""
    stream_id: str
    frame_id: str
    node_id: str
    port_id: str  # 发送端口ID
    recv_port_id: str  # 接收端口ID
    send_offset: float  # 发送开始时间(us)
    recv_offset: float  # 接收开始时间(us)
    frame_size: int
    path_index: int = 0
    segment_index: int = 0



# 拓扑管理类


class EnhancedTopologyWithPorts:
    """端口感知拓扑管理器"""

    def __init__(self):
        self.nodes: Dict[str, NetworkNode] = {}
        self.links: Dict[Tuple[str, str], NetworkLink] = {}
        self.ports: Dict[str, NetworkPort] = {}
        self.adjacency: Dict[str, List[str]] = defaultdict(list)
        self.port_adjacency: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        self.path_cache: Dict[Tuple[str, str], List[DetailedPath]] = {}
        self.port_mapping: Dict[str, Dict] = {}  #端口映射表

    def add_node(self, node: NetworkNode):
        self.nodes[node.node_id] = node  # 将节点加入节点字典

    def add_port(self, port: NetworkPort):
        self.ports[port.port_id] = port  # 将端口加入端口字典
        if port.node_id in self.nodes:
            self.nodes[port.node_id].ports[port.port_id] = port  # 将端口加入所属节点的端口字典

        # 存储端口映射信息，用于后续导出
        self.port_mapping[port.port_id] = {
            'node': port.node_id,
            'port_number': port.port_number,
            'connected_node': port.connected_node,
            'connected_port': port.connected_port,
            'connected_port_number': port.connected_port_number,
            'bandwidth': port.bandwidth
        }

    def add_link(self, link: NetworkLink):
        self.links[(link.src, link.dst)] = link  # 将链路加入链路字典，键为(源节点,目标节点)元组
        self.adjacency[link.src].append(link.dst)  # 将目标节点加入源节点的邻接列表
        self.port_adjacency[link.src].append((link.dst, link.src_port))  # 将(目标节点,源端口)加入端口邻接列表

    def find_shortest_path_with_ports(self, src: str, dst: str) -> Optional[DetailedPath]:
        """Dijkstra最短路径算法"""
        cache_key = (src, dst)
        if cache_key in self.path_cache and self.path_cache[cache_key]:
            return self.path_cache[cache_key][0]  # 如果缓存中有路径，直接返回

        if src == dst:
            return DetailedPath(path_nodes=[src], path_segments=[], total_delay=0, path_id=f"{src}-{dst}-0")  # 源和目标相同，返回空路径

        distances = {src: 0}  # 记录从源到各节点的最短距离
        predecessors = {}  # 记录每个节点的前驱节点
        unvisited = [(0, src)]  # 优先队列，存储(距离,节点)
        visited = set()  # 已访问节点集合

        while unvisited:
            current_dist, current = heapq.heappop(unvisited)  # 取出距离最小的未访问节点
            if current in visited:
                continue  # 节点已访问过，跳过
            visited.add(current)  # 标记为已访问

            if current == dst:  # 找到目标节点，开始回溯路径
                path_nodes = []
                path_segments = []
                node = current

                while node is not None:  # 从目标节点回溯到源节点
                    path_nodes.append(node)
                    if node in predecessors:
                        pred = predecessors[node]
                        if (pred, node) in self.links:
                            link = self.links[(pred, node)]  # 获取链路信息
                            segment = PathSegment(  # 创建路径段对象
                                src_node=pred, dst_node=node,
                                src_port=link.src_port, dst_port=link.dst_port,
                                link=link, segment_index=len(path_segments)
                            )
                            path_segments.append(segment)
                    node = predecessors.get(node)

                path_nodes.reverse()  # 反转节点列表（原本是从目标到源）
                path_segments.reverse()  # 反转路径段列表
                for i, segment in enumerate(path_segments):
                    segment.segment_index = i  # 重新编号路径段索引

                detailed_path = DetailedPath(
                    path_nodes=path_nodes, path_segments=path_segments,
                    total_delay=current_dist, path_id=f"{src}-{dst}-0"
                )
                self.path_cache[cache_key] = [detailed_path]  # 缓存路径
                return detailed_path

            for neighbor_node, out_port in self.port_adjacency[current]:  # 遍历当前节点的所有邻居
                if neighbor_node in visited or (current, neighbor_node) not in self.links:
                    continue  # 邻居已访问或链路不存在，跳过

                link = self.links[(current, neighbor_node)]  # 获取到邻居的链路
                port_weight = 0
                if out_port in self.ports:
                    port_weight = self.ports[out_port].current_util * LINK_UTIL_WEIGHT  # 端口利用率作为权重

                new_dist = current_dist + link.prop_delay + port_weight  # 计算新距离 = 当前距离 + 链路延时 + 端口权重

                if neighbor_node not in distances or new_dist < distances[neighbor_node]:  # 找到更短的路径
                    distances[neighbor_node] = new_dist  # 更新最短距离
                    predecessors[neighbor_node] = current  # 更新前驱节点
                    heapq.heappush(unvisited, (new_dist, neighbor_node))  # 将邻居加入优先队列

        return None  # 没有找到路径

    def export_port_mapping_table(self, filename: str):
        """导出端口映射表"""
        # 按节点分组
        node_ports = defaultdict(list)
        for port_id, port_info in self.port_mapping.items():
            node = port_info['node']
            node_ports[node].append({
                'port_id': port_id,
                'port_number': port_info['port_number'],
                'connected_node': port_info['connected_node'],
                'connected_port': port_info['connected_port'],
                'connected_port_number': port_info['connected_port_number'],
                'bandwidth_mbps': port_info['bandwidth']
            })

        # 排序
        for node in node_ports:
            node_ports[node].sort(key=lambda x: x['port_number'])

        mapping_data = {
            'metadata': {
                'total_ports': len(self.port_mapping),
                'total_nodes': len(node_ports),
                'description': 'TSN网络端口映射表 - 每个端口的详细连接信息',
                'port_naming_format': 'NodeA_pX_to_NodeB_pY (NodeA的X号端口连接到NodeB的Y号端口)'
            },
            'port_mapping_by_node': dict(node_ports),
            'all_ports': self.port_mapping
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)

        logger.info(f"端口映射表已导出: {filename}")



# 约束检查器类（改进版 - 支持帧级调度）


class PortAwareConstraintChecker:
    """端口感知约束检查器（多帧调度版）"""

    def __init__(self, topology):
        self.topology = topology
        self.port_schedules: Dict[str, List[Tuple[float, float, str]]] = defaultdict(list)
        self.node_schedules: Dict[str, List[Tuple[float, float, str]]] = defaultdict(list)
        self.lock = Lock()

    def check_essential_constraints_with_ports(self, frame: Frame, entry: ScheduleEntry,
                                               path_entries: List[ScheduleEntry], topology) -> bool:
        """检查关键约束（帧级别）"""
        if not self._check_frame_deadline(frame, path_entries):  # 检查帧是否在截止时间内完成
            return False
        if not self._check_recv_port_conflicts(entry, frame):  # 检查接收端口是否有冲突
            return False
        if not self._check_send_port_conflicts(entry, frame):  # 检查发送端口是否有冲突
            return False
        if not self._check_recv_send_timing(entry, frame):  # 检查接收-发送时序关系
            return False
        if not self._check_forwarding_delay_flexible(entry, topology):  # 检查转发延时是否满足
            return False
        if not self._check_bandwidth_limit_flexible(entry):  # 检查带宽限制
            return False
        return True  # 所有约束都满足

    def _check_frame_deadline(self, frame: Frame, path_entries: List[ScheduleEntry]) -> bool:
        """检查帧是否满足deadline"""
        if not path_entries:
            return True  # 没有调度条目，默认通过

        first_entry = path_entries[0]
        start_time = first_entry.send_offset  # 帧的开始发送时间

        last_entry = path_entries[-1]  # 路径上的最后一跳
        last_trans_time = get_port_transmission_time(
            last_entry.frame_size, last_entry.port_id, self.topology)

        if last_entry.node_id in self.topology.nodes:
            node = self.topology.nodes[last_entry.node_id]
            if node.is_switch:
                end_time = last_entry.send_offset + last_trans_time  # 交换机：发送完成时间
            else:
                recv_trans_time = get_port_transmission_time(
                    last_entry.frame_size, last_entry.recv_port_id, self.topology)
                end_time = last_entry.recv_offset + recv_trans_time  # 终端设备：接收完成时间
        else:
            recv_trans_time = get_port_transmission_time(
                last_entry.frame_size, last_entry.recv_port_id, self.topology)
            end_time = last_entry.recv_offset + recv_trans_time

        total_delay = end_time - start_time  # 端到端延时
        return end_time <= frame.deadline_us  # 检查是否在截止时间内完成

    def _check_recv_port_conflicts(self, entry: ScheduleEntry, frame: Frame) -> bool:
        """接收端口冲突检查"""
        recv_port_occupation_time = get_port_occupation_time(
            entry.frame_size, entry.recv_port_id, self.topology)

        recv_start = entry.recv_offset  # 接收开始时间
        recv_end = recv_start + recv_port_occupation_time  # 接收结束时间

        with self.lock:  # 加锁保证线程安全
            for start, end, _ in self.port_schedules[entry.recv_port_id]:  # 遍历该端口已有的调度
                if not (recv_end <= start + OVERLAP_TOLERANCE or
                        recv_start >= end - OVERLAP_TOLERANCE):  # 检查时间区间是否重叠
                    return False  # 有冲突，返回False
        return True  # 无冲突，返回True

    def _check_send_port_conflicts(self, entry: ScheduleEntry, frame: Frame) -> bool:
        """发送端口冲突检查"""
        send_port_occupation_time = get_port_occupation_time(
            entry.frame_size, entry.port_id, self.topology)

        send_start = entry.send_offset  # 发送开始时间
        send_end = send_start + send_port_occupation_time  # 发送结束时间

        with self.lock:  # 加锁保证线程安全
            for start, end, _ in self.port_schedules[entry.port_id]:  # 检查端口调度冲突
                if not (send_end <= start + OVERLAP_TOLERANCE or
                        send_start >= end - OVERLAP_TOLERANCE):
                    return False  # 有冲突

            for start, end, _ in self.node_schedules[entry.node_id]:  # 检查节点调度冲突
                if not (send_end <= start + OVERLAP_TOLERANCE or
                        send_start >= end - OVERLAP_TOLERANCE):
                    return False  # 有冲突
        return True  # 无冲突

    def _check_recv_send_timing(self, entry: ScheduleEntry, frame: Frame) -> bool:
        """接收-发送时序检查"""
        recv_trans_time = get_port_transmission_time(
            entry.frame_size, entry.recv_port_id, self.topology)

        recv_complete_time = entry.recv_offset + recv_trans_time  # 接收完成时间

        if entry.node_id in self.topology.nodes:
            node = self.topology.nodes[entry.node_id]
            if node.is_switch:
                return entry.send_offset >= recv_complete_time  # 交换机：发送必须在接收完成后

        return True  # 非交换机节点，无此要求

    def _check_forwarding_delay_flexible(self, entry: ScheduleEntry, topology) -> bool:
        """转发延时检查"""
        node = topology.nodes.get(entry.node_id)
        if not node or not node.is_switch:
            return True  # 非交换机节点，无转发延时要求

        recv_trans_time = get_port_transmission_time(
            entry.frame_size, entry.recv_port_id, topology)

        recv_complete_time = entry.recv_offset + recv_trans_time  # 接收完成时间
        forwarding_delay = entry.send_offset - recv_complete_time  # 实际转发延时

        min_required_delay = node.forward_delay * FORWARDING_DELAY_FACTOR  # 最小要求延时 = 节点延时 × 0.8
        return forwarding_delay >= min_required_delay  # 检查实际延时是否满足最小要求

    def _check_bandwidth_limit_flexible(self, entry: ScheduleEntry) -> bool:
        """带宽检查"""
        return entry.recv_offset >= 0 and entry.send_offset >= entry.recv_offset  # 时间必须为正且发送在接收之后

    def add_schedule_entry(self, entry: ScheduleEntry):
        """添加调度条目"""
        with self.lock:  # 加锁
            recv_port_occupation_time = get_port_occupation_time(
                entry.frame_size, entry.recv_port_id, self.topology)
            recv_time_interval = (entry.recv_offset, entry.recv_offset + recv_port_occupation_time, entry.frame_id)
            self.port_schedules[entry.recv_port_id].append(recv_time_interval)  # 记录接收端口占用时间

            send_port_occupation_time = get_port_occupation_time(
                entry.frame_size, entry.port_id, self.topology)
            send_time_interval = (entry.send_offset, entry.send_offset + send_port_occupation_time, entry.frame_id)
            self.port_schedules[entry.port_id].append(send_time_interval)  # 记录发送端口占用时间

            self.node_schedules[entry.node_id].append(send_time_interval)  # 记录节点占用时间

    def remove_schedule_entry(self, entry: ScheduleEntry):
        """移除调度条目"""
        with self.lock:  # 加锁
            recv_port_occupation_time = get_port_occupation_time(
                entry.frame_size, entry.recv_port_id, self.topology)
            recv_target_interval = (entry.recv_offset, entry.recv_offset + recv_port_occupation_time, entry.frame_id)
            if recv_target_interval in self.port_schedules[entry.recv_port_id]:
                self.port_schedules[entry.recv_port_id].remove(recv_target_interval)  # 移除接收端口占用记录

            send_port_occupation_time = get_port_occupation_time(
                entry.frame_size, entry.port_id, self.topology)
            send_target_interval = (entry.send_offset, entry.send_offset + send_port_occupation_time, entry.frame_id)
            if send_target_interval in self.port_schedules[entry.port_id]:
                self.port_schedules[entry.port_id].remove(send_target_interval)  # 移除发送端口占用记录

            if send_target_interval in self.node_schedules[entry.node_id]:
                self.node_schedules[entry.node_id].remove(send_target_interval)  # 移除节点占用记录

    def find_available_slot_for_port(self, preferred_time: float, frame: Frame,
                                     port_id: str, node_id: str, topology) -> float:
        """为端口查找可用时隙（微秒）

        采用贪心策略向后搜索可用时隙，最大搜索次数为1000次。
        如果超过最大迭代次数，返回当前候选时间（可能导致冲突）。
        """
        port_occupation_time = get_port_occupation_time(
            frame.size_bytes, port_id, topology)

        with self.lock:
            all_occupied_slots = []
            all_occupied_slots.extend([(s, e) for s, e, _ in self.port_schedules[port_id]])  # 收集端口的已占用时隙
            all_occupied_slots.extend([(s, e) for s, e, _ in self.node_schedules[node_id]])  # 收集节点的已占用时隙
            occupied_slots = sorted(set(all_occupied_slots))  # 去重并排序

        if not occupied_slots:
            return preferred_time  # 没有占用时隙，直接返回期望时间

        candidate_time = preferred_time  # 候选时间从期望时间开始
        max_iterations = 1000  # 时隙搜索最大迭代次数
        iteration = 0

        while iteration < max_iterations:
            candidate_end = candidate_time + port_occupation_time  # 候选时隙的结束时间

            has_conflict = False  # 是否有冲突
            conflict_end = 0  # 冲突时隙的结束时间

            for start, end in occupied_slots:  # 检查是否与已占用时隙冲突
                if candidate_time < end - SLOT_SEARCH_TOLERANCE and candidate_end > start + SLOT_SEARCH_TOLERANCE:
                    has_conflict = True  # 有冲突
                    conflict_end = end  # 记录冲突时隙的结束时间
                    break

            if not has_conflict:
                return candidate_time  # 无冲突，返回候选时间
            else:
                candidate_time = conflict_end  # 将候选时间移到冲突时隙的结束时间
                iteration += 1  # 迭代次数+1

        logger.warning(f"find_available_slot_for_port exceeded max iterations for port {port_id}")
        return candidate_time  # 超过最大迭代次数，返回当前候选时间



# 调度器类（多帧调度版）


class MultiFrameScheduler:
    """多帧调度器"""

    def __init__(self, topology: EnhancedTopologyWithPorts):
        self.topology = topology
        self.constraint_checker = PortAwareConstraintChecker(topology)
        self.schedule: List[ScheduleEntry] = []
        self.frame_schedules: Dict[str, List[ScheduleEntry]] = {}
        self.successful_streams: Set[str] = set()
        self.schedule_lock = Lock()

    def schedule_all_streams(self, streams: List[TSNStream]) -> Dict:
        """主调度入口（多帧版本，支持重调度）"""
        start_time = time.time()
        logger.info(f"开始多帧调度 {len(streams)} 条流（支持失败帧重调度）")

        sorted_streams = self._sort_streams_optimized(streams)  # 按优先级和周期排序流

        total_frames = sum(s.num_frames for s in streams)  # 计算总帧数
        scheduled_frames = 0  # 成功调度的帧数
        failed_frames = 0  # 失败的帧数
        retry_attempts = 0  # 统计重试次数

        for stream_idx, stream in enumerate(sorted_streams):
            if (stream_idx + 1) % 100 == 0:
                logger.info(f"  调度进度: {stream_idx + 1}/{len(streams)} 流")

            # 第一轮：正常调度所有帧
            for frame in stream.frames:
                if self._schedule_single_frame(frame, stream):  # 尝试调度帧
                    stream.scheduled_frames += 1
                    scheduled_frames += 1
                else:
                    stream.failed_frames += 1
                    failed_frames += 1

            # 检查流的失败率
            failure_rate = stream.failed_frames / stream.num_frames if stream.num_frames > 0 else 0

            # 如果失败率 < 50%，对失败的帧进行重调度
            if 0 < failure_rate < STREAM_FAILURE_THRESHOLD:
                logger.info(f"  流 {stream.stream_id} 失败率 {failure_rate:.1%}，尝试重调度失败帧")
                failed_frame_list = [f for f in stream.frames if f.status == "FAILED"]  # 收集失败的帧

                for retry_round in range(MAX_FRAME_RETRY_ATTEMPTS):  # 最多重试5次
                    if not failed_frame_list:
                        break  # 没有失败帧了，退出

                    retry_attempts += 1
                    time_offset = (retry_round + 1) * TIME_OFFSET_RETRY_STEP  # 计算时间偏移：10us, 20us, 30us...

                    # 对每个失败帧重试
                    remaining_failed = []  # 存储仍然失败的帧
                    for frame in failed_frame_list:
                        if self._schedule_single_frame(frame, stream, time_offset):  # 使用时间偏移重试
                            stream.scheduled_frames += 1  # 更新成功计数
                            stream.failed_frames -= 1
                            scheduled_frames += 1
                            failed_frames -= 1
                        else:
                            remaining_failed.append(frame)  # 仍然失败，加入列表

                    failed_frame_list = remaining_failed  # 更新失败帧列表

                    if not failed_frame_list:
                        logger.info(f"  流 {stream.stream_id} 重调度成功，所有帧已调度")
                        break  # 所有帧都成功了，退出重试循环

            if stream.all_frames_scheduled:
                self.successful_streams.add(stream.stream_id)  # 将成功的流加入成功集合

        end_time = time.time()
        scheduling_time = end_time - start_time

        result = {
            'total_streams': len(streams),
            'scheduled_streams': len(self.successful_streams),
            'partially_scheduled_streams': len(
                [s for s in streams if s.scheduled_frames > 0 and not s.all_frames_scheduled]),
            'failed_streams': len(streams) - len(self.successful_streams),
            'total_frames': total_frames,
            'scheduled_frames': scheduled_frames,
            'failed_frames': failed_frames,
            'frame_success_rate': scheduled_frames / total_frames if total_frames > 0 else 0,
            'stream_success_rate': len(self.successful_streams) / len(streams) if streams else 0,
            'scheduling_time': scheduling_time,
            'schedule_entries': len(self.schedule),
            'retry_attempts': retry_attempts  # 新增：重试次数统计
        }

        logger.info(f"多帧调度完成: {scheduled_frames}/{total_frames} 帧 "
                    f"({result['frame_success_rate']:.1%}) 用时 {scheduling_time:.2f}秒 "
                    f"重试次数: {retry_attempts}")

        return result

    def _sort_streams_optimized(self, streams: List[TSNStream]) -> List[TSNStream]:
        """优化的流排序"""

        def sort_key(stream):
            urgency = stream.priority.value * URGENCY_PRIORITY_WEIGHT + stream.period_us
            return (stream.priority.value, stream.period_us, -stream.redundancy, urgency)

        return sorted(streams, key=sort_key)

    def _schedule_single_frame(self, frame: Frame, stream: TSNStream, time_offset: float = 0.0) -> bool:
        """调度单个帧

        Args:
            frame: 要调度的帧
            stream: 所属数据流
            time_offset: 时间偏移量（微秒），用于重试时调整起始时间

        Returns:
            bool: 调度成功返回True，失败返回False
        """
        try:
            detailed_path = self.topology.find_shortest_path_with_ports(stream.src_node, stream.dst_nodes[0])
            if not detailed_path:
                frame.status = "FAILED"
                return False

            path_entries = self._schedule_frame_on_path(frame, stream, detailed_path, 0, time_offset)

            if path_entries:
                all_valid = True
                for entry in path_entries:
                    if not self.constraint_checker.check_essential_constraints_with_ports(
                            frame, entry, path_entries, self.topology):
                        all_valid = False
                        break

                if all_valid:
                    with self.schedule_lock:
                        self.schedule.extend(path_entries)
                        self.frame_schedules[frame.frame_id] = path_entries

                        for entry in path_entries:
                            self.constraint_checker.add_schedule_entry(entry)

                    frame.status = "SCHEDULED"

                    first_entry = path_entries[0]
                    last_entry = path_entries[-1]
                    last_trans_time = get_port_transmission_time(
                        last_entry.frame_size, last_entry.port_id, self.topology)
                    frame.e2e_latency_us = (last_entry.send_offset + last_trans_time) - first_entry.send_offset
                    frame.final_arrival_us = last_entry.send_offset + last_trans_time

                    return True

            frame.status = "FAILED"
            return False

        except Exception as e:
            logger.error(f"Error scheduling frame {frame.frame_id}: {e}")
            frame.status = "FAILED"
            return False

    def _schedule_frame_on_path(self, frame: Frame, stream: TSNStream, detailed_path: DetailedPath,
                                path_idx: int, time_offset: float = 0.0) -> List[ScheduleEntry]:
        """在路径上调度帧

        Args:
            time_offset: 时间偏移量，用于重试时调整起始时间
        """
        path_entries = []
        current_time = frame.period_start_us + time_offset

        if detailed_path.path_segments:
            first_segment = detailed_path.path_segments[0]

            src_trans_time_us = get_port_transmission_time(
                frame.size_bytes, first_segment.src_port, self.topology)

            send_offset = self.constraint_checker.find_available_slot_for_port(
                current_time, frame, first_segment.src_port, first_segment.src_node, self.topology)

            src_entry = ScheduleEntry(
                stream_id=stream.stream_id,
                frame_id=frame.frame_id,
                node_id=first_segment.src_node,
                port_id=first_segment.src_port,
                recv_port_id=first_segment.src_port,
                send_offset=send_offset,
                recv_offset=send_offset,
                frame_size=frame.size_bytes,
                path_index=path_idx,
                segment_index=0
            )
            path_entries.append(src_entry)
            current_time = send_offset + src_trans_time_us

        for segment in detailed_path.path_segments:
            link_propagation_delay_us = LINK_LENGTH_M / LIGHT_SPEED_IN_FIBER * 1_000_000
            ideal_arrival_time = current_time + link_propagation_delay_us

            recv_port_id = segment.dst_port
            actual_recv_offset = self.constraint_checker.find_available_slot_for_port(
                ideal_arrival_time, frame, recv_port_id, segment.dst_node, self.topology)

            recv_trans_time_us = get_port_transmission_time(
                frame.size_bytes, recv_port_id, self.topology)

            recv_complete_time = actual_recv_offset + recv_trans_time_us

            if segment.dst_node in self.topology.nodes and self.topology.nodes[segment.dst_node].is_switch:
                switch_forwarding_delay_us = lookup_switching_delay(frame.size_bytes)
                ideal_forward_time = recv_complete_time + switch_forwarding_delay_us

                next_segment_index = segment.segment_index + 1
                if next_segment_index < len(detailed_path.path_segments):
                    next_segment = detailed_path.path_segments[next_segment_index]
                    out_port = next_segment.src_port
                else:
                    out_port = segment.dst_port

                actual_send_offset = self.constraint_checker.find_available_slot_for_port(
                    ideal_forward_time, frame, out_port, segment.dst_node, self.topology)

                send_trans_time_us = get_port_transmission_time(
                    frame.size_bytes, out_port, self.topology)

                entry = ScheduleEntry(
                    stream_id=stream.stream_id,
                    frame_id=frame.frame_id,
                    node_id=segment.dst_node,
                    port_id=out_port,
                    recv_port_id=recv_port_id,
                    send_offset=actual_send_offset,
                    recv_offset=actual_recv_offset,
                    frame_size=frame.size_bytes,
                    path_index=path_idx,
                    segment_index=segment.segment_index + 1
                )
                path_entries.append(entry)
                current_time = actual_send_offset + send_trans_time_us
            else:
                final_entry = ScheduleEntry(
                    stream_id=stream.stream_id,
                    frame_id=frame.frame_id,
                    node_id=segment.dst_node,
                    port_id=recv_port_id,
                    recv_port_id=recv_port_id,
                    send_offset=actual_recv_offset,
                    recv_offset=actual_recv_offset,
                    frame_size=frame.size_bytes,
                    path_index=path_idx,
                    segment_index=segment.segment_index + 1
                )
                path_entries.append(final_entry)

        return path_entries

    def export_complete_schedule_json(self, filename: str, streams: List[TSNStream]):
        """导出调度表JSON（多帧版本 - 增强端口信息）"""
        schedule_data = {
            'metadata': {
                'total_nodes': len(self.topology.nodes),
                'total_links': len(self.topology.links),
                'total_ports': len(self.topology.ports),
                'total_streams': len(streams),
                'total_scheduled_streams': len(self.successful_streams),
                'total_frames': sum(s.num_frames for s in streams),
                'total_scheduled_frames': sum(s.scheduled_frames for s in streams),
                'total_schedule_entries': len(self.schedule),
                'generation_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'algorithm': 'TSN Multi-Frame Scheduler v4 (Enhanced Port Info)',
                'time_unit': 'microseconds (us)',
                'port_naming_format': 'NodeA_pX_to_NodeB_pY (NodeA的X号端口连接到NodeB的Y号端口)'
            },
            'streams': {}
        }

        for stream in streams:
            stream_data = {
                'stream_info': {
                    'source': stream.src_node,
                    'destination': stream.dst_nodes[0] if stream.dst_nodes else 'unknown',
                    'priority': stream.priority.value,
                    'period_us': stream.period_us,
                    'frame_size': stream.frame_size,
                    'num_frames': stream.num_frames,
                    'scheduled_frames': stream.scheduled_frames,
                    'failed_frames': stream.failed_frames,
                    'success_rate': stream.success_rate
                },
                'frames': []
            }

            for frame in stream.frames:
                if frame.status != "SCHEDULED":
                    continue

                frame_entries = self.frame_schedules.get(frame.frame_id, [])
                if not frame_entries:
                    continue

                hop_details = []
                for entry in sorted(frame_entries, key=lambda e: e.segment_index):
                    recv_trans_time = get_port_transmission_time(
                        entry.frame_size, entry.recv_port_id, self.topology)

                    send_trans_time = get_port_transmission_time(
                        entry.frame_size, entry.port_id, self.topology)

                    node = self.topology.nodes.get(entry.node_id)
                    if node and node.is_switch:
                        recv_complete_time = entry.recv_offset + recv_trans_time
                        forwarding_delay = lookup_switching_delay(entry.frame_size)
                        ideal_send_time = recv_complete_time + forwarding_delay
                        send_wait_time = max(0, entry.send_offset - ideal_send_time)
                        hop_total_delay = (entry.send_offset + send_trans_time) - entry.recv_offset
                    else:
                        forwarding_delay = 0
                        send_wait_time = 0
                        hop_total_delay = recv_trans_time

                    # 提取端口详细信息
                    recv_port_info = extract_port_info(entry.recv_port_id)
                    send_port_info = extract_port_info(entry.port_id)

                    hop_details.append({
                        'node': entry.node_id,
                        'recv_port': {
                            'port_id': entry.recv_port_id,
                            'port_number': recv_port_info.get('port_num', -1),
                            'from_node': recv_port_info.get('connected_node', 'unknown'),
                            'from_port_number': recv_port_info.get('connected_port_num', -1)
                        },
                        'send_port': {
                            'port_id': entry.port_id,
                            'port_number': send_port_info.get('port_num', -1),
                            'to_node': send_port_info.get('connected_node', 'unknown'),
                            'to_port_number': send_port_info.get('connected_port_num', -1)
                        },
                        'recv_time_us': entry.recv_offset,
                        'send_time_us': entry.send_offset,
                        'recv_trans_time_us': recv_trans_time,
                        'send_trans_time_us': send_trans_time,
                        'forwarding_delay_us': forwarding_delay,
                        'send_wait_time_us': send_wait_time,
                        'hop_total_delay_us': hop_total_delay
                    })

                frame_data = {
                    'frame_id': frame.frame_id,
                    'frame_index': frame.frame_index,
                    'period_start_us': frame.period_start_us,
                    'deadline_us': frame.deadline_us,
                    'final_arrival_us': frame.final_arrival_us,
                    'e2e_latency_us': frame.e2e_latency_us,
                    'status': frame.status,
                    'hops': hop_details
                }
                stream_data['frames'].append(frame_data)

            schedule_data['streams'][stream.stream_id] = stream_data

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schedule_data, f, indent=2, ensure_ascii=False)

        logger.info(f"调度表已保存到: {filename}")
        return filename



# 数据加载函数


def load_topology_with_ports_from_json(topology_file: str) -> EnhancedTopologyWithPorts:
    """从JSON加载拓扑

    注意：
    - 交转发延时由 lookup_switching_delay() 根据帧大小动态计算

    """
    with open(topology_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    topology = EnhancedTopologyWithPorts()

    for node_data in data['nodes']:
        if node_data['type'] == 'switch':

            forward_delay = 0.0
            is_switch = True
        else:

            forward_delay = 0.0
            is_switch = False

        node = NetworkNode(
            node_id=node_data['id'],
            forward_delay=forward_delay,
            is_switch=is_switch
        )
        topology.add_node(node)

    # 为每个节点维护端口计数器
    port_counter = {}

    # 第一遍：收集所有链路，建立端口映射关系
    link_port_map = {}  # (src, dst) -> (src_port_num, dst_port_num)

    for link_data in data['links']:
        src_node = link_data['source']
        dst_node = link_data['target']

        if src_node not in port_counter:
            port_counter[src_node] = 0  # 初始化源节点的端口计数器
        if dst_node not in port_counter:
            port_counter[dst_node] = 0  # 初始化目标节点的端口计数器

        src_port_num = port_counter[src_node]  # 为源节点分配端口号
        dst_port_num = port_counter[dst_node]  # 为目标节点分配端口号

        link_port_map[(src_node, dst_node)] = (src_port_num, dst_port_num)  # 记录端口映射

        port_counter[src_node] += 1  # 源节点端口计数器+1
        port_counter[dst_node] += 1  # 目标节点端口计数器+1

    # 第二遍：创建端口和链路（使用完整的端口命名）
    for link_data in data['links']:
        src_node = link_data['source']
        dst_node = link_data['target']

        src_port_num, dst_port_num = link_port_map[(src_node, dst_node)]  # 从映射表获取端口号

        # 端口ID格式：NodeA_pX_to_NodeB_pY
        src_port_id = f"{src_node}_p{src_port_num}_to_{dst_node}_p{dst_port_num}"
        dst_port_id = f"{dst_node}_p{dst_port_num}_to_{src_node}_p{src_port_num}"

        # 创建端口对象（包含端口号信息）
        src_port = NetworkPort(  # 创建源端口
            port_id=src_port_id,
            node_id=src_node,
            connected_node=dst_node,
            connected_port=dst_port_id,
            bandwidth=link_data['bandwidth_mbps'],
            port_number=src_port_num,
            connected_port_number=dst_port_num
        )

        dst_port = NetworkPort(  # 创建目标端口
            port_id=dst_port_id,
            node_id=dst_node,
            connected_node=src_node,
            connected_port=src_port_id,
            bandwidth=link_data['bandwidth_mbps'],
            port_number=dst_port_num,
            connected_port_number=src_port_num
        )

        topology.add_port(src_port)  # 将源端口加入拓扑
        topology.add_port(dst_port)  # 将目标端口加入拓扑

        latency_us = 0.5  # 固定链路延时0.5us

        # 创建正向链路（src -> dst）
        forward_link = NetworkLink(
            src=src_node,
            dst=dst_node,
            src_port=src_port_id,
            dst_port=dst_port_id,
            bandwidth=link_data['bandwidth_mbps'],
            prop_delay=latency_us
        )
        topology.add_link(forward_link)  # 将正向链路加入拓扑

        # 创建反向链路（dst -> src），实现双向通信
        reverse_link = NetworkLink(
            src=dst_node,
            dst=src_node,
            src_port=dst_port_id,
            dst_port=src_port_id,
            bandwidth=link_data['bandwidth_mbps'],
            prop_delay=latency_us
        )
        topology.add_link(reverse_link)  # 将反向链路加入拓扑

    logger.info(f"加载拓扑完成: {len(topology.nodes)}节点, "
                f"{len(topology.links)}链路, {len(topology.ports)}端口")

    # 打印端口映射示例（前10个端口）
    logger.info("端口映射示例（增强格式）:")
    port_sample = list(topology.ports.items())[:10]
    for port_id, port in port_sample:
        logger.info(f"  {port_id}")
        logger.info(f"    -> 节点{port.node_id}的端口{port.port_number} 连接到 "
                    f"节点{port.connected_node}的端口{port.connected_port_number}")

    return topology


def load_streams_from_json(flows_file: str) -> List[TSNStream]:
    """从JSON加载流（多帧版本）"""
    with open(flows_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    streams = []
    for flow_data in data['st_flows']:  # 遍历所有流
        period_us = flow_data.get('period_us', 0)  # 获取周期（微秒）
        if period_us == 0:  # 如果没有微秒单位的周期
            period_ms = flow_data.get('period_ms', 1.0)  # 获取毫秒单位的周期
            period_us = period_ms * MS_TO_US  # 转换为微秒

        priority = Priority(flow_data['priority'])  # 创建优先级对象
        frame_size = flow_data.get('frame_size_bytes', DEFAULT_FRAME_SIZE)  # 获取帧大小，默认64字节
        total_size = flow_data.get('total_size_bytes', frame_size)  # 获取总数据大小
        num_frames = flow_data.get('num_frames', 1)  # 获取帧数量，默认1帧

        stream = TSNStream(  # 创建TSN流对象
            stream_id=flow_data['flow_id'],
            src_node=flow_data['source_node'],
            dst_nodes=[flow_data['destination_node']],
            period_us=period_us,
            frame_size=frame_size,
            total_size_bytes=total_size,
            num_frames=num_frames,
            priority=priority,
            redundancy=flow_data.get('redundancy_count', 1),  # 冗余计数，默认1
            max_latency=period_us * MAX_LATENCY_FACTOR  # 最大延时 = 周期 × 0.8
        )
        streams.append(stream)  # 将流加入列表

    total_frames = sum(s.num_frames for s in streams)  # 计算所有流的总帧数
    logger.info(f"加载流完成: {len(streams)}条流, {total_frames}个帧")
    return streams



# 主函数


def main():
    """主函数"""

    # 加载网络拓扑
    topology = load_topology_with_ports_from_json(TOPOLOGY_FILE)
    # 加载数据流
    streams = load_streams_from_json(FLOWS_FILE)
    # 创建调度器
    scheduler = MultiFrameScheduler(topology)

    total_frames = sum(s.num_frames for s in streams)  # 计算总帧数
    print(f"\n拓扑: {len(topology.nodes)}节点, {len(topology.links)}链路, {len(topology.ports)}端口")
    print(f"流: {len(streams)}条流, {total_frames}个帧")

    result = scheduler.schedule_all_streams(streams)  # 执行调度

    print(f"\n{'=' * 60}")
    print("调度结果（多帧版本）")
    print(f"{'=' * 60}")
    print(f"总流数: {result['total_streams']}")
    print(f"完全成功流: {result['scheduled_streams']}")
    print(f"部分成功流: {result['partially_scheduled_streams']}")
    print(f"失败流: {result['failed_streams']}")
    print(f"流成功率: {result['stream_success_rate']:.1%}")
    print(f"\n总帧数: {result['total_frames']}")
    print(f"成功帧: {result['scheduled_frames']}")
    print(f"失败帧: {result['failed_frames']}")
    print(f"帧成功率: {result['frame_success_rate']:.1%}")
    print(f"\n调度时间: {result['scheduling_time']:.2f}秒")
    print(f"重试轮次: {result['retry_attempts']}")

    # 统计各类流的数量
    fully_scheduled = len([s for s in streams if s.all_frames_scheduled])  # 完全成功的流
    partially_scheduled = len([s for s in streams if s.scheduled_frames > 0 and not s.all_frames_scheduled])  # 部分成功的流
    print(f"\n流调度详情:")
    print(f"  完全成功: {fully_scheduled}/{len(streams)}")
    print(f"  部分成功: {partially_scheduled}/{len(streams)}")
    print(f"  完全失败: {len(streams) - fully_scheduled - partially_scheduled}/{len(streams)}")

    # 统计端口利用率
    total_ports = len(topology.ports)  # 总端口数
    used_ports = len([p for p in scheduler.constraint_checker.port_schedules
                      if scheduler.constraint_checker.port_schedules[p]])  # 已使用的端口数
    print(f"\n端口利用: {used_ports}/{total_ports} ({used_ports / total_ports:.1%})")

    # 检查是否满足性能要求
    meets_node_req = len(topology.nodes) >= MIN_NODES_REQUIREMENT  # 节点数量要求
    meets_stream_req = len(streams) >= MIN_STREAMS_REQUIREMENT  # 流数量要求
    meets_time_req = result['scheduling_time'] <= MAX_SCHEDULING_TIME  # 时间要求

    timestamp = int(time.time())  # 获取当前时间戳

    # 导出调度表
    json_filename = f"schedule_multiframe_v4_enhanced_.json"
    scheduler.export_complete_schedule_json(json_filename, streams)

    # 导出端口映射表
    port_mapping_file = f"port_mapping_table_.json"
    topology.export_port_mapping_table(port_mapping_file)

    # 导出结果摘要
    result_file = f"result_multiframe_v4_enhanced_.json"
    with open(result_file, 'w') as f:
        json.dump({
            'metadata': {
                'algorithm': 'TSN Multi-Frame Scheduler v4 (Enhanced Port Info)',
                'time_unit': 'microseconds (us)',
                'port_naming_format': 'NodeA_pX_to_NodeB_pY',
                'nodes': len(topology.nodes),
                'streams': len(streams),
                'total_frames': total_frames,
                'timestamp': timestamp
            },
            'results': result,
            'requirements_met': {
                'nodes_500plus': meets_node_req,
                'streams_1000plus': meets_stream_req,
                'time_60min': meets_time_req,
            },
            'stream_statistics': {
                'fully_scheduled': fully_scheduled,
                'partially_scheduled': partially_scheduled,
                'completely_failed': len(streams) - fully_scheduled - partially_scheduled
            }
        }, f, indent=2)


if __name__ == "__main__":
    main()