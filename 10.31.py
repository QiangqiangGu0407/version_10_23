# 基于区间树的高效调度算法
import json
import time
import math
import heapq
import logging
import sys
import os
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict
from intervaltree import Interval, IntervalTree
#修改如下：
#路径查找过程中对经过的节点，判断是否是端节点还是交换机
#路径查找阶段，对路径评分机制修改为跳数，路径使用次数，端口使用次数的加权，同时对不同优先级区间的数据流设置不同的权重。
# 此外，严格设置所有数据帧完成调度，该数据流完成调度，并更新路径使用次数和端口使用次数
# 日志配置


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('tsn_scheduler.log', mode='w')
    ]
)
logger = logging.getLogger(__name__)


# 全局常量


# 物理常量
LINK_LENGTH_M = 100.0
LIGHT_SPEED_IN_FIBER = 2e8  # m/s
PROPAGATION_DELAY_US = (LINK_LENGTH_M / LIGHT_SPEED_IN_FIBER) * 1e6  # 约0.5us
FRAME_GAP_US = 50.0

# 调度参数
K_PATHS = 20 # K最短路径数量
MAX_PORT_UTILIZATION = 0.9  # 端口最大利用率80%
FRER_MIN_DISJOINT = 0.8  # FRER最小分离度60%
FRER_MAX_LATENCY_DIFF_US = 10.0  # FRER最大延时差10us
FRER_TIME_OFFSET_US = 10.0  # FRER时间偏移±10us

# 交换转发延时表 (字节范围 - 延时us)
SWITCHING_DELAY_TABLE = {
    (64, 95): 3.0,
    (95, 128): 3.5,
    (128, 173): 4.0,
    (173, 256): 5.0,
    (256, 316): 5.5,
    (316, 376): 6.0,
    (376, 436): 7.0,
    (436, 512): 9.0,
    (512, 612): 10.0,
    (612, 712): 11.0,
    (712, 812): 12.0,
    (812, 912): 13.0,
    (912, 1112): 15.0,
    (1112, 1308): 18.0,
    (1308, 1518): 20.0
}


DEFAULT_SAFETY_MARGIN_US = 100.0  # 默认安全余量（微秒）
ENABLE_TEMPORAL_VALIDATION = True  # 是否启用时序约束验证



# 路径评分权重配置（根据优先级区间）
PRIORITY_WEIGHT_CONFIG = {
    'high': {'hop': 0.70, 'path': 0.15, 'port': 0.15},      # 高优先级 [0-300]
    'medium': {'hop': 0.50, 'path': 0.25, 'port': 0.25},    # 中优先级 [400-600]
    'low': {'hop': 0.30, 'path': 0.35, 'port': 0.35}        # 低优先级 [700+]
}

# 优先级区间定义
HIGH_PRIORITY_MAX = 300
MEDIUM_PRIORITY_MAX = 600


# 辅助函数


def extract_port_info(port_id: str) -> Dict:
    """
    从端口ID提取详细信息
    例如: SW1_p0_to_SW2_p1
    """
    try:
        parts = port_id.split('_to_')  # 按 '_to_' 分割端口ID
        if len(parts) != 2:  # 格式不正确，返回基本信息
            return {'raw_id': port_id, 'node': port_id.split('_')[0] if '_' in port_id else port_id}

        # 解析源端口（例如 SW1_p0）
        src_parts = parts[0].rsplit('_p', 1)  # 从右侧按 '_p' 分割
        src_node = src_parts[0]  # 源节点名称
        src_port_num = int(src_parts[1]) if len(src_parts) > 1 else -1  # 源端口号

        # 解析目标端口（例如 SW2_p1）
        dst_parts = parts[1].rsplit('_p', 1)  # 从右侧按 '_p' 分割
        dst_node = dst_parts[0]  # 目标节点名称
        dst_port_num = int(dst_parts[1]) if len(dst_parts) > 1 else -1  # 目标端口号

        return {
            'node': src_node,  # 源节点
            'port_num': src_port_num,  # 源端口号
            'connected_node': dst_node,  # 目标节点
            'connected_port_num': dst_port_num,  # 目标端口号
            'raw_id': port_id  # 原始端口ID
        }
    except:
        return {'raw_id': port_id, 'node': 'unknown'}  # 解析失败，返回默认值


def lookup_switching_delay(frame_size: int) -> float:
    """查找交换延时"""
    for (min_size, max_size), delay in SWITCHING_DELAY_TABLE.items():  # 遍历延时表
        if min_size <= frame_size < max_size:  # 找到对应的帧大小区间
            return delay  # 返回该区间的延时值
    return 20.0  # 默认最大延时（帧大小超出表范围）


def calculate_transmission_time(frame_size: int, bandwidth_bps: float) -> float:
    """计算传输时间（微秒）"""
    return (frame_size * 8) / bandwidth_bps * 1e6  # 帧大小(字节)×8转比特，除以带宽(bps)，乘以1e6转换为微秒


# ============================================================================
# 数据结构
# ============================================================================

@dataclass
class Frame:
    """数据帧"""
    frame_id: str  # 帧ID，格式：流ID-FrameX
    flow_id: str  # 所属流ID
    frame_index: int  # 帧在流中的索引号
    size_bytes: int  # 帧大小（字节）
    period_start: float  # 周期开始时间（微秒）
    deadline: float  # 截止时间（微秒）
    status: str = "PENDING"  # 帧状态：PENDING/SCHEDULING/SCHEDULED/FAILED
    schedule: List[Dict] = field(default_factory=list)  # 调度结果（每跳的详细信息）
    e2e_latency: float = 0.0  # 端到端延时（微秒）
    final_arrival: float = 0.0  # 最终到达时间（微秒）


@dataclass
class PathSegment:
    """路径段"""
    src_node: str  # 源节点ID
    dst_node: str  # 目标节点ID
    send_port_id: str  # 发送端口ID（完整格式：NodeA_pX_to_NodeB_pY）
    recv_port_id: str  # 接收端口ID（完整格式：NodeB_pY_to_NodeA_pX）
    send_port_num: int = -1  # 发送端口号（数字）
    recv_port_num: int = -1  # 接收端口号（数字）
    bandwidth_bps: float = 0.0  # 链路带宽（bps）
    segment_index: int = 0  # 该段在路径中的索引号


@dataclass
class DetailedPath:
    """详细路径"""
    path_id: str  # 路径ID，格式：源-目标-pathX
    path_nodes: List[str]  # 路径上的节点列表
    segments: List[PathSegment]  # 路径段列表（每一跳的详细信息）
    total_hops: int  # 总跳数
    estimated_latency_us: float  # 估计延时（微秒）
    score: float = 0.0  # 路径评分（用于路径选择）


@dataclass
class Flow:
    """数据流"""
    flow_id: str  # 流ID
    flow_type: str  # 流类型
    source: str  # 源节点ID
    destination: str  # 目标节点ID
    priority: int  # 优先级（数值越小优先级越高）
    period_us: float  # 周期（微秒）
    deadline_us: float  # 截止时间（微秒）
    frame_size_bytes: int  # 单帧大小（字节）
    total_size_bytes: int  # 总数据大小（字节）
    num_frames: int  # 帧数量

    # 路径相关
    path_candidates: List[DetailedPath] = field(default_factory=list)  # 候选路径列表
    selected_path: Optional[DetailedPath] = None  # 选中的主路径
    backup_path: Optional[DetailedPath] = None  # 备用路径（用于FRER冗余）

    # 调度相关
    frames: List[Frame] = field(default_factory=list)  # 该流的所有帧
    scheduled: bool = False  # 是否已调度成功
    frer_enabled: bool = False  # 是否启用FRER冗余机制

    # 时序关系相关字段
    is_temporal: bool = False  # 是否是时序关系流
    temporal_order: int = -1  # 在时序链中的顺序（-1表示非时序流）
    delayed_start_time: float = 0.0  # 延迟启动时间（微秒）
    temporal_predecessor_id: str = ""  # 前驱流ID（用于验证）
    actual_finish_time: float = 0.0  # 实际完成时间（调度结果）

    def __post_init__(self):
        """初始化帧列表"""
        if not self.frames:  # 如果帧列表为空，自动创建帧
            for i in range(self.num_frames):  # 为每个帧创建Frame对象
                frame = Frame(
                    frame_id=f"{self.flow_id}-Frame{i}",  # 帧ID = 流ID + 帧索引
                    flow_id=self.flow_id,  # 所属流ID
                    frame_index=i,  # 帧索引
                    size_bytes=self.frame_size_bytes,  # 帧大小
                    period_start=i * self.period_us,  # 第i帧的开始时间 = i × 周期
                    deadline=(i + 1) * self.period_us  # 截止时间 = (i+1) × 周期
                )
                self.frames.append(frame)  # 添加到帧列表


# 时序流管理器（新增）

class TemporalFlowManager:
    """时序流管理器"""

    def __init__(self, safety_margin_us: float = DEFAULT_SAFETY_MARGIN_US):
        self.temporal_flow_ids: List[str] = []  # 时序流ID列表（按顺序）
        self.temporal_flows: List[Flow] = []  # 时序流对象列表
        self.non_temporal_flows: List[Flow] = []  # 非时序流列表
        self.safety_margin_us: float = safety_margin_us  # 安全余量
        self.start_times: Dict[str, float] = {}  # 每条流的启动时间
        self.all_flows: List[Flow] = []  # 所有流的引用

    def set_temporal_flows(self, flow_ids: List[str], all_flows: List[Flow]):
        """
        设置时序关系流

        Args:
            flow_ids: 时序流ID列表（按时序先后顺序）
            all_flows: 所有流的列表
        """
        if not flow_ids or len(flow_ids) == 0:
            logger.info("未配置时序关系流")
            return

        if len(flow_ids) < 2:
            logger.warning(f"时序关系流数量不足（{len(flow_ids)}条），至少需要2条流")
            return

        self.temporal_flow_ids = flow_ids
        self.all_flows = all_flows


        logger.info("配置时序关系流")
        logger.info(f"时序流数量: {len(flow_ids)}")
        logger.info(f"时序链: {' → '.join(flow_ids)}")
        logger.info(f"安全余量: {self.safety_margin_us}us")

        # 验证和标记
        self._validate_and_mark_flows()

        # 计算启动时间
        self._calculate_start_times()

        # 分组流
        self._group_flows()

        logger.info("=" * 80)

    def _validate_and_mark_flows(self):
        """验证流ID并标记时序流"""
        flow_dict = {f.flow_id: f for f in self.all_flows}

        for i, flow_id in enumerate(self.temporal_flow_ids):
            # 检查流是否存在
            if flow_id not in flow_dict:
                raise ValueError(f"时序流ID '{flow_id}' 不存在于流列表中")

            flow = flow_dict[flow_id]

            # 标记时序流
            flow.is_temporal = True
            flow.temporal_order = i

            # 设置前驱流ID
            if i > 0:
                flow.temporal_predecessor_id = self.temporal_flow_ids[i - 1]

            self.temporal_flows.append(flow)

            logger.info(f"  {i + 1}. {flow_id} (帧数: {flow.num_frames}, 周期: {flow.period_us}us)")

    def _calculate_start_times(self):
        """计算所有时序流的启动时间"""
        logger.info("\n计算时序流启动时间:")
        logger.info("-" * 80)

        # 第一条流从0开始
        first_flow = self.temporal_flows[0]
        self.start_times[first_flow.flow_id] = 0.0
        first_flow.delayed_start_time = 0.0

        logger.info(f"流 {first_flow.flow_id}:")
        logger.info(f"  启动时间: 0.0 us (首条流)")

        # 计算后续流的启动时间
        for i in range(1, len(self.temporal_flows)):
            current_flow = self.temporal_flows[i]
            prev_flow = self.temporal_flows[i - 1]

            # 计算时间跨度 T = (帧数 - 1) × 周期
            T_prev = (prev_flow.num_frames) * prev_flow.period_us
            T_current = (current_flow.num_frames) * current_flow.period_us

            # 前驱流的启动时间
            prev_start = self.start_times[prev_flow.flow_id]

            # 根据公式计算当前流的启动时间
            if T_prev > T_current:
                # 情况A：前驱流持续时间更长
                current_start = prev_start + (T_prev - T_current) + prev_flow.period_us#self.safety_margin_us
                reason = f"T_prev({T_prev:.1f}us) > T_current({T_current:.1f}us)"
            else:
                # 情况B：当前流持续时间更长或相等
                current_start = prev_start +   prev_flow.period_us #self.safety_margin_us
                reason = f"T_prev({T_prev:.1f}us) <= T_current({T_current:.1f}us)"

            self.start_times[current_flow.flow_id] = current_start
            current_flow.delayed_start_time = current_start

            # 预估完成时间
            # prev_finish = prev_start + T_prev
            # current_finish = current_start + T_current
            # time_margin = current_finish - prev_finish
            #
            # logger.info(f"\n流 {current_flow.flow_id}:")
            # logger.info(f"  启动时间: {current_start:.2f} us")
            # logger.info(f"  计算依据: {reason}")
            # logger.info(f"  前驱流预估完成: {prev_finish:.2f} us")
            # logger.info(f"  当前流预估完成: {current_finish:.2f} us")
            # logger.info(f"  时序余量: {time_margin:.2f} us")

        # 更新所有帧的period_start
        for flow in self.temporal_flows:
            for frame in flow.frames:
                frame.period_start = flow.delayed_start_time + frame.frame_index * flow.period_us
                frame.deadline = flow.delayed_start_time + (frame.frame_index + 1) * flow.period_us

        logger.info("-" * 80)

    def _group_flows(self):
        """将流分为时序流和非时序流两组"""
        temporal_ids_set = set(self.temporal_flow_ids)

        for flow in self.all_flows:
            if flow.flow_id not in temporal_ids_set:
                self.non_temporal_flows.append(flow)

        logger.info(f"\n流分组:")
        logger.info(f"  时序流: {len(self.temporal_flows)} 条")
        logger.info(f"  非时序流: {len(self.non_temporal_flows)} 条")

    def get_reordered_flows(self) -> List[Flow]:
        """
        获取重排序后的流列表
        时序流在前（按时序顺序），非时序流在后（按优先级排序）

        Returns:
            重排序后的流列表
        """
        # 时序流已经按temporal_order排序
        temporal_sorted = sorted(self.temporal_flows, key=lambda f: f.temporal_order)

        # 非时序流按优先级排序
        non_temporal_sorted = sorted(self.non_temporal_flows,
                                     key=lambda f: (f.priority, f.flow_id))

        # 合并：时序流在前
        reordered = temporal_sorted + non_temporal_sorted

        logger.info("\n流调度顺序（重排序后）:")
        logger.info(f"  时序流部分（前{len(temporal_sorted)}条）:")
        for i, flow in enumerate(temporal_sorted):
            logger.info(f"    {i + 1}. {flow.flow_id} (启动: {flow.delayed_start_time:.2f}us, "
                        f"优先级: {flow.priority})")

        logger.info(f"  非时序流部分（后{len(non_temporal_sorted)}条）:")
        display_count = min(5, len(non_temporal_sorted))
        for i, flow in enumerate(non_temporal_sorted[:display_count]):
            logger.info(f"    {len(temporal_sorted) + i + 1}. {flow.flow_id} "
                        f"(优先级: {flow.priority})")
        if len(non_temporal_sorted) > display_count:
            logger.info(f"    ... 还有{len(non_temporal_sorted) - display_count}条流")

        return reordered

    def has_temporal_flows(self) -> bool:
        """是否有时序流"""
        return len(self.temporal_flows) > 0

    def validate_temporal_constraints(self) -> Tuple[bool, List[Dict]]:
        """
        验证时序约束是否满足

        Returns:
            (是否全部满足, 违规列表)
        """
        if not self.has_temporal_flows():
            return True, []

        violations = []
        all_passed = True

        logger.info("\n" + "=" * 80)
        logger.info("验证时序约束")
        logger.info("=" * 80)

        for i in range(len(self.temporal_flows) - 1):
            prev_flow = self.temporal_flows[i]
            next_flow = self.temporal_flows[i + 1]

            # 检查是否都调度成功
            if not prev_flow.scheduled or not next_flow.scheduled:
                logger.warning(f"流 {prev_flow.flow_id} 或 {next_flow.flow_id} 未成功调度，跳过验证")
                continue

            prev_finish = prev_flow.actual_finish_time
            next_finish = next_flow.actual_finish_time

            # 验证：前驱完成时间 < 后继完成时间
            if prev_finish < next_finish:
                margin = next_finish - prev_finish
                logger.info(f" {prev_flow.flow_id} → {next_flow.flow_id}")
                logger.info(f"    前驱完成: {prev_finish:.2f} us")
                logger.info(f"    后继完成: {next_finish:.2f} us")
                logger.info(f"    时序余量: {margin:.2f} us")
            else:
                all_passed = False
                violation = {
                    'prev_flow': prev_flow.flow_id,
                    'next_flow': next_flow.flow_id,
                    'prev_finish': prev_finish,
                    'next_finish': next_finish,
                    'violation_amount': prev_finish - next_finish
                }
                violations.append(violation)

                logger.error(f"{prev_flow.flow_id} → {next_flow.flow_id} [违规]")
                logger.error(f"    前驱完成: {prev_finish:.2f} us")
                logger.error(f"    后继完成: {next_finish:.2f} us")
                logger.error(f"    违规量: {prev_finish - next_finish:.2f} us")

        logger.info("=" * 80)
        if all_passed:
            logger.info(f" 时序约束验证通过：{len(self.temporal_flows) - 1} 个关系全部满足")
        else:
            logger.error(f" 时序约束验证失败：{len(violations)} 个关系违规")
            logger.error(f"建议：增大安全余量（当前: {self.safety_margin_us}us）")
        logger.info("=" * 80)

        return all_passed, violations




# 区间树优化端口类


class OptimizedPort:
    """使用区间树+缓存优化的端口类"""

    def __init__(self, port_id: str, node_id: str, bandwidth_bps: float,
                 port_number: int = -1, connected_node: str = "",
                 connected_port_number: int = -1):
        self.port_id = port_id  # 端口ID
        self.node_id = node_id  # 所属节点ID
        self.bandwidth_bps = bandwidth_bps  # 端口带宽（bps）
        self.port_number = port_number  # 端口号（数字）
        self.connected_node = connected_node  # 连接的对端节点
        self.connected_port_number = connected_port_number  # 连接的对端端口号
        self.allocated_bps = 0  # 已分配带宽（bps）

        # 区间树（核心数据结构）- 用于高效的时隙冲突检测
        self.interval_tree = IntervalTree()

        # 缓存机制 - 加速时隙查找
        self.next_available_time = 0.0  # 下一个可用时间点
        self.last_query_time = -1.0  # 上次查询的时间
        self.cache_valid = True  # 缓存是否有效

        # 统计信息
        self.slot_count = 0  # 已分配的时隙数量

    @property
    def utilization(self) -> float:
        """端口利用率"""
        return self.allocated_bps / self.bandwidth_bps if self.bandwidth_bps > 0 else 0

    def find_slot(self, start_time: float, duration: float) -> Optional[float]:
        """查找可用时隙（区间树+缓存优化）"""
        required_size = duration + FRAME_GAP_US

        # 快速路径（缓存命中）
        if self.cache_valid and start_time >= self.last_query_time:
            if start_time >= self.next_available_time:
                if not self.interval_tree.overlaps(start_time, start_time + duration):
                    return start_time
                else:
                    self.cache_valid = False

        # 慢速路径（区间树查询）
        gaps = self._find_gaps(start_time, required_size)

        if gaps:
            slot_start = gaps[0][0]
            self.next_available_time = slot_start + duration + FRAME_GAP_US
            self.last_query_time = start_time
            self.cache_valid = True
            return slot_start

        return None

    def _find_gaps(self, earliest: float, min_duration: float) -> List[Tuple[float, float]]:
        """查找间隙"""
        if not self.interval_tree:
            return [(earliest, float('inf'))]

        gaps = []
        current = earliest
        overlapping = sorted(self.interval_tree[earliest:])

        for interval in overlapping:
            if interval.begin - current >= min_duration:
                gaps.append((current, interval.begin))
            current = max(current, interval.end + FRAME_GAP_US)

        gaps.append((current, float('inf')))
        return gaps

    def add_slot(self, start: float, end: float, flow_id: str) -> bool:
        """添加时隙"""
        if self.interval_tree.overlaps(start - FRAME_GAP_US, end + FRAME_GAP_US):
            return False

        self.interval_tree.add(Interval(start, end, data={'flow_id': flow_id}))
        self.slot_count += 1

        if start < self.next_available_time:
            self.cache_valid = False
        else:
            self.next_available_time = end + FRAME_GAP_US

        return True

    def remove_slot(self, start: float, end: float):
        """删除时隙"""
        self.interval_tree.discard(Interval(start, end))
        self.slot_count -= 1
        self.cache_valid = False

    def check_conflict(self, start: float, end: float) -> bool:
        """检查是否有冲突"""
        return self.interval_tree.overlaps(start - FRAME_GAP_US, end + FRAME_GAP_US)



# 拓扑管理器


class TopologyManager:
    """拓扑管理器"""

    def __init__(self, topo_file: str):
        self.nodes = {}
        self.links = {}
        self.ports = {}
        self.adjacency = defaultdict(list)
        self.port_map = {}
        self.port_mapping_table = {}  # 端口映射表

        self._load_topology(topo_file)

    def _load_topology(self, filename: str):
        """加载拓扑（端口命名）"""
        logger.info(f"加载拓扑: {filename}")

        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 加载节点
        for node in data.get('nodes', []):
            node_id = node['id']
            self.nodes[node_id] = {
                'type': node.get('type', 'switch'),
                'ports': node.get('ports', [])
            }

        # 第一遍：收集所有链路，建立端口编号
        port_counter = defaultdict(int)
        link_port_map = {}  # (src, dst) -> (src_port_num, dst_port_num)

        for link in data.get('links', []):
            src = link['source']
            dst = link['target']

            src_port_num = port_counter[src]
            dst_port_num = port_counter[dst]

            link_port_map[(src, dst)] = (src_port_num, dst_port_num)

            port_counter[src] += 1
            port_counter[dst] += 1

        # 第二遍：创建端口和链路（使用完整命名）
        for link in data.get('links', []):
            src = link['source']
            dst = link['target']
            bw_mbps = link.get('bandwidth_mbps', 1000)
            bw_bps = bw_mbps * 1_000_000

            src_port_num, dst_port_num = link_port_map[(src, dst)]

            # 增强的端口ID格式：NodeA_pX_to_NodeB_pY
            src_port_id = f"{src}_p{src_port_num}_to_{dst}_p{dst_port_num}"
            dst_port_id = f"{dst}_p{dst_port_num}_to_{src}_p{src_port_num}"

            # 创建端口对象（包含端口号信息）
            self.ports[src_port_id] = OptimizedPort(
                src_port_id, src, bw_bps,
                port_number=src_port_num,
                connected_node=dst,
                connected_port_number=dst_port_num
            )
            self.ports[dst_port_id] = OptimizedPort(
                dst_port_id, dst, bw_bps,
                port_number=dst_port_num,
                connected_node=src,
                connected_port_number=src_port_num
            )

            # 记录链路
            self.links[(src, dst)] = {'bandwidth_bps': bw_bps}
            self.links[(dst, src)] = {'bandwidth_bps': bw_bps}

            # 邻接表
            self.adjacency[src].append(dst)
            self.adjacency[dst].append(src)

            # 端口映射
            self.port_map[(src, dst)] = (src_port_id, dst_port_id)
            self.port_map[(dst, src)] = (dst_port_id, src_port_id)

            # 端口映射表
            self.port_mapping_table[src_port_id] = {
                'node': src,
                'port_number': src_port_num,
                'connected_node': dst,
                'connected_port': dst_port_id,
                'connected_port_number': dst_port_num,
                'bandwidth_mbps': bw_mbps
            }
            self.port_mapping_table[dst_port_id] = {
                'node': dst,
                'port_number': dst_port_num,
                'connected_node': src,
                'connected_port': src_port_id,
                'connected_port_number': src_port_num,
                'bandwidth_mbps': bw_mbps
            }

        logger.info(f"拓扑加载完成: {len(self.nodes)}节点, {len(self.links) // 2}链路, {len(self.ports)}端口")

        # 打印端口示例
        logger.info("端口映射示例（增强格式）:")
        sample_ports = list(self.ports.items())[:5]
        for port_id, port in sample_ports:
            logger.info(f"  {port_id}")
            logger.info(f"    -> 节点{port.node_id}的端口{port.port_number} 连接到 "
                        f"节点{port.connected_node}的端口{port.connected_port_number}")

    def get_port_pair(self, src: str, dst: str) -> Optional[Tuple[str, str]]:
        """获取端口对"""
        return self.port_map.get((src, dst))

    def export_port_mapping_table(self, filename: str):
        """导出端口映射表"""
        # 按节点分组
        node_ports = defaultdict(list)
        for port_id, port_info in self.port_mapping_table.items():
            node = port_info['node']
            node_ports[node].append({
                'port_id': port_id,
                'port_number': port_info['port_number'],
                'connected_node': port_info['connected_node'],
                'connected_port': port_info['connected_port'],
                'connected_port_number': port_info['connected_port_number'],
                'bandwidth_mbps': port_info['bandwidth_mbps']
            })

        # 排序
        for node in node_ports:
            node_ports[node].sort(key=lambda x: x['port_number'])

        mapping_data = {
            'metadata': {
                'total_ports': len(self.port_mapping_table),
                'total_nodes': len(node_ports),
                'description': 'TSN网络端口映射表 - 每个端口的详细连接信息',
                'port_naming_format': 'NodeA_pX_to_NodeB_pY (NodeA的X号端口连接到NodeB的Y号端口)'
            },
            'port_mapping_by_node': dict(node_ports),
            'all_ports': self.port_mapping_table
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)

        logger.info(f"端口映射表已导出: {filename}")



# 流管理器


class FlowManager:
    """流管理器"""

    def __init__(self, flows_file: str):
        self.flows = []
        self._load_flows(flows_file)

    def _load_flows(self, filename: str):
        """加载流信息"""
        logger.info(f"加载流信息: {filename}")

        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for flow_data in data.get('st_flows', []):
            flow = Flow(
                flow_id=flow_data['flow_id'],
                flow_type=flow_data.get('flow_type', 'unknown'),
                source=flow_data['source_node'],
                destination=flow_data['destination_node'],
                priority=flow_data['priority'],
                period_us=flow_data['period_us'],
                deadline_us=flow_data.get('deadline_us', flow_data['period_us'] * 0.9),
                frame_size_bytes=flow_data['frame_size_bytes'],
                total_size_bytes=flow_data['total_size_bytes'],
                num_frames=flow_data['num_frames']
            )
            self.flows.append(flow)

        logger.info(f"流加载完成: {len(self.flows)}条")

    def sort_by_priority(self) -> List[Flow]:
        """按优先级排序"""
        return sorted(self.flows, key=lambda f: (f.priority, f.flow_id))



# K最短路径算法（Yen算法）


# class YenKShortestPaths:
#     """Yen's K最短路径算法"""
#
#     def __init__(self, adjacency: Dict):
#         self.adj = adjacency

class YenKShortestPaths:
    """Yen's K最短路径算法"""

    def __init__(self, adjacency: Dict, nodes_info: Dict = None):
        self.adj = adjacency
        self.nodes_info = nodes_info if nodes_info else {}


    def dijkstra(self, source: str, destination: str,
                 excluded_edges: set = None,
                 excluded_nodes: set = None) -> Optional[Tuple[List[str], float]]:
        """Dijkstra最短路径"""
        if excluded_edges is None:
            excluded_edges = set()
        if excluded_nodes is None:
            excluded_nodes = set()

        distances = {source: 0}
        previous = {}
        pq = [(0, source)]
        visited = set()

        while pq:
            current_dist, current = heapq.heappop(pq)

            if current in visited:
                continue
            visited.add(current)

            if current == destination:
                path = []
                node = destination
                while node in previous:
                    path.append(node)
                    node = previous[node]
                path.append(source)
                path.reverse()
                return path, current_dist

            if current not in self.adj:
                continue

            # for neighbor in self.adj[current]:
            #     if neighbor in excluded_nodes:
            #         continue
            #     if (current, neighbor) in excluded_edges:
            #         continue
            #
            #     distance = current_dist + 1
            #
            #     if neighbor not in distances or distance < distances[neighbor]:
            #         distances[neighbor] = distance
            #         previous[neighbor] = current
            #         heapq.heappush(pq, (distance, neighbor))
            for neighbor in self.adj[current]:
                if neighbor in excluded_nodes:
                    continue
                if (current, neighbor) in excluded_edges:
                    continue

                # 新增：检查中间节点必须是交换机
                if neighbor != destination:
                    node_info = self.nodes_info.get(neighbor, {})
                    if node_info.get('type', 'switch') != 'switch':
                        continue

                distance = current_dist + 1

                if neighbor not in distances or distance < distances[neighbor]:
                    distances[neighbor] = distance
                    previous[neighbor] = current
                    heapq.heappush(pq, (distance, neighbor))

        return None


    def find_k_shortest_paths(self, source: str, destination: str, k: int) -> List[List[str]]:
        """查找K条最短路径"""
        if source == destination:
            return [[source]]

        A = []
        B = []

        result = self.dijkstra(source, destination)
        if result is None:
            return []

        path, cost = result
        A.append(path)

        for k_iter in range(1, k):
            if not A:
                break

            prev_path = A[k_iter - 1]

            for i in range(len(prev_path) - 1):
                spur_node = prev_path[i]
                root_path = prev_path[:i + 1]

                excluded_edges = set()
                excluded_nodes = set()

                for existing_path in A:
                    if len(existing_path) > i and existing_path[:i + 1] == root_path:
                        if i + 1 < len(existing_path):
                            excluded_edges.add((existing_path[i], existing_path[i + 1]))

                for node in root_path[:-1]:
                    if node != spur_node:
                        excluded_nodes.add(node)

                spur_result = self.dijkstra(spur_node, destination, excluded_edges, excluded_nodes)

                if spur_result:
                    spur_path, spur_cost = spur_result
                    total_path = root_path[:-1] + spur_path
                    total_cost = i + spur_cost

                    path_exists = False
                    for _, existing_path in B:
                        if existing_path == total_path:
                            path_exists = True
                            break

                    for existing_path in A:
                        if existing_path == total_path:
                            path_exists = True
                            break

                    if not path_exists:
                        heapq.heappush(B, (total_cost, total_path))

            if not B:
                break

            _, best_path = heapq.heappop(B)
            A.append(best_path)

        return A



# 路径规划器


class PathPlanner:
    """路径规划器"""

    # def __init__(self, topo: TopologyManager):
    #     self.topo = topo
    #     self.ksp_finder = YenKShortestPaths(topo.adjacency)
    #     self.path_usage_count = defaultdict(int)
    def __init__(self, topo: TopologyManager):
        self.topo = topo
        self.ksp_finder = YenKShortestPaths(topo.adjacency, topo.nodes)  # 添加nodes参数

        # 路径使用次数统计（键：路径节点元组，值：使用次数）
        self.path_usage_count = defaultdict(int)

        # 端口使用次数统计（键：端口ID，值：使用次数）
        self.port_usage_count = defaultdict(int)

        # 路径-端口映射表（键：路径节点元组，值：端口ID列表）
        self.path_port_mapping = {}


    def plan_routes(self, flows: List[Flow]):
        """规划所有流的路径"""
        logger.info(f"开始路径规划: {len(flows)}条流")

        for i, flow in enumerate(flows):
            if (i + 1) % 100 == 0:
                logger.info(f"  路径规划进度: {i + 1}/{len(flows)}")

            self._plan_single_flow(flow)

        logger.info("路径规划完成")

    def _plan_single_flow(self, flow: Flow):
        """为单个流规划路径"""
        paths = self.ksp_finder.find_k_shortest_paths(
            flow.source,
            flow.destination,
            K_PATHS
        )

        for path_nodes in paths:
            detailed_path = self._create_detailed_path(path_nodes, flow)
            if detailed_path:
                flow.path_candidates.append(detailed_path)

                # 记录路径-端口映射
                path_key = tuple(path_nodes)
                port_list = []
                for seg in detailed_path.segments:
                    port_list.append(seg.send_port_id)
                    port_list.append(seg.recv_port_id)
                self.path_port_mapping[path_key] = port_list

        if flow.path_candidates:
            # 动态评分（传入流信息以获取优先级）
            for path in flow.path_candidates:
                path.score = self._calculate_score_dynamic(path, flow)

            flow.path_candidates.sort(key=lambda p: p.score, reverse=True)

    # def _create_detailed_path(self, path_nodes: List[str], flow: Flow) -> Optional[DetailedPath]:
    #     """创建详细路径（包含端口号信息）"""
    #     if len(path_nodes) < 2:
    #         return None
    #
    #     segments = []
    #     total_latency = 0.0
    def _create_detailed_path(self, path_nodes: List[str], flow: Flow) -> Optional[DetailedPath]:
        """创建详细路径（包含端口号信息）"""
        if len(path_nodes) < 2:
            return None

         # 新增：验证中间节点必须是交换机
        for i in range(1, len(path_nodes) - 1):
            node_info = self.topo.nodes.get(path_nodes[i], {})
            if node_info.get('type', 'switch') != 'switch':
                logger.warning(f"路径{path_nodes}包含非交换机中间节点{path_nodes[i]}，拒绝")
                return None

        segments = []
        total_latency = 0.0

        for i in range(len(path_nodes) - 1):
            src, dst = path_nodes[i], path_nodes[i + 1]

            port_pair = self.topo.get_port_pair(src, dst)
            if not port_pair:
                return None

            send_port_id, recv_port_id = port_pair
            send_port = self.topo.ports[send_port_id]
            recv_port = self.topo.ports[recv_port_id]

            # 创建包含端口号的PathSegment
            segment = PathSegment(
                src_node=src,
                dst_node=dst,
                send_port_id=send_port_id,
                recv_port_id=recv_port_id,
                send_port_num=send_port.port_number,  # 端口号
                recv_port_num=recv_port.port_number,  # 端口号
                bandwidth_bps=send_port.bandwidth_bps,
                segment_index=i
            )

            segments.append(segment)

            tx_time = calculate_transmission_time(flow.frame_size_bytes, send_port.bandwidth_bps)
            switching_delay = lookup_switching_delay(flow.frame_size_bytes)
            total_latency += tx_time + PROPAGATION_DELAY_US + switching_delay + FRAME_GAP_US

        detailed_path = DetailedPath(
            path_id=f"{flow.source}->{flow.destination}-path{len(flow.path_candidates)}",
            path_nodes=path_nodes,
            segments=segments,
            total_hops=len(path_nodes) - 1,
            estimated_latency_us=total_latency
        )

        return detailed_path

    def _get_priority_weights(self, priority: int) -> Dict[str, float]:
        """根据优先级获取评分权重"""
        if priority <= HIGH_PRIORITY_MAX:
            return PRIORITY_WEIGHT_CONFIG['high']
        elif priority <= MEDIUM_PRIORITY_MAX:
            return PRIORITY_WEIGHT_CONFIG['medium']
        else:
            return PRIORITY_WEIGHT_CONFIG['low']

    def _calculate_score_dynamic(self, path: DetailedPath, flow: Flow) -> float:
        """
        动态计算路径评分（考虑优先级和使用次数）

        Args:
            path: 待评分路径
            flow: 所属流（用于获取优先级）

        Returns:
            路径评分
        """
        # 1. 获取权重配置
        weights = self._get_priority_weights(flow.priority)

        # 2. 计算跳数评分
        hop_score = 1.0 / (1 + path.total_hops)

        # 3. 计算路径使用评分
        path_key = tuple(path.path_nodes)
        path_usage = self.path_usage_count.get(path_key, 0)
        path_score = 1.0 / (1 + path_usage)

        # 4. 计算端口使用评分
        port_usage_list = []
        for seg in path.segments:
            # 统计发送端口和接收端口的使用次数
            send_usage = self.port_usage_count.get(seg.send_port_id, 0)
            recv_usage = self.port_usage_count.get(seg.recv_port_id, 0)
            port_usage_list.append(send_usage)
            port_usage_list.append(recv_usage)

        # 计算平均端口使用次数
        avg_port_usage = sum(port_usage_list) / len(port_usage_list) if port_usage_list else 0
        port_score = 1.0 / (1 + avg_port_usage)

        # 5. 综合评分
        total_score = (weights['hop'] * hop_score +
                       weights['path'] * path_score +
                       weights['port'] * port_score)

        # 归一化到0-100
        return total_score * 100

    def update_usage_counts(self, flow: Flow):
        """
        更新路径和端口使用次数
        仅在流的所有帧都成功调度后调用

        Args:
            flow: 成功调度的流
        """
        if not flow.selected_path:
            return

        # 1. 更新路径使用次数
        path_key = tuple(flow.selected_path.path_nodes)
        self.path_usage_count[path_key] += 1

        logger.debug(f"路径 {flow.selected_path.path_nodes} 使用次数更新为 {self.path_usage_count[path_key]}")

        # 2. 更新端口使用次数
        if path_key in self.path_port_mapping:
            port_list = self.path_port_mapping[path_key]
            for port_id in port_list:
                self.port_usage_count[port_id] += 1
                logger.debug(f"端口 {port_id} 使用次数更新为 {self.port_usage_count[port_id]}")
        else:
            # 如果映射表中没有，直接从path的segments获取
            for seg in flow.selected_path.segments:
                self.port_usage_count[seg.send_port_id] += 1
                self.port_usage_count[seg.recv_port_id] += 1
                logger.debug(f"端口 {seg.send_port_id} 使用次数更新为 {self.port_usage_count[seg.send_port_id]}")
                logger.debug(f"端口 {seg.recv_port_id} 使用次数更新为 {self.port_usage_count[seg.recv_port_id]}")

    # def _calculate_score(self, path: DetailedPath) -> float:
    #     """计算路径评分"""
    #     hop_score = 1.0 / (1 + path.total_hops)
    #
    #     avg_load = sum(self.topo.ports[seg.send_port_id].utilization
    #                    for seg in path.segments) / len(path.segments) if path.segments else 0
    #     load_score = 1.0 - avg_load
    #
    #     path_key = tuple(seg.src_node for seg in path.segments)
    #     usage_count = self.path_usage_count.get(path_key, 0)
    #     usage_score = 1.0 / (1 + usage_count)
    #
    #     min_bandwidth = min(self.topo.ports[seg.send_port_id].bandwidth_bps
    #                         for seg in path.segments) if path.segments else 0
    #     bandwidth_score = min_bandwidth / 1e9
    #
    #     score = (0.4 * hop_score +
    #              0.3 * load_score +
    #              0.2 * usage_score +
    #              0.1 * bandwidth_score)
    #
    #     return score * 100



# 时隙分配器


class TimeSlotAllocator:
    """时隙分配器"""

    def __init__(self, topo: TopologyManager):
        self.topo = topo
        self.scheduled_flows = []
        self.failed_flows = []

    #def allocate_flows(self, flows: List[Flow]):
        # """分配所有流的时隙"""
        # logger.info(f"开始时隙分配: {len(flows)}条流")
        #
        # for i, flow in enumerate(flows):
        #     if (i + 1) % 100 == 0:
        #         progress = (i + 1) / len(flows) * 100
        #         success_rate = len(self.scheduled_flows) / (i + 1) * 100
        #         logger.info(f"  分配进度: {i + 1}/{len(flows)} ({progress:.1f}%) | "
        #                     f"成功率: {success_rate:.1f}%")
        #
        #     success = self._allocate_single_flow(flow)
        #
        #     if success:
        #         self.scheduled_flows.append(flow)
        #     else:
        #         self.failed_flows.append(flow)
        #
        # logger.info(f"时隙分配完成: {len(self.scheduled_flows)}/{len(flows)} 成功")
    def allocate_flows(self, flows: List[Flow], path_planner=None):
        """
        分配所有流的时隙
            flows: 待调度的流列表
            path_planner: PathPlanner对象，用于更新使用次数
        """
        logger.info(f"开始时隙分配: {len(flows)}条流")

        for i, flow in enumerate(flows):
            if (i + 1) % 100 == 0:
                progress = (i + 1) / len(flows) * 100
                success_rate = len(self.scheduled_flows) / (i + 1) * 100
                logger.info(f"  分配进度: {i + 1}/{len(flows)} ({progress:.1f}%) | "
                            f"成功率: {success_rate:.1f}%")

            success = self._allocate_single_flow(flow)

            if success:
                self.scheduled_flows.append(flow)

                # 立即更新使用次数（关键修改）
                if path_planner:
                    path_planner.update_usage_counts(flow)
                    logger.info(f"流 {flow.flow_id} 调度成功，已更新路径和端口使用次数")
            else:
                self.failed_flows.append(flow)

        logger.info(f"时隙分配完成: {len(self.scheduled_flows)}/{len(flows)} 成功")



    # def _allocate_single_flow(self, flow: Flow) -> bool:
    #     """分配单个流"""
    #     if not flow.path_candidates:
    #         return False
    #
    #     for path in flow.path_candidates:
    #         if self._try_allocate_on_path(flow, path):
    #             flow.selected_path = path
    #             flow.scheduled = True
    #             return True
    #
    #     return False

    def _allocate_single_flow(self, flow: Flow) -> bool:
        """
        分配单个流
        只有所有帧都成功调度才返回True
        """
        if not flow.path_candidates:
            logger.debug(f"流 {flow.flow_id} 没有候选路径")
            return False

        for path_idx, path in enumerate(flow.path_candidates):
            logger.debug(f"流 {flow.flow_id} 尝试路径 {path_idx + 1}/{len(flow.path_candidates)}")

            if self._try_allocate_on_path(flow, path):
                # 验证所有帧都成功调度
                all_frames_scheduled = all(frame.status == "SCHEDULED" for frame in flow.frames)

                if all_frames_scheduled:
                    flow.selected_path = path
                    flow.scheduled = True

                    # 记录实际完成时间（新增）

                    if flow.frames:
                        last_frame = flow.frames[-1]
                        if last_frame.status == "SCHEDULED" and last_frame.schedule:
                            # 从最后一跳获取接收结束时间
                            last_hop = last_frame.schedule[-1]
                            actual_finish_time = last_hop['recv_end']
                            flow.actual_finish_time = actual_finish_time

                            if flow.is_temporal:
                                logger.info(f"时序流 {flow.flow_id} 实际完成时间: {actual_finish_time:.2f} us")

                    logger.info(f"流 {flow.flow_id} 在路径 {path.path_nodes} 上调度成功，"
                                f"所有 {len(flow.frames)} 帧已分配")
                    return True
                else:
                    logger.warning(f"流 {flow.flow_id} 部分帧未调度，这不应该发生")
                    return False

                #     logger.info(
                #         f"流 {flow.flow_id} 在路径 {path.path_nodes} 上调度成功，所有 {len(flow.frames)} 帧已分配")
                #     return True
                # else:
                #     # 如果有帧未成功，这不应该发生（因为_try_allocate_on_path会回滚）
                #     logger.warning(f"流 {flow.flow_id} 部分帧未调度，这不应该发生")
                #     return False

        logger.debug(f"流 {flow.flow_id} 所有候选路径都失败")
        return False



    def _try_allocate_on_path(self, flow: Flow, path: DetailedPath) -> bool:
        """尝试在路径上分配"""
        allocated_slots = []

        for frame in flow.frames:
            frame.status = "SCHEDULING"

            success = self._allocate_single_frame(frame, flow, path, allocated_slots)

            if not success:
                self._rollback_slots(allocated_slots)
                return False

            frame.status = "SCHEDULED"

        return True

    def _allocate_single_frame(self, frame: Frame, flow: Flow,
                               path: DetailedPath, allocated_slots: List) -> bool:
        """分配单个帧的时隙（增强端口信息记录）"""
        current_time = frame.period_start
        hop_schedule = []

        for seg in path.segments:
            send_port = self.topo.ports[seg.send_port_id]
            recv_port = self.topo.ports[seg.recv_port_id]

            tx_time = calculate_transmission_time(frame.size_bytes, send_port.bandwidth_bps)
            rx_time = tx_time

            send_slot_start = send_port.find_slot(current_time, tx_time + FRAME_GAP_US)
            if send_slot_start is None:
                return False

            if not self._check_constraints(send_port, send_slot_start, tx_time, flow, frame):
                return False

            send_slot_end = send_slot_start + tx_time + FRAME_GAP_US
            if not send_port.add_slot(send_slot_start, send_slot_end - FRAME_GAP_US, flow.flow_id):
                return False
            allocated_slots.append(('send', seg.send_port_id, send_slot_start, send_slot_end - FRAME_GAP_US))

            recv_arrival = send_slot_start + tx_time + PROPAGATION_DELAY_US

            recv_slot_end = recv_arrival + rx_time + FRAME_GAP_US
            if recv_port.check_conflict(recv_arrival, recv_slot_end - FRAME_GAP_US):
                return False

            if not recv_port.add_slot(recv_arrival, recv_slot_end - FRAME_GAP_US, flow.flow_id):
                return False
            allocated_slots.append(('recv', seg.recv_port_id, recv_arrival, recv_slot_end - FRAME_GAP_US))

            switching_delay = lookup_switching_delay(frame.size_bytes)
            forwarding_complete = recv_arrival + rx_time + switching_delay

            # 提取端口详细信息
            send_port_info = extract_port_info(seg.send_port_id)
            recv_port_info = extract_port_info(seg.recv_port_id)

            # 记录跳信息（包含端口号）
            hop_schedule.append({
                'hop_index': seg.segment_index,
                'src_node': seg.src_node,
                'dst_node': seg.dst_node,
                'send_port': {
                    'port_id': seg.send_port_id,
                    'port_number': seg.send_port_num,
                    'to_node': seg.dst_node,
                    'to_port_number': seg.recv_port_num
                },
                'send_start': send_slot_start,
                'send_end': send_slot_end - FRAME_GAP_US,
                'recv_port': {
                    'port_id': seg.recv_port_id,
                    'port_number': seg.recv_port_num,
                    'from_node': seg.src_node,
                    'from_port_number': seg.send_port_num
                },
                'recv_start': recv_arrival,
                'recv_end': recv_slot_end - FRAME_GAP_US,
                'transmission_time': tx_time,
                'propagation_delay': PROPAGATION_DELAY_US,
                'switching_delay': switching_delay
            })

            current_time = forwarding_complete

        e2e_latency = current_time - frame.period_start

        if current_time >= frame.deadline:
            return False

        if e2e_latency >= flow.period_us:
            return False

        frame.schedule = hop_schedule
        frame.e2e_latency = e2e_latency
        frame.final_arrival = current_time

        return True

    def _check_constraints(self, port: OptimizedPort, slot_start: float,
                           duration: float, flow: Flow, frame: Frame) -> bool:
        """检查约束"""
        future_load_bps = port.allocated_bps + int((frame.size_bytes * 8) / flow.period_us * 1e6)
        if future_load_bps > port.bandwidth_bps * MAX_PORT_UTILIZATION:
            return False

        if slot_start + duration > frame.deadline:
            return False

        return True

    def _rollback_slots(self, allocated_slots: List):
        """回滚已分配的时隙"""
        for slot_type, port_id, start, end in reversed(allocated_slots):
            try:
                port = self.topo.ports.get(port_id)
                if port:
                    port.remove_slot(start, end)
            except Exception as e:
                logger.error(f"回滚失败: {e}")
        allocated_slots.clear()



# 约束验证器


class ConstraintValidator:
    """约束验证器"""

    def __init__(self, topo: TopologyManager):
        self.topo = topo
        self.violations = []

    def validate_all(self, flows: List[Flow]) -> bool:
        """验证所有约束"""
        logger.info("开始约束验证...")
        self.violations = []

        for flow in flows:
            if not flow.scheduled:
                continue

            self._validate_flow(flow)

        self._validate_port_conflicts()
        self._validate_port_gaps()
        self._validate_port_utilization()

        if self.violations:
            logger.warning(f"发现{len(self.violations)}个约束违规")
            for v in self.violations[:10]:
                logger.warning(f"  - {v}")
        else:
            logger.info("约束验证通过")

        return len(self.violations) == 0

    def _validate_flow(self, flow: Flow):
        """验证单个流"""
        for frame in flow.frames:
            if frame.status != "SCHEDULED":
                continue

            if frame.e2e_latency >= flow.period_us:
                self.violations.append(
                    f"{frame.frame_id}: 延时{frame.e2e_latency:.2f}us >= 周期{flow.period_us}us"
                )

            if frame.final_arrival >= frame.deadline:
                self.violations.append(
                    f"{frame.frame_id}: 到达{frame.final_arrival:.2f}us >= deadline{frame.deadline}us"
                )

            for hop in frame.schedule:
                expected_delay = lookup_switching_delay(frame.size_bytes)
                actual_delay = hop['switching_delay']
                if abs(expected_delay - actual_delay) > 0.1:
                    self.violations.append(
                        f"{frame.frame_id} hop{hop['hop_index']}: "
                        f"交换延时不匹配 (预期{expected_delay}us, 实际{actual_delay}us)"
                    )

    def _validate_port_conflicts(self):
        """验证端口无冲突"""
        for port_id, port in self.topo.ports.items():
            intervals = sorted(port.interval_tree)

            for i in range(len(intervals) - 1):
                curr = intervals[i]
                next_iv = intervals[i + 1]

                if curr.end > next_iv.begin:
                    self.violations.append(
                        f"端口{port_id}: 时隙冲突 [{curr.begin}-{curr.end}] vs [{next_iv.begin}-{next_iv.end}]"
                    )

    def _validate_port_gaps(self):
        """验证帧间隔"""
        for port_id, port in self.topo.ports.items():
            intervals = sorted(port.interval_tree)

            for i in range(len(intervals) - 1):
                curr = intervals[i]
                next_iv = intervals[i + 1]
                gap = next_iv.begin - curr.end

                if gap < FRAME_GAP_US and gap > 0.01:
                    self.violations.append(
                        f"端口{port_id}: 帧间隔{gap:.2f}us < 50us"
                    )

    def _validate_port_utilization(self):
        """验证端口利用率"""
        for port_id, port in self.topo.ports.items():
            if port.utilization > 1.0:
                self.violations.append(
                    f"端口{port_id}: 利用率{port.utilization * 100:.1f}% > 100%"
                )



# 结果导出器


class ResultExporter:
    """结果导出器（增强版）"""

    def __init__(self, topo: TopologyManager):
        self.topo = topo

    # def export_results(self, scheduled_flows: List[Flow], failed_flows: List[Flow],
    #                    statistics: Dict, output_file: str):
    #     """导出结果（增强端口信息）"""

    def export_results(self, scheduled_flows: List[Flow], failed_flows: List[Flow],
                        statistics: Dict, output_file: str, path_planner=None,temporal_mgr=None):
        """导出结果"""
        logger.info(f"导出结果: {output_file}")

        output = {
            'metadata': {
                **statistics,
                'port_naming_format': 'NodeA_pX_to_NodeB_pY (NodeA的X号端口连接到NodeB的Y号端口)'
            },
            'flows': {},
            'failed_flows': [f.flow_id for f in failed_flows],
            'gate_control_lists': self._generate_gcl(scheduled_flows),
            'port_statistics': self._generate_port_stats(),
            'path_table': self._generate_path_table(scheduled_flows),
            'usage_statistics': self._generate_usage_stats(path_planner),
            'temporal_report': self._generate_temporal_report(temporal_mgr) if temporal_mgr else {}  # 新增

        }

        # 流调度详情
        for flow in scheduled_flows:
            output['flows'][flow.flow_id] = {
                'flow_info': {
                    'source': flow.source,
                    'destination': flow.destination,
                    'priority': flow.priority,
                    'period_us': flow.period_us,
                    'num_frames': flow.num_frames,
                    #  新增以下4行
                    'is_temporal': flow.is_temporal,
                    'temporal_order': flow.temporal_order if flow.is_temporal else None,
                    'delayed_start_time': flow.delayed_start_time if flow.is_temporal else None,
                    'actual_finish_time': flow.actual_finish_time if flow.scheduled else None

                },
                'routing': {
                    'primary_path': flow.selected_path.path_nodes if flow.selected_path else [],
                    'total_hops': flow.selected_path.total_hops if flow.selected_path else 0,
                    'path_segments': [
                        {
                            'src_node': seg.src_node,
                            'dst_node': seg.dst_node,
                            'send_port_id': seg.send_port_id,
                            'send_port_number': seg.send_port_num,
                            'recv_port_id': seg.recv_port_id,
                            'recv_port_number': seg.recv_port_num
                        }
                        for seg in flow.selected_path.segments
                    ] if flow.selected_path else []
                },
                'scheduling': {
                    'frames': [
                        {
                            'frame_id': frame.frame_id,
                            'period_start': frame.period_start,
                            'final_arrival': frame.final_arrival,
                            'e2e_latency': frame.e2e_latency,
                            'hops': frame.schedule
                        }
                        for frame in flow.frames if frame.status == "SCHEDULED"
                    ]
                }
            }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        logger.info("结果导出完成")

    def _generate_gcl(self, flows: List[Flow]) -> Dict:
        """生成GCL表"""
        gcl_by_port = defaultdict(list)

        for flow in flows:
            for frame in flow.frames:
                if frame.status != "SCHEDULED":
                    continue

                for hop in frame.schedule:
                    # 发送端口GCL
                    send_port_id = hop['send_port']['port_id']
                    send_entry = {
                        'time_offset_us': hop['send_start'],
                        'gate_state': 'OPEN',
                        'duration_us': hop['send_end'] - hop['send_start'],
                        'flow_id': flow.flow_id,
                        'frame_id': frame.frame_id,
                        'port_number': hop['send_port']['port_number'],
                        'to_node': hop['send_port']['to_node'],
                        'to_port_number': hop['send_port']['to_port_number']
                    }
                    gcl_by_port[send_port_id].append(send_entry)

                    # 接收端口GCL
                    recv_port_id = hop['recv_port']['port_id']
                    recv_entry = {
                        'time_offset_us': hop['recv_start'],
                        'gate_state': 'OPEN',
                        'duration_us': hop['recv_end'] - hop['recv_start'],
                        'flow_id': flow.flow_id,
                        'frame_id': frame.frame_id,
                        'port_number': hop['recv_port']['port_number'],
                        'from_node': hop['recv_port']['from_node'],
                        'from_port_number': hop['recv_port']['from_port_number']
                    }
                    gcl_by_port[recv_port_id].append(recv_entry)

        # 排序并格式化
        gcl_output = {}
        for port_id, entries in gcl_by_port.items():
            entries.sort(key=lambda e: e['time_offset_us'])

            port_info = extract_port_info(port_id)

            gcl_output[port_id] = {
                'port_id': port_id,
                'node': port_info.get('node', 'unknown'),
                'port_number': port_info.get('port_num', -1),
                'num_entries': len(entries),
                'entries': entries
            }

        return gcl_output

    def _generate_port_stats(self) -> Dict:
        """生成端口统计"""
        port_stats = {}

        for port_id, port in self.topo.ports.items():
            if port.slot_count > 0:
                port_stats[port_id] = {
                    'port_id': port_id,
                    'node': port.node_id,
                    'port_number': port.port_number,
                    'connected_node': port.connected_node,
                    'connected_port_number': port.connected_port_number,
                    'utilization': port.utilization,
                    'allocated_bps': port.allocated_bps,
                    'bandwidth_bps': port.bandwidth_bps,
                    'slot_count': port.slot_count
                }

        return port_stats

    def _generate_path_table(self, flows: List[Flow]) -> Dict:
        """生成路径表（包含端口号）"""
        path_table = {}

        for flow in flows:
            if not flow.path_candidates:
                continue

            path_table[flow.flow_id] = {
                'total_candidates': len(flow.path_candidates),
                'selected_index': None,
                'paths': []
            }

            for idx, path in enumerate(flow.path_candidates):
                is_selected = (flow.selected_path == path)

                if is_selected:
                    path_table[flow.flow_id]['selected_index'] = idx

                path_info = {
                    'index': idx,
                    'nodes': path.path_nodes,
                    'segments': [
                        {
                            'src_node': seg.src_node,
                            'dst_node': seg.dst_node,
                            'send_port': seg.send_port_id,
                            'send_port_num': seg.send_port_num,
                            'recv_port': seg.recv_port_id,
                            'recv_port_num': seg.recv_port_num
                        }
                        for seg in path.segments
                    ],
                    'hops': path.total_hops,
                    'estimated_latency_us': path.estimated_latency_us,
                    'score': path.score,
                    'selected': is_selected
                }

                path_table[flow.flow_id]['paths'].append(path_info)

        return path_table

    # 新增：生成时序关系报告
    def _generate_temporal_report(self, temporal_mgr) -> Dict:
        """生成时序关系报告"""
        if not temporal_mgr or not temporal_mgr.has_temporal_flows():
            return {}

        temporal_flows = temporal_mgr.temporal_flows

        report = {
            'temporal_chain': [f.flow_id for f in temporal_flows],
            'safety_margin_us': temporal_mgr.safety_margin_us,
            'flow_details': []
        }

        for i, flow in enumerate(temporal_flows):
            flow_info = {
                'order': i,
                'flow_id': flow.flow_id,
                'delayed_start_time': flow.delayed_start_time,
                'num_frames': flow.num_frames,
                'period_us': flow.period_us,
                'planned_duration': (flow.num_frames - 1) * flow.period_us,
                'actual_finish_time': flow.actual_finish_time if flow.scheduled else None,
                'scheduled': flow.scheduled
            }

            # 添加与前驱流的关系信息
            if i > 0:
                prev_flow = temporal_flows[i - 1]
                if flow.scheduled and prev_flow.scheduled:
                    flow_info['temporal_margin'] = (flow.actual_finish_time -
                                                    prev_flow.actual_finish_time)
                    flow_info['constraint_satisfied'] = (flow.actual_finish_time >
                                                         prev_flow.actual_finish_time)
                    flow_info['predecessor'] = prev_flow.flow_id

            report['flow_details'].append(flow_info)

        return report

    def _generate_usage_stats(self, path_planner) -> Dict:
        """生成使用次数统计（新增方法）"""
        if not path_planner:
            return {}

        usage_stats = {
            'path_usage': {},
            'port_usage': {},
            'summary': {
                'total_paths_used': sum(1 for c in path_planner.path_usage_count.values() if c > 0),
                'total_ports_used': sum(1 for c in path_planner.port_usage_count.values() if c > 0),
                'max_path_usage': max(path_planner.path_usage_count.values()) if path_planner.path_usage_count else 0,
                'max_port_usage': max(path_planner.port_usage_count.values()) if path_planner.port_usage_count else 0
            }
        }

        # 路径使用次数（只导出被使用的）
        for path_key, count in path_planner.path_usage_count.items():
            if count > 0:
                usage_stats['path_usage'][str(path_key)] = count

        # 端口使用次数（只导出被使用的，按使用次数排序）
        port_usage_sorted = sorted(
            [(port_id, count) for port_id, count in path_planner.port_usage_count.items() if count > 0],
            key=lambda x: x[1],
            reverse=True
        )

        for port_id, count in port_usage_sorted:
            port_info = extract_port_info(port_id)
            usage_stats['port_usage'][port_id] = {
                'usage_count': count,
                'node': port_info.get('node', 'unknown'),
                'port_number': port_info.get('port_num', -1),
                'connected_node': port_info.get('connected_node', 'unknown'),
                'connected_port_number': port_info.get('connected_port_num', -1)
            }

        return usage_stats



# 主调度器


class TSNScheduler:
    """TSN调度器主类"""

    def __init__(self, topo_file: str, flows_file: str):

        start = time.time()

        self.topo = TopologyManager(topo_file)
        self.flow_mgr = FlowManager(flows_file)

        self.path_planner = PathPlanner(self.topo)
        self.allocator = TimeSlotAllocator(self.topo)
        self.validator = ConstraintValidator(self.topo)
        self.exporter = ResultExporter(self.topo)

        # 创建时序流管理器（新增）
        self.temporal_mgr = TemporalFlowManager()

        logger.info(f"初始化完成 ({time.time() - start:.2f}秒)\n")



    def schedule(self) -> Dict:
        """执行调度"""
        start_time = time.time()

        # 修改：根据是否有时序流选择排序方式
        if self.temporal_mgr.has_temporal_flows():
            sorted_flows = self.temporal_mgr.get_reordered_flows()
            logger.info(f"使用时序关系调度顺序，共{len(sorted_flows)}条流\n")
        else:
            sorted_flows = self.flow_mgr.sort_by_priority()
            logger.info(f"使用标准优先级调度顺序，共{len(sorted_flows)}条流\n")

        #sorted_flows = self.flow_mgr.sort_by_priority()
        logger.info(f"共{len(sorted_flows)}条流待调度\n")

        # 阶段1：路径规划
        stage1_start = time.time()
        self.path_planner.plan_routes(sorted_flows)
        stage1_time = time.time() - stage1_start
        logger.info(f"阶段1完成: 路径规划 ({stage1_time:.2f}秒)\n")

        # # 阶段2：时隙分配
        # stage2_start = time.time()
        # self.allocator.allocate_flows(sorted_flows)
        # stage2_time = time.time() - stage2_start
        # logger.info(f"阶段2完成: 时隙分配 ({stage2_time:.2f}秒)\n")

        # 阶段2：时隙分配
        stage2_start = time.time()
        self.allocator.allocate_flows(sorted_flows, self.path_planner)  # 传递path_planner
        stage2_time = time.time() - stage2_start
        logger.info(f"阶段2完成: 时隙分配 ({stage2_time:.2f}秒)\n")

        # 阶段3：约束验证
        stage3_start = time.time()
        constraints_valid = self.validator.validate_all(self.allocator.scheduled_flows)
        stage3_time = time.time() - stage3_start
        logger.info(f"阶段3完成: 约束验证 ({stage3_time:.2f}秒)\n")

        # 新增：时序约束验证
        temporal_valid = True
        temporal_violations = []
        if self.temporal_mgr.has_temporal_flows():
            temporal_valid, temporal_violations = self.temporal_mgr.validate_temporal_constraints()

        total_time = time.time() - start_time
        statistics = self._generate_statistics(
            len(sorted_flows),
            len(self.allocator.scheduled_flows),
            total_time,
            constraints_valid,
            stage1_time,
            stage2_time,
            stage3_time,
            temporal_valid,  # 新增参数
            temporal_violations  # 新增参数
        )

        return statistics

    def export_results(self, output_file: str = "tsn_schedule_result_enhanced.json"):
        """导出结果"""
        statistics = self.schedule()

        self.exporter.export_results(
            self.allocator.scheduled_flows,
            self.allocator.failed_flows,
            statistics,
            output_file,
            self.path_planner,  # 传递path_planner
            self.temporal_mgr  # 新增：传递时序管理器

        )


        # 导出端口映射表
        port_mapping_file = output_file.replace('.json', '_port_mapping.json')
        self.topo.export_port_mapping_table(port_mapping_file)

        return statistics

    def _generate_statistics(self, total: int, success: int, exec_time: float,
                             constraints_valid: bool, stage1: float,
                             stage2: float, stage3: float,
                             temporal_valid: bool = True,  # 新增参数
                             temporal_violations: List = None) -> Dict:  # 新增参数

        """生成统计信息"""

        logger.info("调度结果统计")

        logger.info(f"总流数:           {total}")
        logger.info(f"成功调度:         {success}")
        logger.info(f"失败:             {total - success}")
        logger.info(f"成功率:           {success / total * 100:.2f}%")
        logger.info(f"执行时间:         {exec_time:.2f}秒")
        logger.info(f"  - 路径规划:     {stage1:.2f}秒")
        logger.info(f"  - 时隙分配:     {stage2:.2f}秒")
        logger.info(f"  - 约束验证:     {stage3:.2f}秒")
        logger.info(f"约束验证:         {'通过' if constraints_valid else '失败'}")

        port_utils = [p.utilization for p in self.topo.ports.values() if p.slot_count > 0]
        if port_utils:
            avg_util = sum(port_utils) / len(port_utils)
            max_util = max(port_utils)
            logger.info(f"平均端口利用率:   {avg_util * 100:.2f}%")
            logger.info(f"最大端口利用率:   {max_util * 100:.2f}%")

            # 新增：时序关系流统计
            if self.temporal_mgr.has_temporal_flows():
                temporal_flows = self.temporal_mgr.temporal_flows

                logger.info("\n时序关系流统计:")
                logger.info(f"  时序流总数: {len(temporal_flows)}")

                scheduled_temporal = [f for f in temporal_flows if f.scheduled]
                logger.info(f"  成功调度: {len(scheduled_temporal)}/{len(temporal_flows)}")
                logger.info(f"  时序约束验证: {'通过' if temporal_valid else '失败'}")

                if len(scheduled_temporal) >= 2:
                    # 计算时序余量统计
                    margins = []
                    for i in range(len(scheduled_temporal) - 1):
                        if scheduled_temporal[i + 1].actual_finish_time > scheduled_temporal[i].actual_finish_time:
                            margin = (scheduled_temporal[i + 1].actual_finish_time -
                                      scheduled_temporal[i].actual_finish_time)
                            margins.append(margin)

                    if margins:
                        avg_margin = sum(margins) / len(margins)
                        min_margin = min(margins)
                        max_margin = max(margins)

                        logger.info(f"  平均时序余量: {avg_margin:.2f} us")
                        logger.info(f"  最小时序余量: {min_margin:.2f} us")
                        logger.info(f"  最大时序余量: {max_margin:.2f} us")

            # 输出使用次数统计
            logger.info("\n使用次数统计:")
            logger.info(f"  路径总数: {len(self.path_planner.path_usage_count)}")
            logger.info(f"  被使用的路径数: {sum(1 for c in self.path_planner.path_usage_count.values() if c > 0)}")

            if self.path_planner.path_usage_count:
                max_path_usage = max(self.path_planner.path_usage_count.values())
                avg_path_usage = sum(self.path_planner.path_usage_count.values()) / len(
                    self.path_planner.path_usage_count)
                logger.info(f"  路径最大使用次数: {max_path_usage}")
                logger.info(f"  路径平均使用次数: {avg_path_usage:.2f}")

            used_ports = [p for p, c in self.path_planner.port_usage_count.items() if c > 0]
            logger.info(f"  被使用的端口数: {len(used_ports)}/{len(self.topo.ports)}")

            if self.path_planner.port_usage_count:
                max_port_usage = max(
                    self.path_planner.port_usage_count.values()) if self.path_planner.port_usage_count else 0
                used_port_values = [c for c in self.path_planner.port_usage_count.values() if c > 0]
                avg_port_usage = sum(used_port_values) / len(used_port_values) if used_port_values else 0
                logger.info(f"  端口最大使用次数: {max_port_usage}")
                logger.info(f"  端口平均使用次数: {avg_port_usage:.2f}")


        return {
            'total_flows': total,
            'scheduled_flows': success,
            'failed_flows': total - success,
            'success_rate': success / total if total > 0 else 0,
            'execution_time_seconds': exec_time,
            'stage1_routing_time_seconds': stage1,
            'stage2_scheduling_time_seconds': stage2,
            'stage3_validation_time_seconds': stage3,
            'average_port_utilization': sum(port_utils) / len(port_utils) if port_utils else 0,
            'max_port_utilization': max(port_utils) if port_utils else 0,
            'constraints_validated': constraints_valid,
            'algorithm_version': 'TSN Scheduler v2.0 (Enhanced Port Info)',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }



# 主程序入口


def main():
    """主函数"""
    # 设置需要时序关系的数据流ID列表（按时序先后顺序）
    # 例如：["flow_85", "flow_80", "flow_90", "flow_70"] 表示 flow_85 → flow_80 → flow_90 → flow_70
    # 如果不需要时序关系，设置为空列表 []

    TEMPORAL_FLOW_IDS = ["ST_0081", "ST_0080", "ST_0090", "ST_0070"]  # 修改此处配置时序流
    #TEMPORAL_FLOW_IDS = []  # 修改此处配置时序流
    SAFETY_MARGIN_US = 2000.0  # 安全余量（微秒）

    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_topo = os.path.join(script_dir, "tsn_mesh_550_dual.json")
    default_flows = os.path.join(script_dir, "tsn_st_flows_id_priority.json")
    default_output = os.path.join(script_dir, "tsn_schedule_result_enhanced.json")

    if not os.path.exists(default_topo):
        logger.error(f"找不到拓扑文件: {default_topo}")
        sys.exit(1)

    if not os.path.exists(default_flows):
        logger.error(f"找不到流文件: {default_flows}")
        sys.exit(1)


    logger.info(f"拓扑文件:   {os.path.basename(default_topo)}")
    logger.info(f"流文件:     {os.path.basename(default_flows)}")
    logger.info(f"输出文件:   {os.path.basename(default_output)}")

    try:
        scheduler = TSNScheduler(default_topo, default_flows)
        # statistics = scheduler.export_results(default_output)
        #
        # success_rate = statistics['success_rate']
        #
        # logger.info(f"\n 调度成功: 成功率 {success_rate * 100:.2f}%")
        # sys.exit(0)

        # 配置时序关系流
        if TEMPORAL_FLOW_IDS and len(TEMPORAL_FLOW_IDS) > 0:
            logger.info(f"\n配置时序关系流: {TEMPORAL_FLOW_IDS}")
            logger.info(f"安全余量: {SAFETY_MARGIN_US}us\n")

            try:
                scheduler.temporal_mgr.safety_margin_us = SAFETY_MARGIN_US
                scheduler.temporal_mgr.set_temporal_flows(
                    TEMPORAL_FLOW_IDS,
                    scheduler.flow_mgr.flows
                )
            except ValueError as e:
                logger.error(f"时序流配置错误: {e}")
                logger.error("可用的流ID列表（前20个）:")
                for flow in scheduler.flow_mgr.flows[:20]:
                    logger.error(f"  - {flow.flow_id}")
                if len(scheduler.flow_mgr.flows) > 20:
                    logger.error(f"  ... 还有 {len(scheduler.flow_mgr.flows) - 20} 个流")
                sys.exit(1)
        else:
            logger.info("\n未配置时序关系流，使用标准调度模式\n")

        # 执行调度并导出结果
        statistics = scheduler.export_results(default_output)

        success_rate = statistics['success_rate']
        logger.info(f"\n调度完成: 成功率 {success_rate * 100:.2f}%")

        # 显示时序验证结果
        if 'temporal_constraints_validated' in statistics:
            if statistics['temporal_constraints_validated']:
                logger.info("时序约束验证:通过")
            else:
                logger.warning("时序约束验证:失败")
                if statistics.get('temporal_violations'):
                    logger.warning(f"违规数量: {len(statistics['temporal_violations'])}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"调度错误: {e}", exc_info=True)
        sys.exit(1)





if __name__ == '__main__':
    main()