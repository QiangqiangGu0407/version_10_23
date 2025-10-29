#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速路径生成器
使用多种策略快速生成大量端到端路径
不需要路径调度，只专注于路径生成性能
"""

import json
import time
import random
import heapq
from typing import List, Set, Tuple, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict, deque

@dataclass
class SimplePath:
    """简化的路径表示"""
    nodes: List[str]
    delay: int = 0
    
    def __hash__(self):
        return hash("->".join(self.nodes))
    
    def __eq__(self, other):
        return self.nodes == other.nodes

class FastPathGenerator:
    """快速路径生成器"""
    
    def __init__(self, topology_file: str):
        """加载拓扑数据"""
        self.nodes = {}
        self.adjacency = defaultdict(list)
        self.links = {}
        self._load_topology(topology_file)
    
    def _load_topology(self, topology_file: str):
        """加载拓扑结构"""
        with open(topology_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 加载节点
        self.endpoint_nodes = set()  # 端节点集合
        self.switch_nodes = set()    # 交换机节点集合
        
        for node_data in data['nodes']:
            node_id = node_data['id']
            self.nodes[node_id] = node_data
            
            # 区分端节点和交换机
            if node_id.startswith('EP'):
                self.endpoint_nodes.add(node_id)
            else:
                self.switch_nodes.add(node_id)
        
        # 加载链路和邻接关系
        for link_data in data['links']:
            src = link_data['source']
            dst = link_data['target']
            delay = link_data.get('propagation_delay_ns', 1000)
            
            self.adjacency[src].append(dst)
            self.links[(src, dst)] = delay
            
            # 反向链路
            self.adjacency[dst].append(src)
            self.links[(dst, src)] = delay
        
        print(f"加载拓扑: {len(self.nodes)}个节点 (交换机={len(self.switch_nodes)}, 端节点={len(self.endpoint_nodes)}), {len(self.links)}条链路")
    
    def _can_pass_through(self, node: str, src: str, dst: str) -> bool:
        """
        检查节点是否可以作为路径中间节点
        规则：只有交换机可以作为中间节点，端节点不能作为中间节点（除非它是源或目标）
        """
        if node == src or node == dst:
            return True  # 源节点和目标节点总是可以的
        return node in self.switch_nodes  # 中间节点必须是交换机
    
    def find_shortest_path(self, src: str, dst: str) -> Optional[SimplePath]:
        """Dijkstra最短路径 - 不经过其他端节点"""
        if src == dst:
            return SimplePath(nodes=[src], delay=0)
        
        distances = {src: 0}
        predecessors = {}
        unvisited = [(0, src)]
        visited = set()
        
        while unvisited:
            current_delay, current = heapq.heappop(unvisited)
            
            if current in visited:
                continue
            visited.add(current)
            
            if current == dst:
                # 重建路径
                path_nodes = []
                node = current
                total_delay = distances[dst]
                
                while node is not None:
                    path_nodes.append(node)
                    node = predecessors.get(node)
                
                path_nodes.reverse()
                return SimplePath(nodes=path_nodes, delay=total_delay)
            
            for neighbor in self.adjacency[current]:
                if neighbor in visited:
                    continue
                
                # 检查邻居节点是否可以通过（不能经过其他端节点）
                if not self._can_pass_through(neighbor, src, dst):
                    continue
                
                link_delay = self.links.get((current, neighbor), 1000)
                new_delay = current_delay + link_delay
                
                if neighbor not in distances or new_delay < distances[neighbor]:
                    distances[neighbor] = new_delay
                    predecessors[neighbor] = current
                    heapq.heappush(unvisited, (new_delay, neighbor))
        
        return None
    
    def find_random_walk_paths(self, src: str, dst: str, count: int, max_hops: int = 15) -> List[SimplePath]:
        """随机游走生成路径 - 快速但可能较长，不经过其他端节点"""
        paths = []
        paths_set = set()
        attempts = 0
        max_attempts = count * 20  # 增加尝试次数
        
        while len(paths) < count and attempts < max_attempts:
            attempts += 1
            current = src
            path_nodes = [src]
            visited = {src}
            
            # 随机游走
            for hop in range(max_hops):
                if current == dst:
                    break
                
                # 获取邻居，过滤掉端节点（除非是目标）
                neighbors = [n for n in self.adjacency[current] 
                           if n not in visited and self._can_pass_through(n, src, dst)]
                
                # 如果没有未访问的邻居，允许访问已访问的（但限制次数）
                if not neighbors and hop < max_hops - 3:
                    neighbors = [n for n in self.adjacency[current]
                               if self._can_pass_through(n, src, dst)]
                
                if not neighbors:
                    break
                
                # 随机选择，但偏向目标方向
                if dst in neighbors and random.random() < 0.7:  # 70%概率选择目标
                    next_node = dst
                else:
                    next_node = random.choice(neighbors)
                
                path_nodes.append(next_node)
                visited.add(next_node)
                current = next_node
            
            # 如果到达目标，创建路径
            if current == dst and len(path_nodes) > 1:
                path = SimplePath(nodes=path_nodes)
                path_hash = hash(path)
                
                if path_hash not in paths_set:
                    paths.append(path)
                    paths_set.add(path_hash)
        
        return paths
    
    def find_bfs_paths(self, src: str, dst: str, count: int, max_depth: int = 10) -> List[SimplePath]:
        """分层BFS生成路径 - 不经过其他端节点"""
        paths = []
        paths_set = set()
        
        # BFS队列
        queue = deque([(src, [src], 0)])
        
        while queue and len(paths) < count:
            current, path, depth = queue.popleft()
            
            if current == dst and len(path) > 1:
                simple_path = SimplePath(nodes=path)
                path_hash = hash(simple_path)
                
                if path_hash not in paths_set:
                    paths.append(simple_path)
                    paths_set.add(path_hash)
                continue
            
            if depth >= max_depth:
                continue
            
            # 添加邻居到队列（过滤掉端节点）
            for neighbor in self.adjacency[current]:
                if neighbor not in path and self._can_pass_through(neighbor, src, dst):  # 避免环路并检查端节点
                    new_path = path + [neighbor]
                    queue.append((neighbor, new_path, depth + 1))
        
        return paths
    
    def find_weight_perturbed_paths(self, src: str, dst: str, count: int) -> List[SimplePath]:
        """权重扰动算法"""
        paths = []
        paths_set = set()
        
        # 不同的扰动因子
        perturbation_factors = [0.5, 0.7, 0.9, 1.1, 1.3, 1.5, 2.0, 3.0]
        
        for factor in perturbation_factors:
            if len(paths) >= count:
                break
            
            # 临时修改权重
            original_links = self.links.copy()
            
            for link_key in self.links:
                self.links[link_key] = int(self.links[link_key] * factor)
            
            # 寻找路径
            path = self.find_shortest_path(src, dst)
            
            # 恢复权重
            self.links = original_links
            
            if path:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    paths.append(path)
                    paths_set.add(path_hash)
        
        return paths
    
    def find_dfs_paths(self, src: str, dst: str, count: int, max_depth: int = 12) -> List[SimplePath]:
        """深度优先搜索生成路径 - 不经过其他端节点"""
        paths = []
        paths_set = set()
        
        def dfs(current: str, path: List[str], depth: int):
            if len(paths) >= count:
                return
            
            if current == dst and len(path) > 1:
                simple_path = SimplePath(nodes=path.copy())
                path_hash = hash(simple_path)
                
                if path_hash not in paths_set:
                    paths.append(simple_path)
                    paths_set.add(path_hash)
                return
            
            if depth >= max_depth:
                return
            
            # 随机打乱邻居顺序增加多样性（过滤掉端节点）
            neighbors = [n for n in self.adjacency[current] 
                        if self._can_pass_through(n, src, dst)]
            random.shuffle(neighbors)
            
            for neighbor in neighbors:
                if neighbor not in path:
                    path.append(neighbor)
                    dfs(neighbor, path, depth + 1)
                    path.pop()
        
        dfs(src, [src], 0)
        return paths
    
    def find_hybrid_fast_paths(self, src: str, dst: str, count: int) -> List[SimplePath]:
        """
        混合快速算法 - 结合多种策略
        优先速度，确保生成指定数量的路径
        策略优先级：最短路径 -> 随机游走 -> DFS -> 权重扰动 -> 随机游走(扩展) -> BFS(最后备选)
        """
        all_paths = []
        paths_set = set()
        
        # 策略1: 最短路径 (1条)
        shortest = self.find_shortest_path(src, dst)
        if shortest:
            all_paths.append(shortest)
            paths_set.add(hash(shortest))
        
        # 策略2: 随机游走 (50%的路径) - 最快的方法
        if len(all_paths) < count:
            random_count = max(5, int(count * 0.5))
            random_paths = self.find_random_walk_paths(src, dst, random_count)
            for path in random_paths:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    all_paths.append(path)
                    paths_set.add(path_hash)
        
        # 策略3: DFS (20%的路径) - 速度较快
        if len(all_paths) < count:
            dfs_count = max(5, int(count * 0.5))
            dfs_paths = self.find_dfs_paths(src, dst, dfs_count)
            for path in dfs_paths:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    all_paths.append(path)
                    paths_set.add(path_hash)
        
        # 策略4: 权重扰动 (10%的路径) - 质量好但数量有限
        if len(all_paths) < count:
            weight_count = max(5, int(count * 0.5))
            weight_paths = self.find_weight_perturbed_paths(src, dst, weight_count)
            for path in weight_paths:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    all_paths.append(path)
                    paths_set.add(path_hash)
        
        # 策略5: 继续用随机游走填充 (扩展搜索范围)
        if len(all_paths) < count:
            extra_random_count = count - len(all_paths)
            extra_random_paths = self.find_random_walk_paths(src, dst, extra_random_count, max_hops=20)
            for path in extra_random_paths:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    all_paths.append(path)
                    paths_set.add(path_hash)
        
        # 策略6 (最后备选): BFS - 仅在其他方法都不足时使用
        # BFS速度较慢，但能保证找到路径
        if len(all_paths) < count:
            bfs_count = count - len(all_paths)
            print(f"    使用BFS补充 {bfs_count} 条路径 (当前已有 {len(all_paths)} 条)")
            bfs_paths = self.find_bfs_paths(src, dst, bfs_count)
            for path in bfs_paths:
                path_hash = hash(path)
                if path_hash not in paths_set:
                    all_paths.append(path)
                    paths_set.add(path_hash)
        
        return all_paths[:count]

def load_flows(flows_file: str) -> List[Tuple[str, str]]:
    """加载流数据，返回源-目标对列表"""
    with open(flows_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    flow_pairs = []
    for flow_data in data['st_flows']:
        src = flow_data['source_node']
        dst = flow_data['destination_node']
        flow_pairs.append((src, dst))
    
    return flow_pairs

def test_algorithm_performance(algorithm_name: str, generator: FastPathGenerator, 
                               flow_pairs: List[Tuple[str, str]], paths_per_flow: int):
    """测试单个算法的性能"""
    print(f"\n{'='*80}")
    print(f"测试算法: {algorithm_name}")
    print(f"每条流生成路径数: {paths_per_flow}")
    print(f"{'='*80}")
    
    start_time = time.time()
    total_paths = 0
    successful_flows = 0
    path_lengths = []
    
    for i, (src, dst) in enumerate(flow_pairs):
        if i % 100 == 0:
            print(f"进度: {i}/{len(flow_pairs)} ({i/len(flow_pairs)*100:.1f}%)")
        
        if algorithm_name == "随机游走":
            paths = generator.find_random_walk_paths(src, dst, paths_per_flow)
        elif algorithm_name == "分层BFS":
            paths = generator.find_bfs_paths(src, dst, paths_per_flow)
        elif algorithm_name == "深度优先":
            paths = generator.find_dfs_paths(src, dst, paths_per_flow)
        elif algorithm_name == "权重扰动":
            paths = generator.find_weight_perturbed_paths(src, dst, paths_per_flow)
        elif algorithm_name == "混合快速":
            paths = generator.find_hybrid_fast_paths(src, dst, paths_per_flow)
        else:
            paths = []
        
        if paths:
            successful_flows += 1
            total_paths += len(paths)
            for path in paths:
                path_lengths.append(len(path.nodes))
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # 统计结果
    print(f"\n性能结果:")
    print(f"  总时间: {elapsed_time:.4f}秒")
    print(f"  成功流数: {successful_flows}/{len(flow_pairs)}")
    print(f"  总路径数: {total_paths}")
    print(f"  平均每流路径数: {total_paths/len(flow_pairs):.2f}")
    print(f"  平均每流时间: {elapsed_time/len(flow_pairs):.6f}秒")
    print(f"  平均每条路径时间: {elapsed_time/total_paths:.6f}秒" if total_paths > 0 else "  无路径生成")
    
    if path_lengths:
        avg_length = sum(path_lengths) / len(path_lengths)
        min_length = min(path_lengths)
        max_length = max(path_lengths)
        print(f"  路径长度统计: 平均={avg_length:.2f}, 最小={min_length}, 最大={max_length}")
    
    return {
        'algorithm': algorithm_name,
        'total_time': elapsed_time,
        'total_paths': total_paths,
        'successful_flows': successful_flows,
        'avg_time_per_flow': elapsed_time/len(flow_pairs),
        'avg_time_per_path': elapsed_time/total_paths if total_paths > 0 else 0,
        'avg_path_length': sum(path_lengths)/len(path_lengths) if path_lengths else 0
    }

def compare_algorithms(paths_per_flow_list: List[int]):
    """比较不同算法在不同路径数量下的性能"""
    print("="*80)
    print("快速路径生成算法性能测试")
    print("="*80)
    
    # 加载数据
    print("\n加载拓扑和流数据...")
    generator = FastPathGenerator("tsn_mesh_550_dual_distance.json")
    flow_pairs = load_flows("tsn_st_flows_id_priority.json")
    
    print(f"流数据: {len(flow_pairs)}条流")
    
    # 使用前100条流进行测试
    test_flow_pairs = flow_pairs[:100]
    print(f"测试流数: {len(test_flow_pairs)}条")
    
    all_results = []
    
    # 测试不同的路径数量
    for paths_per_flow in paths_per_flow_list:
        print(f"\n{'#'*80}")
        print(f"测试配置: 每条流生成 {paths_per_flow} 条路径")
        print(f"{'#'*80}")
        
        # 测试不同算法
        algorithms = [
            "随机游走",
            "分层BFS",
            "深度优先",
            "混合快速"
        ]
        
        for algo in algorithms:
            result = test_algorithm_performance(algo, generator, test_flow_pairs, paths_per_flow)
            result['paths_per_flow'] = paths_per_flow
            all_results.append(result)
    
    # 汇总结果
    print(f"\n{'='*80}")
    print("性能测试总结")
    print(f"{'='*80}")
    print(f"{'算法':<12} {'路径数/流':<10} {'总时间(s)':<12} {'总路径':<10} "
          f"{'时间/流(ms)':<15} {'时间/路径(ms)':<18} {'平均长度':<10}")
    print("-"*100)
    
    for result in all_results:
        print(f"{result['algorithm']:<12} {result['paths_per_flow']:<10} "
              f"{result['total_time']:<12.4f} {result['total_paths']:<10} "
              f"{result['avg_time_per_flow']*1000:<15.2f} "
              f"{result['avg_time_per_path']*1000:<18.6f} "
              f"{result['avg_path_length']:<10.2f}")
    
    # 推荐最佳算法
    print(f"\n{'='*80}")
    print("算法推荐")
    print(f"{'='*80}")
    
    # 按速度排序
    fastest = min(all_results, key=lambda x: x['avg_time_per_flow'])
    print(f"最快算法: {fastest['algorithm']} "
          f"(每流 {fastest['paths_per_flow']} 条路径, "
          f"{fastest['avg_time_per_flow']*1000:.2f}ms/流)")
    
    # 按路径生成率排序
    best_generation = max(all_results, key=lambda x: x['total_paths']/x['total_time'] if x['total_time'] > 0 else 0)
    print(f"最高生成率: {best_generation['algorithm']} "
          f"(每流 {best_generation['paths_per_flow']} 条路径, "
          f"{best_generation['total_paths']/best_generation['total_time']:.2f} 路径/秒)")

def test_scalability():
    """测试可扩展性 - 1000条流"""
    print(f"\n{'='*80}")
    print("可扩展性测试 - 1000条流")
    print(f"{'='*80}")
    
    generator = FastPathGenerator("tsn_mesh_550_dual_distance.json")
    flow_pairs = load_flows("tsn_st_flows_id_priority.json")
    
    # 使用全部1000条流
    test_scales = [100, 500, 1000]
    paths_per_flow = 10  # 每条流生成10条路径
    
    print(f"测试配置: 每条流生成 {paths_per_flow} 条路径")
    
    results = []
    
    for scale in test_scales:
        if scale > len(flow_pairs):
            continue
        
        print(f"\n测试规模: {scale}条流")
        test_flows = flow_pairs[:scale]
        
        start_time = time.time()
        total_paths = 0
        
        for i, (src, dst) in enumerate(test_flows):
            if i % 100 == 0:
                print(f"  进度: {i}/{scale}")
            
            paths = generator.find_hybrid_fast_paths(src, dst, paths_per_flow)
            total_paths += len(paths)
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        result = {
            'scale': scale,
            'time': elapsed_time,
            'paths': total_paths,
            'time_per_flow': elapsed_time / scale,
            'paths_per_sec': total_paths / elapsed_time if elapsed_time > 0 else 0
        }
        results.append(result)
        
        print(f"  时间: {elapsed_time:.4f}秒")
        print(f"  路径数: {total_paths}")
        print(f"  每流时间: {elapsed_time/scale:.6f}秒")
        print(f"  生成率: {total_paths/elapsed_time:.2f} 路径/秒")
    
    # 分析可扩展性
    print(f"\n可扩展性分析:")
    print(f"{'流数':<10} {'时间(s)':<12} {'路径数':<10} {'时间/流(ms)':<15} {'路径/秒':<12}")
    print("-"*60)
    
    for result in results:
        print(f"{result['scale']:<10} {result['time']:<12.4f} {result['paths']:<10} "
              f"{result['time_per_flow']*1000:<15.2f} {result['paths_per_sec']:<12.2f}")
    
    # 线性度分析
    if len(results) >= 2:
        time_ratio = results[-1]['time'] / results[0]['time']
        scale_ratio = results[-1]['scale'] / results[0]['scale']
        linearity = time_ratio / scale_ratio
        
        print(f"\n时间增长比例: {time_ratio:.2f}x")
        print(f"规模增长比例: {scale_ratio:.2f}x")
        print(f"线性度: {linearity:.2f} ({'线性' if linearity < 1.2 else '超线性' if linearity < 2 else '非线性'})")

if __name__ == "__main__":
    # 测试不同的路径数量配置
    paths_per_flow_configs = [5, 10, 20]
    
    # 比较算法
    compare_algorithms(paths_per_flow_configs)
    
    # 可扩展性测试
    test_scalability()
    
    print(f"\n{'='*80}")
    print("测试完成！")
    print(f"{'='*80}")
