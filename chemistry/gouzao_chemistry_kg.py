import pandas as pd
import json
from collections import defaultdict


def build_knowledge_graph_from_excel(file_path):
    # 读取Excel文件
    df = pd.read_excel(file_path)

    # 打印数据基本信息用于调试
    print(f"Excel数据行数: {len(df)}")
    print(f"列名: {list(df.columns)}")

    # 初始化图谱列表和ID计数器
    knowledge_graph = []
    current_id = 0

    # 用于存储各层级节点的映射关系
    level0_nodes = {}  # 学段节点
    level1_nodes = {}  # 一级知识点节点
    level2_nodes = {}  # 二级知识点节点
    level3_nodes = {}  # 三级知识点节点

    # 第一轮：处理学段节点（level 0）
    print("处理学段节点...")
    for _, row in df.iterrows():
        level0_name = str(row['学段']).strip()
        level0_code = str(row['学段编号']).strip()

        if level0_name and level0_name != 'nan' and level0_name != 'NaN':
            if level0_name not in level0_nodes:
                level0_node = {
                    "id": current_id,
                    "name": level0_name,
                    "xueduanID": level0_code,
                    "level": 0,
                    "relatedID": []
                }
                knowledge_graph.append(level0_node)
                level0_nodes[level0_name] = current_id
                current_id += 1
                print(f"创建学段节点: {level0_name} (ID: {level0_nodes[level0_name]})")

    # 第二轮：处理一级知识点节点（level 1）
    print("处理一级知识点节点...")
    for _, row in df.iterrows():
        level0_name = str(row['学段']).strip()
        level1_name = str(row['一级知识点']).strip()
        level1_code = str(row['一级知识点编号']).strip()

        if level1_name and level1_name != 'nan' and level1_name != 'NaN':
            level1_key = level1_name

            if level1_key not in level1_nodes:
                level1_node = {
                    "id": current_id,
                    "name": level1_name,
                    "zhishidianid": level1_code,
                    "zhishidianyaoqiuid": "",
                    "level": 1,
                    "relatedID": []
                }
                knowledge_graph.append(level1_node)
                level1_nodes[level1_key] = current_id

                # 建立学段到一级知识点的关系
                if level0_name and level0_name != 'nan' and level0_name in level0_nodes:
                    level0_id = level0_nodes[level0_name]
                    knowledge_graph[level0_id]["relatedID"].append(current_id)
                    print(f"建立关系: 学段 {level0_name} -> 一级知识点 {level1_name}")

                current_id += 1
                print(f"创建一级知识点节点: {level1_name} (ID: {level1_nodes[level1_key]})")

    # 第三轮：处理二级知识点节点（level 2）- 修复版本
    print("处理二级知识点节点...")
    for _, row in df.iterrows():
        level1_name = str(row['一级知识点']).strip()
        level2_name = str(row['二级知识点']).strip()
        level2_code = str(row['二级知识点编号']).strip()
        level2_desc = str(row['二级知识点要求']).strip()
        level2_desc_code = str(row['二级知识点要求编号']).strip()

        if level2_name and level2_name != 'nan' and level2_name != 'NaN':
            # 使用二级知识点要求作为唯一标识，确保每个不同的要求都创建独立节点
            level2_key = f"{level2_name}_{level2_desc}_{level2_desc_code}"

            if level2_key not in level2_nodes:
                level2_node = {
                    "id": current_id,
                    "name": level2_name,
                    "zhishidianid": level2_code,
                    "zhishidianyaoqiu": level2_desc if level2_desc != 'nan' else "",
                    "zhishidianyaoqiuid": level2_desc_code if level2_desc_code != 'nan' else "",
                    "level": 2,
                    "relatedID": []
                }
                knowledge_graph.append(level2_node)
                level2_nodes[level2_key] = current_id

                # 建立一级知识点到二级知识点的关系
                if level1_name and level1_name != 'nan' and level1_name in level1_nodes:
                    level1_id = level1_nodes[level1_name]
                    knowledge_graph[level1_id]["relatedID"].append(current_id)
                    print(f"建立关系: 一级知识点 {level1_name} -> 二级知识点 {level2_name}")

                current_id += 1
                print(f"创建二级知识点节点: {level2_name} (要求: {level2_desc}, ID: {level2_nodes[level2_key]})")

    # 第四轮：处理三级知识点节点（level 3）
    print("处理三级知识点节点...")
    processed_count = 0
    for index, row in df.iterrows():
        level2_name = str(row['二级知识点']).strip()
        level2_desc = str(row['二级知识点要求']).strip()
        level2_desc_code = str(row['二级知识点要求编号']).strip()
        level3_name = str(row['三级知识点']).strip()
        level3_code = str(row['三级知识点编号']).strip()
        level3_desc = str(row['三级知识点要求']).strip()
        level3_desc_code = str(row['三级知识点要求编号']).strip()

        # 检查是否为NaN或空字符串
        if level3_name and level3_name != 'nan' and level3_name != 'NaN':
            # 使用行索引和三级知识点编号作为唯一标识，确保每个三级知识点都被处理
            level3_key = f"{index}_{level3_name}_{level3_code}"

            if level3_key not in level3_nodes:
                level3_node = {
                    "id": current_id,
                    "name": level3_name,
                    "zhishidianid": level3_code,
                    "zhishidianyaoqiu": level3_desc if level3_desc != 'nan' else "",
                    "zhishidianyaoqiuid": level3_desc_code if level3_desc_code != 'nan' else "",
                    "level": 3
                }
                knowledge_graph.append(level3_node)
                level3_nodes[level3_key] = current_id

                # 建立二级知识点到三级知识点的关系
                if level2_name and level2_name != 'nan':
                    # 使用相同的规则查找对应的二级知识点
                    level2_key = f"{level2_name}_{level2_desc}_{level2_desc_code}"
                    if level2_key in level2_nodes:
                        level2_id = level2_nodes[level2_key]
                        knowledge_graph[level2_id]["relatedID"].append(current_id)
                        print(
                            f"建立关系: 二级知识点 {level2_name} -> 三级知识点 {level3_name} (ID: {current_id}, 编号: {level3_code})")
                    else:
                        print(f"警告: 未找到二级知识点 {level2_name} 对应的节点")

                current_id += 1
                processed_count += 1

    print(f"实际处理的三级知识点行数: {processed_count}")

    # 打印详细的统计信息
    print(f"\n=== 详细统计信息 ===")
    print(f"Excel总行数: {len(df)}")
    print(f"学段节点数量: {len(level0_nodes)}")
    print(f"一级知识点节点数量: {len(level1_nodes)}")
    print(f"二级知识点节点数量: {len(level2_nodes)}")
    print(f"三级知识点节点数量: {len(level3_nodes)}")
    print(f"总节点数量: {len(knowledge_graph)}")

    # 检查每个二级知识点下的三级知识点数量
    print(f"\n=== 二级知识点下的三级知识点统计 ===")
    for level2_key, level2_id in level2_nodes.items():
        level2_node = knowledge_graph[level2_id]
        level2_name = level2_node['name']
        related_count = len(level2_node.get('relatedID', []))
        print(f"二级知识点 '{level2_name}': {related_count} 个三级知识点")

    return knowledge_graph


def save_knowledge_graph_to_json(knowledge_graph, output_file):
    """将知识图谱保存为JSON文件"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(knowledge_graph, f, ensure_ascii=False, indent=2)


# 使用示例
if __name__ == "__main__":
    # 输入Excel文件路径
    excel_file_path = "化学课标构建知识点图谱(1).xlsx"

    try:
        # 构建知识图谱
        graph = build_knowledge_graph_from_excel(excel_file_path)

        # 输出结果
        print(f"\n构建完成的知识图谱节点数量: {len(graph)}")
        print("\n各层级节点数量统计:")

        # 按层级统计
        levels = {}
        for node in graph:
            level = node['level']
            if level not in levels:
                levels[level] = 0
            levels[level] += 1

        for level in sorted(levels.keys()):
            print(f"Level {level}: {levels[level]} 个节点")

        # 保存为JSON文件
        output_json_file = "knowledge_graph_chemistry_complete.json"
        save_knowledge_graph_to_json(graph, output_json_file)
        print(f"\n知识图谱已保存到: {output_json_file}")

    except Exception as e:
        print(f"处理过程中出现错误: {e}")
        import traceback

        traceback.print_exc()
