import json
import pandas as pd
from collections import OrderedDict


def read_excel_and_generate_graph(excel_file_path):
    """
    从Excel文件读取数据并严格按照层级顺序生成知识图谱
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file_path)
        df = df.fillna('')  # 填充空值

        nodes = []
        current_id = 0

        print("开始处理Excel数据...")
        print(f"数据行数: {len(df)}")

        # 1. 处理学段 (level 0)
        print("=== 处理学段 (level 0) ===")
        xueduan_data = df.iloc[0]
        xueduan_node = {
            "id": current_id,
            "name": xueduan_data["学段"],
            "xueduanID": xueduan_data["学段编号"],
            "level": 0,
            "relatedID": []
        }
        current_id += 1
        nodes.append(xueduan_node)
        print(f"创建学段节点: ID={xueduan_node['id']}, Name={xueduan_node['name']}")

        # 存储映射关系
        level1_map = OrderedDict()  # 一级知识点名称 -> 节点信息
        level2_map = OrderedDict()  # 二级知识点名称 -> 节点信息
        level3_map = OrderedDict()  # 三级知识点名称 -> 节点信息

        # 2. 处理一级知识点 (level 1)
        print("\n=== 处理一级知识点 (level 1) ===")
        for _, row in df.iterrows():
            level1_name = row["一级知识点"]
            if level1_name and level1_name not in level1_map:
                level1_node = {
                    "id": current_id,
                    "name": level1_name,
                    "zhishidianid": row["一级知识点编号"],
                    "level": 1,
                    "relatedID": []
                }
                level1_map[level1_name] = level1_node
                nodes.append(level1_node)
                print(f"创建一级知识点: ID={current_id}, Name={level1_name}")
                current_id += 1

        # 3. 处理二级知识点 (level 2)
        print("\n=== 处理二级知识点 (level 2) ===")
        for _, row in df.iterrows():
            level2_name = row["二级知识点"]
            if level2_name and level2_name not in level2_map:
                level2_node = {
                    "id": current_id,
                    "name": level2_name,
                    "zhishidianid": row["二级知识点编号"],
                    "level": 2,
                    "relatedID": []
                }
                level2_map[level2_name] = level2_node
                nodes.append(level2_node)
                print(f"创建二级知识点: ID={current_id}, Name={level2_name}")
                current_id += 1

        # 4. 处理三级知识点 (level 3)
        print("\n=== 处理三级知识点 (level 3) ===")
        for _, row in df.iterrows():
            level3_name = row["三级知识点"]
            if level3_name and level3_name not in level3_map:
                level3_node = {
                    "id": current_id,
                    "name": level3_name,
                    "zhishidianid": row["三级知识点编号"],
                    "level": 3,
                    "litishuliang": str(row.get("例题数量", 0))
                }
                level3_map[level3_name] = level3_node
                nodes.append(level3_node)
                print(f"创建三级知识点: ID={current_id}, Name={level3_name}")
                current_id += 1

        # 建立层级关系
        print("\n=== 建立层级关系 ===")

        # 学段 → 一级知识点
        print("建立学段 → 一级知识点关系:")
        for level1_name, level1_node in level1_map.items():
            xueduan_node["relatedID"].append(level1_node["id"])
            print(f"  学段[{xueduan_node['id']}] → 一级[{level1_node['id']}]: {level1_name}")

        # 一级知识点 → 二级知识点
        print("\n建立一级知识点 → 二级知识点关系:")
        for _, row in df.iterrows():
            level1_name = row["一级知识点"]
            level2_name = row["二级知识点"]

            if level1_name in level1_map and level2_name in level2_map:
                level1_id = level1_map[level1_name]["id"]
                level2_id = level2_map[level2_name]["id"]

                if level2_id not in level1_map[level1_name]["relatedID"]:
                    level1_map[level1_name]["relatedID"].append(level2_id)
                    print(f"  一级[{level1_id}] → 二级[{level2_id}]: {level2_name}")

        # 二级知识点 → 三级知识点
        print("\n建立二级知识点 → 三级知识点关系:")
        for _, row in df.iterrows():
            level2_name = row["二级知识点"]
            level3_name = row["三级知识点"]

            if level2_name in level2_map and level3_name in level3_map:
                level2_id = level2_map[level2_name]["id"]
                level3_id = level3_map[level3_name]["id"]

                if level3_id not in level2_map[level2_name]["relatedID"]:
                    level2_map[level2_name]["relatedID"].append(level3_id)
                    print(f"  二级[{level2_id}] → 三级[{level3_id}]: {level3_name}")

        # 验证ID连续性
        print(f"\n=== ID连续性验证 ===")
        print(f"总节点数: {len(nodes)}")
        print(f"ID范围: 0 ~ {current_id - 1}")

        # 检查ID是否连续
        expected_ids = list(range(current_id))
        actual_ids = [node["id"] for node in nodes]

        if expected_ids == actual_ids:
            print("✓ ID连续性验证通过")
        else:
            print("✗ ID连续性问题")
            missing_ids = set(expected_ids) - set(actual_ids)
            duplicate_ids = set([x for x in actual_ids if actual_ids.count(x) > 1])
            print(f"缺失ID: {missing_ids}")
            print(f"重复ID: {duplicate_ids}")

        # 检查层级分布
        level_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for node in nodes:
            level_counts[node["level"]] += 1

        print(f"\n=== 层级分布 ===")
        for level, count in level_counts.items():
            print(f"Level {level}: {count}个节点")

        return nodes

    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return None


def save_knowledge_graph(nodes, filename="knowledge_graph.json"):
    """
    保存知识图谱到JSON文件
    """
    result = {
        "knowledgeGraph": nodes,
        "totalNodes": len(nodes),
        "levels": {
            "level0": len([node for node in nodes if node["level"] == 0]),
            "level1": len([node for node in nodes if node["level"] == 1]),
            "level2": len([node for node in nodes if node["level"] == 2]),
            "level3": len([node for node in nodes if node["level"] == 3])
        }
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result


# 主程序
if __name__ == "__main__":
    try:
        # Excel文件路径
        excel_file_path = "物理课表构建知识点图谱(2).xlsx"

        print(f"开始读取Excel文件: {excel_file_path}")

        # 读取Excel并生成知识图谱
        knowledge_nodes = read_excel_and_generate_graph(excel_file_path)

        if knowledge_nodes:
            # 保存结果
            result = save_knowledge_graph(knowledge_nodes, "physics_knowledge_graph.json")

            print(f"\n=== 最终结果 ===")
            print(f"总节点数: {result['totalNodes']}")
            print(f"学段节点 (level 0): {result['levels']['level0']}")
            print(f"一级知识点 (level 1): {result['levels']['level1']}")
            print(f"二级知识点 (level 2): {result['levels']['level2']}")
            print(f"三级知识点 (level 3): {result['levels']['level3']}")
            print(f"结果已保存到 physics_knowledge_graph.json")

            # 打印节点结构示例
            print(f"\n=== 节点结构示例 ===")
            for i, node in enumerate(knowledge_nodes[:10]):  # 显示前10个节点
                level_desc = {0: "学段", 1: "一级", 2: "二级", 3: "三级"}[node["level"]]
                print(f"\nID {node['id']} [{level_desc}级]:")
                print(f"  名称: {node['name']}")

                if node["level"] == 0:
                    print(f"  学段ID: {node['xueduanID']}")
                    print(f"  关联一级知识点: {node['relatedID']}")
                elif node["level"] == 1:
                    print(f"  知识点ID: {node['zhishidianid']}")
                    print(f"  关联二级知识点: {node['relatedID']}")
                elif node["level"] == 2:
                    print(f"  知识点ID: {node['zhishidianid']}")
                    print(f"  关联三级知识点: {node['relatedID']}")
                elif node["level"] == 3:
                    print(f"  知识点ID: {node['zhishidianid']}")
                    print(f"  例题数量: {node['litishuliang']}")

        else:
            print("知识图谱生成失败！")

    except Exception as e:
        print(f"处理过程中出现错误: {e}")
        import traceback

        traceback.print_exc()