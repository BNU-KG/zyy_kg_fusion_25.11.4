import json
import pandas as pd
from collections import defaultdict


def extract_knowledge_graph_from_excel(file_path):
    """
    从Excel文件中提取知识图谱并构造JSON结构
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path)
        print(f"成功读取Excel文件，共{len(df)}行数据")

        # 初始化结果列表和ID计数器
        result = []
        current_id = 0

        # 1. 处理学段 (level 0)
        xueduan_data = df[['学段', '学段编号']].drop_duplicates()
        if len(xueduan_data) > 0:
            xueduan_row = xueduan_data.iloc[0]
            xueduan_node = {
                "id": current_id,
                "name": str(xueduan_row['学段']) if pd.notna(xueduan_row['学段']) else "",
                "xueduanID": str(xueduan_row['学段编号']) if pd.notna(xueduan_row['学段编号']) else "",
                "level": 0,
                "relatedID": []
            }
            result.append(xueduan_node)
            current_id += 1
            print(f"已创建学段节点: {xueduan_node['name']}")
        else:
            print("警告: 未找到学段数据")
            return []

        # 存储各级别节点的映射关系
        level1_map = {}  # 一级知识点编号 -> 节点ID
        level2_map = {}  # 二级知识点编号 -> 节点ID
        level3_map = {}  # 三级知识点编号 -> 节点ID

        # 2. 处理一级知识点 (level 1)
        level1_data = df[['一级知识点', '一级知识点编号', '一级知识点描述']].drop_duplicates()
        for _, row in level1_data.iterrows():
            if pd.notna(row['一级知识点']) and str(row['一级知识点']).strip():
                level1_node = {
                    "id": current_id,
                    "name": str(row['一级知识点']),
                    "zhishidianid": str(row['一级知识点编号']) if pd.notna(row['一级知识点编号']) else "",
                    "yijizhishidianmiaoshu": str(row['一级知识点描述']) if pd.notna(row['一级知识点描述']) else "",
                    "level": 1,
                    "relatedID": []
                }
                result.append(level1_node)
                if pd.notna(row['一级知识点编号']):
                    level1_map[str(row['一级知识点编号'])] = current_id
                # 添加到学段的relatedID中
                result[0]["relatedID"].append(current_id)
                current_id += 1

        print(f"已创建{len(level1_map)}个一级知识点节点")

        # 3. 处理二级知识点 (level 2)
        level2_data = df[['二级知识点', '二级知识点编号', '一级知识点编号']].drop_duplicates()
        for _, row in level2_data.iterrows():
            if pd.notna(row['二级知识点']) and str(row['二级知识点']).strip():
                level2_node = {
                    "id": current_id,
                    "name": str(row['二级知识点']),
                    "zhishidianid": str(row['二级知识点编号']) if pd.notna(row['二级知识点编号']) else "",
                    "level": 2,
                    "relatedID": []
                }
                result.append(level2_node)
                if pd.notna(row['二级知识点编号']):
                    level2_map[str(row['二级知识点编号'])] = current_id

                # 添加到对应一级知识点的relatedID中
                if pd.notna(row['一级知识点编号']):
                    parent_key = str(row['一级知识点编号'])
                    if parent_key in level1_map:
                        parent_id = level1_map[parent_key]
                        for node in result:
                            if node["id"] == parent_id:
                                node["relatedID"].append(current_id)
                                break

                current_id += 1

        print(f"已创建{len(level2_map)}个二级知识点节点")

        # 4. 处理三级知识点 (level 3)
        level3_data = df[['三级知识点', '三级知识点编号', '二级知识点编号']].drop_duplicates()
        for _, row in level3_data.iterrows():
            if pd.notna(row['三级知识点']) and str(row['三级知识点']).strip():
                level3_node = {
                    "id": current_id,
                    "name": str(row['三级知识点']),
                    "zhishidianid": str(row['三级知识点编号']) if pd.notna(row['三级知识点编号']) else "",
                    "level": 3,
                    "relatedID": []
                }
                result.append(level3_node)
                if pd.notna(row['三级知识点编号']):
                    level3_map[str(row['三级知识点编号'])] = current_id

                # 添加到对应二级知识点的relatedID中
                if pd.notna(row['二级知识点编号']):
                    parent_key = str(row['二级知识点编号'])
                    if parent_key in level2_map:
                        parent_id = level2_map[parent_key]
                        for node in result:
                            if node["id"] == parent_id:
                                node["relatedID"].append(current_id)
                                break

                current_id += 1

        print(f"已创建{len(level3_map)}个三级知识点节点")

        # 5. 处理四级知识点 (level 4)
        level4_data = df[['四级知识点', '四级知识点编号', '实验描述', '三级知识点编号']].drop_duplicates()
        level4_count = 0

        for _, row in level4_data.iterrows():
            if pd.notna(row['四级知识点']) and str(row['四级知识点']).strip():
                level4_node = {
                    "id": current_id,
                    "name": str(row['四级知识点']),
                    "zhishidianid": str(row['四级知识点编号']) if pd.notna(row['四级知识点编号']) else "",
                    "level": 4,
                    "shiyanmiaoshu": str(row['实验描述']) if pd.notna(row['实验描述']) else ""
                }
                result.append(level4_node)
                level4_count += 1

                # 添加到对应三级知识点的relatedID中
                if pd.notna(row['三级知识点编号']):
                    parent_key = str(row['三级知识点编号'])
                    if parent_key in level3_map:
                        parent_id = level3_map[parent_key]
                        for node in result:
                            if node["id"] == parent_id and node["level"] == 3:
                                node["relatedID"].append(current_id)
                                break

                current_id += 1

        print(f"已创建{level4_count}个四级知识点节点")

        # 统计信息
        total_nodes = len(result)
        level_counts = {}
        for node in result:
            level = node['level']
            level_counts[level] = level_counts.get(level, 0) + 1

        print(f"\n知识图谱构建完成！")
        print(f"总节点数: {total_nodes}")
        for level in sorted(level_counts.keys()):
            print(f"Level {level}节点数: {level_counts[level]}")

        return result

    except Exception as e:
        print(f"处理Excel文件时出现错误: {e}")
        return []


def save_knowledge_graph(knowledge_graph, output_file='knowledge_graph.json'):
    """
    保存知识图谱到JSON文件
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(knowledge_graph, f, ensure_ascii=False, indent=2)
        print(f"知识图谱已保存到: {output_file}")
    except Exception as e:
        print(f"保存文件时出现错误: {e}")


def main():
    # Excel文件路径
    excel_file = '生物课标构建知识点图谱(1).xlsx'

    # 提取知识图谱
    knowledge_graph = extract_knowledge_graph_from_excel(excel_file)

    if knowledge_graph:
        # 保存为JSON文件
        save_knowledge_graph(knowledge_graph)

        # 可选：打印前几个节点作为示例
        print("\n前5个节点示例:")
        for i, node in enumerate(knowledge_graph[:5]):
            print(f"{i + 1}. {node}")
    else:
        print("知识图谱提取失败，请检查Excel文件格式和内容")


if __name__ == "__main__":
    # 检查必要的库
    try:
        import pandas as pd
    except ImportError:
        print("请先安装pandas库: pip install pandas openpyxl")
        exit(1)

    main()