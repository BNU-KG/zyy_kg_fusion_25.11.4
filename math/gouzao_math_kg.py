import pandas as pd
import json
import os


def create_knowledge_graph_from_excel():
    """
    从Excel文件读取数据并生成知识图谱JSON
    """
    try:
        # 读取Excel文件
        file_path = "数学课标构建知识点图谱.xlsx"
        df = pd.read_excel(file_path, sheet_name='Sheet1')

        # 打印列名以便调试
        print("Excel文件列名:", df.columns.tolist())
        print("数据行数:", len(df))

        # 数据清洗：处理NaN值
        df = df.fillna('')

        knowledge_graph = []
        current_id = 0

        # Level 0: 学段（只有一个学段）
        xueduan_data = df[['学段', '学段编号']].drop_duplicates().iloc[0]
        level0_related_ids = []

        # 收集所有一级知识点的id
        level1_nodes = df[['一级知识点', '一级知识点编号']].drop_duplicates()
        level1_nodes = level1_nodes[level1_nodes['一级知识点'] != '']

        print(f"发现 {len(level1_nodes)} 个一级知识点")

        for i in range(len(level1_nodes)):
            level0_related_ids.append(current_id + 1 + i)

        level0_node = {
            'id': current_id,
            'name': str(xueduan_data['学段']),
            'xueduanID': str(xueduan_data['学段编号']),
            'level': 0,
            'relatedID': level0_related_ids
        }
        knowledge_graph.append(level0_node)
        current_id += 1
        print(f"创建学段节点: {level0_node['name']}")

        # Level 1: 一级知识点
        level1_nodes_data = []

        for i, (_, row) in enumerate(level1_nodes.iterrows()):
            level1_name = str(row['一级知识点'])
            level1_id = str(row['一级知识点编号'])

            # 获取该一级知识点下的所有二级知识点
            level2_subset = df[df['一级知识点'] == level1_name]
            level2_nodes = level2_subset[['二级知识点', '二级知识点编号']].drop_duplicates()
            level2_nodes = level2_nodes[level2_nodes['二级知识点'] != '']

            # 计算二级知识点的ID范围
            level2_start_id = current_id + len(level1_nodes_data) + 1
            level2_related_ids = list(range(level2_start_id, level2_start_id + len(level2_nodes)))

            level1_node = {
                'id': current_id,
                'name': level1_name,
                'zhishidianid': level1_id,
                'level': 1,
                'relatedID': level2_related_ids
            }
            level1_nodes_data.append(level1_node)
            current_id += 1

        knowledge_graph.extend(level1_nodes_data)
        print(f"创建 {len(level1_nodes_data)} 个一级知识点节点")

        # Level 2: 二级知识点
        level2_nodes_data = []

        for _, level1_row in level1_nodes.iterrows():
            level1_name = str(level1_row['一级知识点'])
            level2_subset = df[df['一级知识点'] == level1_name]
            level2_unique = level2_subset[['二级知识点', '二级知识点编号']].drop_duplicates()
            level2_unique = level2_unique[level2_unique['二级知识点'] != '']

            for _, level2_row in level2_unique.iterrows():
                level2_name = str(level2_row['二级知识点'])
                level2_id = str(level2_row['二级知识点编号'])

                # 获取该二级知识点下的所有三级知识点
                level3_subset = level2_subset[level2_subset['二级知识点'] == level2_name]
                level3_nodes = level3_subset[['三级知识点', '三级知识点编号']].drop_duplicates()
                level3_nodes = level3_nodes[level3_nodes['三级知识点'] != '']

                # 计算三级知识点的ID范围
                level3_start_id = current_id + len(level2_nodes_data) + 1
                level3_related_ids = list(range(level3_start_id, level3_start_id + len(level3_nodes)))

                level2_node = {
                    'id': current_id,
                    'name': level2_name,
                    'zhishidianid': level2_id,
                    'level': 2,
                    'relatedID': level3_related_ids
                }
                level2_nodes_data.append(level2_node)
                current_id += 1

        knowledge_graph.extend(level2_nodes_data)
        print(f"创建 {len(level2_nodes_data)} 个二级知识点节点")

        # Level 3: 三级知识点
        level3_nodes_data = []

        for _, level1_row in level1_nodes.iterrows():
            level1_name = str(level1_row['一级知识点'])
            level2_subset = df[df['一级知识点'] == level1_name]

            for _, level2_row in level2_subset[['二级知识点', '二级知识点编号']].drop_duplicates().iterrows():
                level2_name = str(level2_row['二级知识点'])
                if level2_name == '':
                    continue

                level3_subset = level2_subset[level2_subset['二级知识点'] == level2_name]
                level3_unique = level3_subset[['三级知识点', '三级知识点编号']].drop_duplicates()
                level3_unique = level3_unique[level3_unique['三级知识点'] != '']

                for _, level3_row in level3_unique.iterrows():
                    level3_name = str(level3_row['三级知识点'])
                    level3_id = str(level3_row['三级知识点编号'])

                    # 获取该三级知识点下的所有四级知识点
                    level4_subset = level3_subset[level3_subset['三级知识点'] == level3_name]
                    level4_nodes = level4_subset[['四级知识点', '四级知识点编号']].drop_duplicates()
                    level4_nodes = level4_nodes[level4_nodes['四级知识点'] != '']

                    # 计算四级知识点的ID范围
                    level4_start_id = current_id + len(level3_nodes_data) + 1
                    level4_related_ids = list(range(level4_start_id, level4_start_id + len(level4_nodes)))

                    level3_node = {
                        'id': current_id,
                        'name': level3_name,
                        'zhishidianid': level3_id,
                        'level': 3,
                        'relatedID': level4_related_ids
                    }
                    level3_nodes_data.append(level3_node)
                    current_id += 1

        knowledge_graph.extend(level3_nodes_data)
        print(f"创建 {len(level3_nodes_data)} 个三级知识点节点")

        # Level 4: 四级知识点（叶子节点，不需要relatedID）
        level4_nodes_data = []

        for _, level1_row in level1_nodes.iterrows():
            level1_name = str(level1_row['一级知识点'])
            level2_subset = df[df['一级知识点'] == level1_name]

            for _, level2_row in level2_subset[['二级知识点', '二级知识点编号']].drop_duplicates().iterrows():
                level2_name = str(level2_row['二级知识点'])
                if level2_name == '':
                    continue

                level3_subset = level2_subset[level2_subset['二级知识点'] == level2_name]

                for _, level3_row in level3_subset[['三级知识点', '三级知识点编号']].drop_duplicates().iterrows():
                    level3_name = str(level3_row['三级知识点'])
                    if level3_name == '':
                        continue

                    level4_subset = level3_subset[level3_subset['三级知识点'] == level3_name]
                    level4_unique = level4_subset[
                        ['四级知识点', '四级知识点编号', '有无例题', '例题标题', '例题内容', '例题说明', '例题图片id',
                         '注释内容']].drop_duplicates()
                    level4_unique = level4_unique[level4_unique['四级知识点'] != '']

                    for _, level4_row in level4_unique.iterrows():
                        # 处理数据，确保字符串类型
                        youwuliti = str(level4_row['有无例题']) if level4_row['有无例题'] != '' else '0'

                        # Level 4节点不包含relatedID字段
                        level4_node = {
                            'id': current_id,
                            'name': str(level4_row['四级知识点']),
                            'zhishidianid': str(level4_row['四级知识点编号']),
                            'level': 4,
                            'youwuliti': youwuliti,
                            'litibiaoti': str(level4_row['例题标题']) if level4_row['例题标题'] != '' else '',
                            'litineirong': str(level4_row['例题内容']) if level4_row['例题内容'] != '' else '',
                            'litishuoming': str(level4_row['例题说明']) if level4_row['例题说明'] != '' else '',
                            'lititupianid': str(level4_row['例题图片id']) if level4_row['例题图片id'] != '' else '',
                            'zhushineirong': str(level4_row['注释内容']) if level4_row['注释内容'] != '' else ''
                            # 注意：level 4节点没有relatedID字段
                        }
                        level4_nodes_data.append(level4_node)
                        current_id += 1

        knowledge_graph.extend(level4_nodes_data)
        print(f"创建 {len(level4_nodes_data)} 个四级知识点节点（叶子节点）")

        # 重新计算关联关系（因为ID分配是动态的）
        print("重新计算关联关系...")
        knowledge_graph = recalculate_related_ids(knowledge_graph, df)

        return knowledge_graph

    except Exception as e:
        print(f"处理Excel数据时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return create_sample_graph()


def recalculate_related_ids(knowledge_graph, df):
    """
    重新计算所有节点的relatedID，确保关联正确
    """
    # 按level分组
    levels = {0: [], 1: [], 2: [], 3: [], 4: []}
    for node in knowledge_graph:
        levels[node['level']].append(node)

    # 清空所有非叶子节点的relatedID
    for node in knowledge_graph:
        if node['level'] < 4:  # 只有level 0-3需要relatedID
            node['relatedID'] = []

    # 重新建立关联关系（只关联到level 3）
    for level in range(3):  # 从level 0到level 2（level 3是最后一个有子节点的层级）
        for parent_node in levels[level]:
            # 找到所有可能的子节点
            for child_node in levels[level + 1]:
                if is_nodes_related(parent_node, child_node, df):
                    parent_node['relatedID'].append(child_node['id'])

    # 处理level 3到level 4的关联
    for parent_node in levels[3]:
        for child_node in levels[4]:
            if is_nodes_related(parent_node, child_node, df):
                parent_node['relatedID'].append(child_node['id'])

    return knowledge_graph


def is_nodes_related(parent, child, df):
    """
    判断两个节点是否有父子关系
    """
    try:
        if parent['level'] == 0 and child['level'] == 1:
            # 学段关联所有一级知识点
            return True

        elif parent['level'] == 1 and child['level'] == 2:
            # 一级知识点关联二级知识点
            child_rows = df[df['二级知识点'] == child['name']]
            if len(child_rows) > 0:
                return child_rows.iloc[0]['一级知识点'] == parent['name']

        elif parent['level'] == 2 and child['level'] == 3:
            # 二级知识点关联三级知识点
            child_rows = df[df['三级知识点'] == child['name']]
            if len(child_rows) > 0:
                child_row = child_rows.iloc[0]
                return (child_row['一级知识点'] == df[df['二级知识点'] == parent['name']].iloc[0]['一级知识点'] and
                        child_row['二级知识点'] == parent['name'])

        elif parent['level'] == 3 and child['level'] == 4:
            # 三级知识点关联四级知识点
            child_rows = df[df['四级知识点'] == child['name']]
            if len(child_rows) > 0:
                child_row = child_rows.iloc[0]
                return (child_row['三级知识点'] == parent['name'])

    except Exception as e:
        return False

    return False


def create_sample_graph():
    """
    创建示例图谱（当Excel文件不存在或处理出错时使用）
    """
    print("使用示例数据创建图谱")
    sample_graph = [
        {
            "id": 0,
            "name": "初中数学",
            "xueduanID": "czmath",
            "level": 0,
            "relatedID": [1, 2]
        },
        {
            "id": 1,
            "name": "数与代数",
            "zhishidianid": "czmath01",
            "level": 1,
            "relatedID": [3, 4]
        },
        {
            "id": 2,
            "name": "图形与几何",
            "zhishidianid": "czmath02",
            "level": 1,
            "relatedID": [5]
        },
        {
            "id": 3,
            "name": "有理数",
            "zhishidianid": "czmath010101",
            "level": 2,
            "relatedID": [6, 7]
        },
        {
            "id": 4,
            "name": "实数",
            "zhishidianid": "czmath010102",
            "level": 2,
            "relatedID": [8]
        },
        {
            "id": 5,
            "name": "点、线、面、角",
            "zhishidianid": "czmath020101",
            "level": 2,
            "relatedID": [9]
        },
        {
            "id": 6,
            "name": "理解负数的意义",
            "zhishidianid": "czmath01010101",
            "level": 3,
            "relatedID": [10]
        },
        {
            "id": 7,
            "name": "理解有理数的意义",
            "zhishidianid": "czmath01010102",
            "level": 3,
            "relatedID": []  # 假设这个三级知识点没有四级子节点
        },
        {
            "id": 8,
            "name": "了解无理数和实数",
            "zhishidianid": "czmath01010201",
            "level": 3,
            "relatedID": [11]
        },
        {
            "id": 9,
            "name": "通过实物和模型了解几何体",
            "zhishidianid": "czmath02010101",
            "level": 3,
            "relatedID": [12]
        },
        {
            "id": 10,
            "name": "理解负数的意义(例64)",
            "zhishidianid": "czmath0101010101",
            "level": 4,
            "youwuliti": "1",
            "litibiaoti": "例64",
            "litineirong": "负数的引入",
            "litishuoming": "借助历史资料说明人们最初引入负数的目的...",
            "lititupianid": "0",
            "zhushineirong": "0"
            # Level 4节点没有relatedID字段
        },
        {
            "id": 11,
            "name": "了解无理数和实数概念",
            "zhishidianid": "czmath0101020101",
            "level": 4,
            "youwuliti": "0",
            "litibiaoti": "",
            "litineirong": "",
            "litishuoming": "",
            "lititupianid": "",
            "zhushineirong": ""
            # Level 4节点没有relatedID字段
        },
        {
            "id": 12,
            "name": "几何体抽象概念",
            "zhishidianid": "czmath0201010101",
            "level": 4,
            "youwuliti": "0",
            "litibiaoti": "",
            "litineirong": "",
            "litishuoming": "",
            "lititupianid": "",
            "zhushineirong": ""
            # Level 4节点没有relatedID字段
        }
    ]
    return sample_graph


def main():
    """
    主函数
    """
    try:
        print("开始生成数学知识点图谱...")

        # 生成知识图谱
        graph = create_knowledge_graph_from_excel()

        # 保存为JSON文件
        output_file = "数学知识点图谱.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(graph, f, ensure_ascii=False, indent=2)

        print(f"\n知识图谱生成完成！")
        print(f"共生成 {len(graph)} 个节点")
        print(f"结果已保存到: {output_file}")

        # 显示统计信息
        levels = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        for node in graph:
            levels[node['level']] += 1

        print("\n各层级节点数量统计:")
        for level, count in levels.items():
            print(f"Level {level}: {count} 个节点")

        # 显示示例节点
        print(f"\n节点示例:")
        for level in range(5):
            level_nodes = [node for node in graph if node['level'] == level]
            if level_nodes:
                sample_node = level_nodes[0]
                related_str = f", 关联ID: {sample_node['relatedID']}" if 'relatedID' in sample_node and sample_node[
                    'relatedID'] else ""
                print(f"Level {level}示例: ID={sample_node['id']}, 名称={sample_node['name']}{related_str}")

    except Exception as e:
        print(f"程序执行出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()