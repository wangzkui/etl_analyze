from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL
from openai import OpenAI
import yaml
import markdown
from datetime import datetime as dt
import re


class DatabaseAnalyzer:
    def __init__(self):
        with open('config.yaml') as f:
            config = yaml.safe_load(f)

        mysql_config = config['database']['mysql']
        self.db_config = {
            "drivername": "mysql+pymysql",
            "username": mysql_config['user'],
            "password": mysql_config['password'],
            "host": mysql_config['host'],
            "port": mysql_config['port'],
            "database": mysql_config['database']
        }
        self.engine = create_engine(URL.create(**self.db_config))

        self.client = OpenAI(
            base_url="https://ark.cn-beijing.volces.com/api/v3",
            api_key="2ee4018c-d640-4818-8879-94b29665b275"
        )

        self.model = "doubao-1-5-pro-256k-250115"

    def get_metadata(self):
        inspector = inspect(self.engine)
        metadata = {
            "tables": [],
            "relationships": []
        }

        with open('config.yaml') as f:
            config = yaml.safe_load(f)

        tables = config.get('tables')
        if tables is None:
            tables = inspector.get_table_names()

        for table in tables:
            columns = inspector.get_columns(table)
            pks = inspector.get_pk_constraint(table)["constrained_columns"]
            # fks = inspector.get_foreign_keys(table)
            table_meta = {
                "name": table,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": str(col["default"]) if col["default"] else None,
                        "comment": col.get("comment")
                    } for col in columns
                ],
                "primary_keys": pks,
                # "foreign_keys": [
                #     {
                #         "column": fk["constrained_columns"][0],
                #         "ref_table": fk["referred_table"]
                #     } for fk in fks
                # ]
            }
            metadata["tables"].append(table_meta)

            # for fk in fks:
            #     metadata["relationships"].append({
            #         "source_table": table,
            #         "source_column": fk["constrained_columns"][0],
            #         "target_table": fk["referred_table"],
            #         "target_column": fk["referred_columns"][0]
            #     })

        return metadata

    def get_completion(self, messages):
        full_response = ""
        while True:
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    stream=True,
                    temperature=0.7
                )

                partial_response = ""
                for chunk in response:
                    if not chunk.choices:
                        continue
                    msg = chunk.choices[0].delta.content
                    if msg:
                        print(msg, end="")
                        partial_response += msg

                full_response += partial_response

                # 判断输出是否完整
                if "#END#" in partial_response:
                    # 移除标识
                    full_response = full_response.replace("#END#", "")
                    break
                else:
                    # 请求继续输出，带上已输出内容作为上下文
                    new_message = {
                        "role": "user",
                        "content": f"输出似乎被截断了，请接着以下内容继续完成，并在结尾添加 #END#：\n{full_response}"
                    }
                    messages.append(new_message)
            except Exception as e:
                print(f"调用deepseek发生异常: {e}")
                break

        return full_response

    def generate_report(self, analysis):
        design_principles_old = analysis["design_principles"]
        design_principles = markdown.markdown(design_principles_old)
        print(design_principles + "\n" + "================" + "\n")
        fact_tables_old = analysis["fact_tables"]
        fact_tables = markdown.markdown(fact_tables_old)
        print(fact_tables + "\n" + "================" + "\n")
        dimension_tables_old = analysis["dimension_tables"]
        dimension_tables = markdown.markdown(dimension_tables_old)
        print(dimension_tables + "\n" + "================" + "\n")
        mermaid_diagram_old = analysis["mermaid_diagram"]
        mermaid_diagram_old = mermaid_diagram_old.replace("```mermaid", "")
        mermaid_diagram_old = mermaid_diagram_old.replace("```", "")
        pattern = r'DECIMAL\(\d+,\d+\)'
        mermaid_diagram = re.sub(pattern, 'NUMBER', mermaid_diagram_old)
        print(mermaid_diagram + "\n" + "================" + "\n")
        sql_statements_old = analysis["sql_statements"]
        sql_statements_old = sql_statements_old.replace("PRIMARY KEY", "")
        sql_statements = markdown.markdown(sql_statements_old)
        print(sql_statements + "\n" + "================" + "\n")

        design_desc_old = analysis["design_desc"]
        design_desc = markdown.markdown(design_desc_old)
        print(design_desc + "\n" + "================" + "\n")

        html = f"""<!DOCTYPE html>
                <html lang="zh-CN">
                <head>
                    <meta charset="UTF-8">
                    <title>数据仓库建模报告</title>
                    <style>
                        body {{ font-family: 'Microsoft YaHei', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }}
                        h1 {{ color: #2c3e50; text-align: center; }}
                        .section {{ margin: 30px 0; padding: 20px; background: #f8f9fa; border-radius: 8px; }}
                        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
                        th, td {{ padding: 12px; border: 1px solid #dee2e6; }}
                        th {{ background-color: #e9ecef; }}
                        pre {{ background: #f8f9fa; padding: 15px; border-radius: 5px; overflow-x: auto; }}
                        .mermaid {{ text-align: center; background: white; padding: 20px; border-radius: 8px; }}
                    </style>
                </head>
                <body>
                    <h1>维度建模设计报告</h1>
                    <div class="meta-info">
                        <p>生成时间：{dt.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                        <p>版本：version, 1.0.0 </p>
                    </div>

                    <div class="section">
                        <h2>📖 设计原理</h2>
                            {design_principles}
                    </div>

                    <div class="section">
                        <h2>📊 事实表设计</h2>
                        {fact_tables}
                    </div>

                    <div class="section">
                        <h2>📈 维度表设计</h2>
                        {dimension_tables}
                    </div>


                    <div class="section">
                        <h2>🔗 表关系图</h2>
                        <div class="mermaid">
                            {mermaid_diagram}
                        </div>
                    </div>

                    <div class="section">
                        <h2>💾 SQL语句</h2>
                        <pre>{sql_statements}</pre>
                    </div>

                    <div class="section">
                        <h2>📋 设计说明</h2>
                        <pre>{design_desc}</pre>
                    </div>

                    <script src="https://cdn.jsdelivr.net/npm/mermaid@10.6.1/dist/mermaid.min.js"></script>
                    <script>mermaid.initialize({{ startOnLoad: true }});</script>
                </body>
                </html>"""
        return html


if __name__ == "__main__":
    analyzer = DatabaseAnalyzer()
    print("正在获取源数据...")
    metadata = analyzer.get_metadata()

    analysis = {}
    messages = []
    elt_all_text = ""

    # 第一轮对话
    round1_prompt = f"""
请根据以下原系统表结构信息，运用Kimball方法论进行维度建模，并生成一份包含特定部分的报告，并以Markdown格式输出：
首先，请仔细阅读原系统表结构信息：
<原系统表结构信息>
{metadata}
</原系统表结构信息>

Kimball方法论维度建模，重点在于围绕业务过程构建事实表，以及相关的维度表。
 - **设计原理**：需要简单阐述设计背后的逻辑和考虑因素,在输出结尾添加 #END#。
以Markdown格式输出,输出样式：
<实体>
"""
    messages.append({"role": "user", "content": round1_prompt})
    print("==================================设计原理分析==================================\n")
    response = analyzer.get_completion(messages)
    analysis["design_principles"] = response
    elt_all_text += "### 1. 设计原理\n"
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第二轮对话
    round2_prompt = """
请根据上述原系统表结构信息，生成事实表设计。
设计要求：
 - 字段名由三类字符：小写字母、下划线、数字组成。
 - 字段名必须由小写字母开头。
 - 字段名中最多包含2个下划线字符、3个英文单词，长度限制为30个字符。
 - 字段名主要参考源系统中的字段英文描述，删除英文描述中的虚词（the、of、for、on等）并调整顺序。
 - 非特有（在多张表中均出现）字段命名时，需查询全局字段表，优先使用全模型共用的字段名。
 - 使用长度大于10个字符的单词时，需查询缩写对照表，优先使用全模型共用的缩写。
 - 单词缩写应省略在辅音之后元音之前，并省略不发音字母（优先使用常用缩写，若无则自行缩略元音字母和不发音字母）。
 - 明确事实表中应包含的具体字段，注意要包含代理键，代理键使用dim_前缀，如：dim_department_id 。
 - 表名以fact_为前缀。
 - 在输出结尾添加 #END#。
以Markdown格式输出,输出样式：
#### <表名>
- 字段名
    """
    messages.append({"role": "user", "content": round2_prompt})
    print("==================================事实表设计==================================\n")
    response = analyzer.get_completion(messages)
    analysis["fact_tables"] = response
    elt_all_text += "### 2. 详细的事实表设计\n"
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第三轮对话
    round3_prompt = """
请根据上述原系统表结构信息，生成维度表设计。
设计要求：
 - 字段名由三类字符：小写字母、下划线、数字组成。
 - 字段名必须由小写字母开头。
 - 字段名中最多包含2个下划线字符、3个英文单词，长度限制为30个字符。
 - 字段名主要参考源系统中的字段英文描述，删除英文描述中的虚词（the、of、for、on等）并调整顺序。
 - 非特有（在多张表中均出现）字段命名时，需查询全局字段表，优先使用全模型共用的字段名。
 - 使用长度大于10个字符的单词时，需查询缩写对照表，优先使用全模型共用的缩写。
 - 单词缩写应省略在辅音之后元音之前，并省略不发音字母（优先使用常用缩写，若无则自行缩略元音字母和不发音字母）。
 - 每个维度表要列出具体字段，同样需包含代理键，代理键使用dim_前缀，如：dim_department_id 。
 - 表名以dim_为前缀。
 - 在输出结尾添加 #END#。
以Markdown格式输出,输出样式：
#### <表名>
- 字段名
    """
    messages.append({"role": "user", "content": round3_prompt})
    print("==================================维度表设计==================================\n")
    response = analyzer.get_completion(messages)
    analysis["dimension_tables"] = response
    elt_all_text += "### 3. 维度表设计\n"
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第四轮对话
    round4_prompt = """
请根据上述的事实表与维度表模型，生成表关系图。并以Markdown格式输出：
设计要求：
 - 生成对应的Mermaid代码，只涉及表，不包含字段，代码语法正确，能在HTML中显示，且代码中不能有中文,去掉关系定义语句结尾的 } 符号。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round4_prompt})
    print("==================================表关系图==================================\n")
    response = analyzer.get_completion(messages)
    analysis["mermaid_diagram"] = response
    elt_all_text += "### 4. 表关系图\n"
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第五轮对话
    round5_prompt = f"""
请根据上述原系统表结构信息以及事实表和维度表结构，生成建表SQL语句。并以Markdown格式输出：
设计要求：
 - 用标准SQL编写。
 - 排除`PRIMARY KEY`定义代码，要包含代理键，代理键使用dim_前缀，如：dim_department_id
 - 排除`FOREIGN KEY`定义代码。
 - 排除 COLLATE 字符集定义
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round5_prompt})
    print("==================================建表SQL语句==================================\n")
    response = analyzer.get_completion(messages)
    analysis["sql_statements"] = response
    elt_all_text += "### 5. SQL语句\n"
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})


    # 第六轮对话：阐述分层思路
    round6_prompt = f"""
请基于上述信息，总结设计说明，并以Markdown格式输出，在输出结尾添加 #END#。：
按以下几方面阐述：
 - 代理键: <内容实体>
 - SCD处理: <内容实体>
 - 反规范化: <内容实体>
 - 计算字段: <内容实体>
 
    """
    messages.append({"role": "user", "content": round6_prompt})
    print("==================================设计说明阐述==================================\n")
    response = analyzer.get_completion(messages)
    elt_all_text += "### 6. 设计说明\n"
    analysis["design_desc"] = response
    elt_all_text += response

    with open('modeling_analyzer_new.txt', "w", encoding="utf-8") as f:
        f.write(elt_all_text)

    print("正在生成分析报告...")
    html = analyzer.generate_report(analysis)

    # 保存报告
    output_file = "dimensional_modeling_report1.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print("分析报告已生成：dimensional_modeling_report1.html")