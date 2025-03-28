from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.url import URL
from openai import OpenAI
import yaml
import markdown
from datetime import datetime as dt


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
                        "default": str(col["default"]) if col["default"] else None
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
                    temperature=1.0
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
        ods_ddl_old = analysis["ods_ddl"]
        ods_ddl = markdown.markdown(ods_ddl_old)
        print(ods_ddl + "\n" + "================" + "\n")

        cls_ddl_old = analysis["cls_ddl"]
        cls_ddl = markdown.markdown(cls_ddl_old)
        print(cls_ddl + "\n" + "================" + "\n")

        cls_etl_old = analysis["cls_etl"]
        cls_etl = markdown.markdown(cls_etl_old)
        print(cls_etl + "\n" + "================" + "\n")

        dw_ddl_old = analysis["dw_ddl"]
        dw_ddl = markdown.markdown(dw_ddl_old)
        print(dw_ddl + "\n" + "================" + "\n")

        dw_etl_old = analysis["dw_etl"]
        dw_etl = markdown.markdown(dw_etl_old)
        print(dw_etl + "\n" + "================" + "\n")

        dim_ddl_old = analysis["dim_ddl"]
        dim_ddl = markdown.markdown(dim_ddl_old)
        print(dim_ddl + "\n" + "================" + "\n")

        dim_etl_old = analysis["dim_etl"]
        dim_etl = markdown.markdown(dim_etl_old)
        print(dim_etl + "\n" + "================" + "\n")

        desc_old = analysis["desc"]
        desc = markdown.markdown(desc_old)
        print(desc + "\n" + "================" + "\n")

        html = f"""<!DOCTYPE html>
                    <html lang="zh-CN">
                    <head>
                        <meta charset="UTF-8">
                        <title>ETL设计报告</title>
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
                        <h1>ETL设计报告</h1>
                        <div class="meta-info">
                            <p>生成时间：{dt.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                            <p>版本：version, 1.0.0 </p>
                        </div>
                        <div class="section">
                            <h2>📋 分层思路</h2>
                            {desc}
                        </div>

                        <div class="section">
                            <h2>📖 ODS层建表语句</h2>
                            <pre>
                                <code>
                                {ods_ddl}
                                </code>
                            </pre>    
                        </div>

                        <div class="section">
                            <h2>📖 CLS层建表语句</h2>
                            <pre>
                                <code>
                                {cls_ddl}
                                </code>
                            </pre> 
                        </div>

                        <div class="section">
                            <h2>📖 CLS层ETL脚本</h2>
                            <pre>
                                <code>
                                {cls_etl}
                                </code>
                            </pre> 
                        </div>

                        <div class="section">
                            <h2>📖 DW层建表语句</h2>
                            <pre>
                                <code>
                                {dw_ddl}
                                </code>
                            </pre> 
                        </div>

                        <div class="section">
                            <h2>📖 DW层ETL脚本</h2>
                            <pre>
                                <code>
                                 {dw_etl}
                                </code>
                            </pre> 
                        </div>

                        <div class="section">
                            <h2>📖 DIM层建表语句</h2>
                            <pre>
                                <code>
                                 {dim_ddl}
                                </code>
                            </pre>
                        </div>

                         <div class="section">
                            <h2>📖 DIM层ETL脚本</h2>
                            <pre>
                                <code>
                                 {dim_etl}
                                </code>
                            </pre>

                        </div>

                    </body>
                    </html>"""
        return html


if __name__ == "__main__":
    analyzer = DatabaseAnalyzer()
    print("正在获取源数据...")
    metadata = analyzer.get_metadata()

    # 读取输入文件
    input_file = "modeling_analyzer_new.txt"
    with open(input_file, "r", encoding="utf-8") as f:
        business_requirements = f.read()

    analysis = {}
    messages = []
    elt_all_text = ""

    # 第一轮对话：生成ODS层建表语句
    round1_prompt = f"""
请根据以下原系统表结构信息，为贴源层ODS设计建表语句，并以Markdown格式输出：

原系统表结构信息如下：
<原系统表结构信息>
{metadata}
</原系统表结构信息>

设计要求：
 - 所有表的结构设计中不使用PRIMARY KEY与FOREIGN KEY进行标识。
 - 建表语句只需保留字段名与字段类型，不需要字符集校对规则的指定。
 - 数据与数据源1:1一致，无转换，保留当前数据处理批次数据。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 以ods_开头表示ODS层表名。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round1_prompt})
    print("正在调用DeepSeek API进行ODS层建表语句分析...")
    response = analyzer.get_completion(messages)
    analysis["ods_ddl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第二轮对话：生成CLS层建表语句
    round2_prompt = f"""
请根据上述原系统表结构信息，生成CLS层建表语句。并以Markdown格式输出：
设计要求：
 - 所有表的结构设计中不使用PRIMARY KEY与FOREIGN KEY进行标识。
 - 建表语句只需保留字段名与字段类型，不需要字符集校对规则的指定。
 - 装载ODS层数据，按业务属性过滤字段。
 - 以cls_开头表示CLS层表名。
 - 对ODS层字段改名，进行格式化、标准化、解密操作（在此仅需体现在建表字段命名上，实际操作在ETL脚本中）。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round2_prompt})
    print("正在调用DeepSeek API进行CLS层建表语句分析...")
    response = analyzer.get_completion(messages)
    analysis["cls_ddl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第三轮对话：生成CLS层ETL脚本
    round3_prompt = f"""
请根据上述原系统表结构信息以及ODS层、CLS层建表结构，生成CLS层ETL脚本。并以Markdown格式输出：
设计要求：
 - 用标准SQL编写。
 - 需要对CLS历史数据进行删除后再插入。
 - 进行一级数据质量校验，过滤空值、脏数据和重复数据（基于业务主键）。
 - 对字符类型字段添加trim处理。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round3_prompt})
    print("正在调用DeepSeek API进行CLS层ETL脚本分析...")
    response = analyzer.get_completion(messages)
    analysis["cls_etl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第四轮对话：生成DW层建表语句
    round4_prompt = f"""
请根据上述原系统表结构信息,维度建模报告以及CLS层建表结构，生成DW层建表语句。并以Markdown格式输出：
维度建模报告如下：
<建模报告>
{business_requirements}
</建模报告>
设计要求：
 - 表名以dwh开头，示例：dwh_xxx_xx_xx。
 - 所有表的结构设计中不使用PRIMARY KEY与FOREIGN KEY进行标识。
 - 建表语句只需保留字段名与字段类型，不需要字符集校对规则的指定。
 - 着重处理历史数据，通过业务主键判断，新增数据插入，已存在数据更新（体现在建表字段上，实际操作在ETL脚本中）。
 - 进行二级数据质量业务校验（体现在建表字段上，实际操作在ETL脚本中），通过多表合并实现数据组装、过滤与校验。
 - 从关联维度获取代理键填充事实表（体现在建表字段上，实际操作在ETL脚本中）。
 - 不包含维表。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime和update_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round4_prompt})
    print("正在调用DeepSeek API进行DW层建表语句分析...")
    response = analyzer.get_completion(messages)
    analysis["dw_ddl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第五轮对话：生成DW层ETL脚本
    round5_prompt = f"""
请根据上述原系统表结构信息，维度建模报告以及CLS层、DW层建表结构，生成DW层ETL脚本。并以Markdown格式输出：
设计要求：
 - 用标准SQL编写。
 - 着重处理历史数据，通过业务主键判断，新增数据插入，已存在数据更新。
 - 进行二级数据质量业务校验，通过多表合并实现数据组装、过滤与校验。
 - 从关联维度获取代理键填充事实表。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime和update_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round5_prompt})
    print("正在调用DeepSeek API进行DW层ETL脚本分析...")
    response = analyzer.get_completion(messages)
    analysis["dw_etl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第六轮对话：生成DIM层建表语句
    round6_prompt = f"""
请根据上述原系统表结构信息，维度建模报告以及CLS层建表结构，生成DIM层建表语句。并以Markdown格式输出：
设计要求：
 - 所有表的结构设计中不使用PRIMARY KEY与FOREIGN KEY进行标识。
 - 建表语句只需保留字段名与字段类型，不需要字符集校对规则的指定。
 - 基于CLS层构建维度建模层，存放维表。
 - 严格参照维度建模设计报告中“维度表设计”模块对SCD处理的要求（对于可能变化的维度，如dim_employee和dim_department，采用SCD Type 2跟踪历史记录）。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round6_prompt})
    print("正在调用DeepSeek API进行DIM层建表语句分析...")
    response = analyzer.get_completion(messages)
    analysis["dim_ddl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第七轮对话：生成DIM层ETL脚本
    round7_prompt = f"""
请根据上述原系统表结构信息，维度建模报告以及CLS层、DIM层建表结构，生成DIM层ETL脚本。并以Markdown格式输出：
设计要求：
 - 用标准SQL编写。
 - 基于CLS层构建维度建模层，存放维表。
 - 严格参照维度建模设计报告中“维度表设计” 
 - 模块对SCD处理的要求（对于可能变化的维度，如dim_employee和dim_department，采用SCD Type 2跟踪历史记录）。
 - 添加TIMESTAMP类型的时间戳字段insert_datetime。
 - 只需生成代码，不要生成任务无关的内容，包括解释。
 - 在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round7_prompt})
    print("正在调用DeepSeek API进行DIM层ETL脚本分析...")
    response = analyzer.get_completion(messages)
    analysis["dim_etl"] = response
    elt_all_text += response
    messages.append({"role": "assistant", "content": response})

    # 第八轮对话：阐述分层思路
    round8_prompt = f"""
请基于上述原系统表结构信息以及各层的设计，总结分层思路，并以Markdown格式输出：
### 分层思路
<思路阐述实体>
请在输出结尾添加 #END#。
    """
    messages.append({"role": "user", "content": round8_prompt})
    print("正在调用DeepSeek API进行分层思路阐述...")
    response = analyzer.get_completion(messages)
    analysis["desc"] = response
    elt_all_text += response

    with open("etl_analyze.txt", "w", encoding="utf-8") as f:
        f.write(elt_all_text)

    print("正在生成分析报告...")
    html = analyzer.generate_report(analysis)

    # 保存报告
    output_file = "etl_report.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print("分析报告已生成：etl_report.html")