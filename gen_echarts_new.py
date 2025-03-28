from openai import OpenAI
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.url import URL
import yaml
import datetime
import decimal




class gen_program:
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
            api_key="181f3b01-451e-43a7-9e6e-f49142ed52c8"
        )

        self.model = "doubao-1-5-pro-256k-250115"

    def get_metadata(self):
        inspector = inspect(self.engine)
        metadata = {
            "tables": [],
            "relationships": []
        }

        tables = inspector.get_table_names()

        for table in tables:
            columns = inspector.get_columns(table)
            pks = inspector.get_pk_constraint(table)["constrained_columns"]
            fks = inspector.get_foreign_keys(table)

            table_meta = {
                "name": table,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": str(col["default"]) if col["default"] else None,
                        # 获取字段的注释
                        "comment": col.get("comment")
                    } for col in columns
                ],
                "primary_keys": pks,
                "foreign_keys": [
                    {
                        "column": fk["constrained_columns"][0],
                        "ref_table": fk["referred_table"]
                    } for fk in fks
                ]
            }
            metadata["tables"].append(table_meta)

            for fk in fks:
                metadata["relationships"].append({
                    "source_table": table,
                    "source_column": fk["constrained_columns"][0],
                    "target_table": fk["referred_table"],
                    "target_column": fk["referred_columns"][0]
                })

        return metadata
    def _serialize_value(self, value):
        """处理无法JSON序列化的数据类型"""
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return value.isoformat()
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        if isinstance(value, decimal.Decimal):
            return float(value)
        return value

    def sample_data(self):
        samples = {}
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()

        for table in tables:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM `{table}` LIMIT 10"))
                samples[table] = [
                    {k: self._serialize_value(v) for k, v in dict(row._mapping).items()}
                    for row in result
                ]
        return samples

    def get_completion(self, messages):
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                stream=True,
                temperature=1,
                max_tokens=12288
            )
            full_msg = ""
            for chunk in response:
                if not chunk.choices:
                    continue
                msg = chunk.choices[0].delta.content
                print(msg, end="")
                full_msg += msg

            with open("deepseek_responses.txt", "w", encoding="utf-8") as f:
                f.write(full_msg)
            return full_msg
        except Exception as e:
            print(f"调用deepseek发生异常: {e}")
            return None

    def call_AiApi(self,metadata_table_structure,sample_data):
        prompt = f"""你的任务是根据给定的元数据表结构与样本数据生成一份报表设计说明书。这份说明书要清晰、详细地阐述报表的各项设计内容，为后续的报表开发工作提供明确指导。
首先，请仔细阅读以下元数据表结构：
<元数据表结构>
{metadata_table_structure}
</元数据表结构>
接着，查看以下样本数据：
<样本数据>
{sample_data}
</样本数据>
报表设计说明书应涵盖以下内容：
1. 报表概述：简要说明报表的用途、目标受众以及报表的主要内容。
2. 数据来源：明确报表数据的来源，即后端数据接口。
3. 图表设计：
    - 至少选择两种不同类型的Echarts图表进行设计，例如柱状图、折线图、饼图等。
    - 详细描述每种图表的用途、展示的数据维度和指标。
    - 说明图表的样式设计，如颜色、字体、图例等，以实现炫酷的视觉效果。
4. 明细信息设计：
    - 确定需要展示的明细信息内容，如客户信息、员工信息、项目信息等。
    - 设计明细信息的展示格式，如表格形式，并说明表格的列名和数据类型。
5. 数据过滤组件设计：
    - 描述数据过滤组件的功能和使用场景。
    - 确定过滤的维度和条件，例如按部门、项目、时间等进行过滤。
    - 说明过滤组件的交互方式，如下拉框、复选框等。
6. 页面布局设计：
    - 规划报表页面的整体布局，包括图表、明细信息和数据过滤组件的位置和大小。
    - 考虑页面的可读性和美观性，合理安排各个元素的间距和比例。
7. 交互设计：
    - 定义报表的交互功能，如鼠标悬停提示、点击查看详情等。
    - 说明交互操作的触发条件和响应效果。
请在<report_design_specification>标签内写下你的报表设计说明书。确保说明书内容完整、逻辑清晰，能够为报表开发提供明确的指导。

           """
        print(prompt)

        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        return messages

    def call_AiApi_code1(self, bi_report, metadata_table_structure):
        prompt = f"""你的任务是根据提供的报表设计说明书和数据库表结构来生成后端代码。后端代码用于从数据库获取数据，同时要处理好数据更新和异常情况。
    首先，请仔细阅读以下报表设计说明书：
    <报表设计说明书>
    {bi_report}
    </报表设计说明书>
    接着，请仔细查看以下数据库表结构：
    <数据库表结构>
    {metadata_table_structure}
    </数据库表结构>
    在生成代码时，请遵循以下要求和注意事项：
    1. 代码要具备良好的可读性和可维护性，添加必要的注释。
    2. 后端代码里面的SQL要保证能正确执行。
    3. 对于代码中使用的数据库表结构和字段，要与说明书和数据库表结构中提到的维度表和事实表对应。、
    4. 数据库为mysql, 请使用mysql的sql语法，数据信息：host: localhost，port: 3306，user: root，password: "123456"，database: amway
    5. 后端代码要保证前端可以访问到后端数据，并且能正确展示报表。

    请在<后端代码>标签中输出后端代码。
    <后端代码>
    [在此输出后端代码]
    </后端代码>

               """
        print(prompt)

        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        return messages

    def call_AiApi_code2(self, bi_report, metadata_table_structure, code):
        prompt = f"""你的任务是基于提供的报表设计说明书、数据库表结构以及后端代码，生成用于实现报表系统的前端代码。该报表系统要能对后端数据接口数据进行综合呈现，具备明细信息展示、多种图表可视化、数据过滤等功能。
请仔细阅读以下报表设计说明书：
<报表设计说明书>
{bi_report}
</报表设计说明书>
接下来，请查看以下数据库表结构：
<数据库表结构>
{metadata_table_structure}
</数据库表结构>
最后，请阅读以下后端代码：
<后端代码>
{code}
</后端代码>
在生成代码时，请严格遵循以下要求和注意事项：
1. 代码要具备良好的可读性和可维护性，添加必要的注释。注释应清晰说明代码的功能和逻辑，便于后续维护和理解。
2. 要确保代码实现说明书中提到的各项功能，如明细信息展示、柱状图和折线图绘制、数据过滤等。详细检查说明书中的功能需求，确保每个功能都有对应的代码实现。
3. 代码要考虑数据更新频率和异常处理的情况，按照说明书的要求进行处理。例如，根据说明书规定的数据更新时间间隔，编写相应的代码实现数据的定时更新；对可能出现的网络请求失败、数据格式错误等异常情况进行捕获和处理。
4. 对于代码中使用的数据库表结构和字段，要与说明书中提到的维度表和事实表对应。仔细核对数据库表结构和说明书中的表结构信息，确保代码中使用的表名和字段名准确无误。
5. 可以使用合适的前端框架和库来实现图表和交互功能，如Echarts等。根据功能需求选择合适的前端框架和库，并确保正确引入和使用。

请在<前端代码>标签中输出前端代码。
<前端代码>
[在此输出前端代码]
</前端代码>

               """
        print(prompt)

        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        return messages


if __name__ == "__main__":
    gen_program = gen_program()
    print("正在获取数据库元数据...")
    metadata =  gen_program.get_metadata()
    print("正在获取样本数据...")
    sample_data = gen_program.sample_data()
    message_report = gen_program.call_AiApi(metadata, sample_data)
    bi_report = gen_program.get_completion(message_report)

    messages1 = gen_program.call_AiApi_code1(bi_report, metadata)
    print("正在生成后端代码...")
    response = gen_program.get_completion(messages1)

    messages2 = gen_program.call_AiApi_code2(bi_report, metadata, response)
    print("正在生成前端代码...")
    response = gen_program.get_completion(messages2)

