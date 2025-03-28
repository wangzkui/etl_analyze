from decimal import Decimal
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.url import URL
import datetime
import markdown
import re
from openai import OpenAI

class DatabaseAnalyzer:
    def __init__(self):
        import yaml
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
        # self.api_key = os.getenv("DEEPSEEK_API_KEY")
        # self.api_key = "sk-hsmgtgwskguvrxkntsypherhswqibgdlqjfckhrtpilovomo"
        self.client = OpenAI(
            base_url="https://ark.cn-beijing.volces.com/api/v3",
            api_key="181f3b01-451e-43a7-9e6e-f49142ed52c8"
        )
        self.model = "doubao-1-5-pro-256k-250115"
    def get_metadata(self):
        inspector = inspect(self.engine)
        metadata = {
            "tables": []
            # ,
            # "relationships": []
        }

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
                        # 获取字段的注释
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

    def _serialize_value(self, value):
        """处理无法JSON序列化的数据类型"""
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return value.isoformat()
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        if isinstance(value, Decimal):
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

    def analyze_with_deepseek(self,metadata,samples):
        prompt = f"""
你的任务是根据数据库元数据和样本数据完成一系列分析，并以markdown格式输出。
首先，这是原系统表结构信息：
<原系统表结构信息>
{metadata}
</原系统表结构信息>
接着，这是样例数据：
<样例数据>
{samples}
</样例数据>
以下是各项分析的具体要求：
1. **推断表之间的业务关系**：仔细研究原系统表结构信息和样例数据，从业务角度出发，推断不同表之间的联系，使用中文表述。在markdown格式中，以 “### 1. 推断表之间的业务关系” 为标题，每项关系以 “#### <内容实体>” 形式呈现。
2. **分析每个表的核心业务含义**：依据所给信息，解读每个表在整体业务中的核心意义。输出时以 “### 2. 分析每个表的核心业务含义” 为标题，每个表以 “#### <表名>:<核心业务含义>” 格式展示。
3. **标注关键字段的业务意义**：确定每个表中的关键字段，并阐述其业务层面的意义。以 “### 3. 标注关键字段的业务意义” 为标题，每个表以 “#### <表名> - <关键字段名>: <业务意义>” 形式输出。
4. **识别包含个人信息的敏感字段**：从原系统表结构信息和样例数据中找出包含个人信息的敏感字段，并说明敏感信息的类型。以 “### 4. 识别包含个人信息的敏感字段” 为标题，每个表以 “#### <表名> - <敏感字段>(敏感信息说明)” 格式呈现。
5. **补充建议**：审视现有元数据，思考元数据存在哪些遗漏需要补充，哪些信息还不明确，并给出其他合理建议。以 “### 5. 补充建议” 为标题，以 “#### <建议内容，可扩展>” 形式输出。
6. **绘制一个表与表的实体关系图**：生成对应的Mermaid代码，只涉及表，不包含字段，代码语法正确，能在HTML中显示，且代码中不能有中文, 去掉关系定义语句结尾的 }} 符号。以 “### 6. 生成可用于Mermaid图表的表关系描述” 为标题，在下方给出Mermaid格式代码。

请务必严格按照上述markdown格式要求，丰富且全面地完成各项分析并输出结果。

        """
        print(prompt)

        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                stream=True,
                temperature=0.7
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



    def generate_report(self, analysis):
        """生成带章节结构的分析报告"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mermaid_code = self._extract_mermaid_code(analysis)
        print(analysis)
        patterns = {
            "design_principles": r"### 1\. 推断表之间的业务关系\s+([\s\S]*?)(?=### 6\. 生成可用于Mermaid图表的表关系描述)",
            "mermaid_diagram": r"### 6\. 生成可用于Mermaid图表的表关系描述\s+([\s\S]*)"
        }

        rep_txt = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, analysis, re.DOTALL)
            if match:
                rep_txt[key] = match.group(1).strip()
            else:
                print(f"未匹配到 {key}，模式为: {pattern}")
                rep_txt[key] = ""
        design_principles_old = rep_txt["design_principles"]
        # 启用额外的扩展
        extensions = ['markdown.extensions.tables', 'markdown.extensions.fenced_code']
        design_principles = markdown.markdown(design_principles_old, extensions=extensions)

        # # # 读取 HTML 模板文件
        # with open('model_report_template.html', 'r', encoding='utf-8') as file:
        #      html_template = file.read()

        html_template = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>源系统分析报告 - {timestamp}</title>
                    <style>
                        body {{ 
                            font-family: 'Segoe UI', Arial, sans-serif; 
                            margin: 2rem;
                            background-color: #f5f6fa;
                            overflow-x: hidden;
                        }}
                        .container {{
                            max-width: 100%;
                            margin: 0 auto;
                            padding: 0 20px;
                        }}
                        .header {{
                            text-align: center;
                            padding: 2rem;
                            background: #2c3e50;
                            color: white;
                            border-radius: 10px;
                            margin-bottom: 2rem;
                        }}
                        .diagram-section {{
                            background: white;
                            padding: 2rem;
                            border-radius: 10px;
                            box-shadow: 0 2px 15px rgba(0,0,0,0.1);
                            margin-bottom: 2rem;
                        }}
                        .mermaid-wrapper {{
                            min-height: 500px;
                            background: #ffffff;
                            border-radius: 8px;
                            padding: 2rem;
                            margin: 0 auto;
                            max-width: 100%;
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            flex-direction: column;
                            box-shadow: 0 2px 15px rgba(0,0,0,0.1);
                            overflow: auto;
                            box-sizing: border-box;
                            width: 100%;
                            min-width: 100%;
                            flex-shrink: 0;
                        }}
                        .mermaid-wrapper svg {{
                            position: relative;
                            max-width: 100% !important;
                        }}
                        .analysis-details {{
                            display: grid;
                            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                            gap: 1.5rem;
                            margin-top: 2rem;
                        }}
                        .detail-card {{
                            background: white;
                            padding: 1.5rem;
                            border-radius: 8px;
                            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
                        }}
                        pre {{
                            white-space: pre-wrap;
                            background: #f8f9fa;
                            padding: 1rem;
                            border-radius: 6px;
                            overflow-x: auto;
                            max-width: 100%;
                        }}
                    </style>
                    <script src="https://cdn.jsdelivr.net/npm/mermaid@10.6.1/dist/mermaid.min.js"></script>
                </head>
                <body>
                    <div class="container">
                        <div class="header">
                            <h1>源系统分析报告</h1>
                            <p>生成时间：{timestamp}</p>
                        </div>

                        <div class="diagram-section">
                            <h2>🔗 表关系结构图</h2>
                            <div class="mermaid-wrapper">
                                <div class="mermaid">
                                    {mermaid_code or '未检测到关系图代码'}
                                </div>
                            </div>
                        </div>

                        <div class="analysis-details">
                            <div class="detail-card">
                                <h3>📋 完整分析结果</h3>
                                <pre>
                                <h3>1. 推断表之间的业务关系</h3>
                                {design_principles}
                                </pre>
                            </div>
                            <div class="detail-card">
                                <h3>📊 Mermaid 源代码</h3>
                                <pre>{mermaid_code or '无可用代码'}</pre>
                            </div>
                        </div>
                    </div>

                    <script>
                        // 初始化Mermaid配置
                        const mermaidConfig = {{
                            startOnLoad: true,
                            theme: 'forest',
                            flowchart: {{ curve: 'basis' }},
                            er: {{
                                layoutDirection: 'LR',
                                useMaxWidth: true,
                                diagramPadding: 20
                            }},
                            zoom: {{
                                enabled: true,
                                scale: 1
                            }},
                            pan: {{
                                enabled: true
                            }},
                            securityLevel: 'loose'
                        }};

                        mermaid.initialize(mermaidConfig);

                        // 自动缩放图表
                        const initAutoZoom = () => {{
                            const observer = new MutationObserver((mutations) => {{
                                const svg = document.querySelector('.mermaid svg');
                                if (svg) {{
                                    const container = document.querySelector('.mermaid-wrapper');
                                    const {{ 
                                        width: svgWidth,
                                        height: svgHeight 
                                    }} = svg.getBBox();

                                    // 计算最佳缩放比例（保留10%边距）
                                    // 动态计算容器内边距
                                    const containerStyle = getComputedStyle(container);
                                    const horizontalPadding = parseFloat(containerStyle.paddingLeft) + parseFloat(containerStyle.paddingRight);
                                    const verticalPadding = parseFloat(containerStyle.paddingTop) + parseFloat(containerStyle.paddingBottom);

                                    // 计算可用空间时考虑padding
                                    const scale = Math.min(
                                        (container.clientWidth - horizontalPadding) / svgWidth,
                                        (container.clientHeight - verticalPadding) / svgHeight
                                    );

                                    // 应用缩放并居中，添加最小缩放限制
                                    const finalScale = Math.max(scale, 0.5);
                                    // 调整transform平移值并添加边界限制
                                    svg.style.transform = `scale(${{finalScale}}) translate(-50%, -50%)`;
                                    svg.style.width = '100%';
                                    svg.style.transformOrigin = 'center center';
                                    svg.style.left = '50%';
                                    svg.style.top = '50%';
                                    svg.style.position = 'absolute';

                                    // 确保SVG不会超出容器
                                    svg.style.overflow = 'visible';
                                    svg.style.maxHeight = `calc(100% - ${{verticalPadding}}px)`;
                                    container.style.overflow = 'auto';

                                    observer.disconnect();
                                }}
                            }});

                            observer.observe(document.body, {{
                                childList: true,
                                subtree: true
                            }});
                        }};

                        // 初始化缩放和拖拽
                        window.addEventListener('load', () => {{
                            initAutoZoom();

                            // 支持鼠标滚轮缩放
                            document.querySelector('.mermaid').addEventListener('wheel', (e) => {{
                                e.preventDefault();
                                const svg = e.currentTarget.querySelector('svg');
                                const scale = parseFloat(svg.style.transform.match(/scale\(([\d.]+)\)/)[1]) || 1;
                                const newScale = e.deltaY > 0 ? scale * 0.9 : scale * 1.1;
                                svg.style.transform = `scale(${{newScale}}) translate(50%, 50%)`;
                                svg.style.transformOrigin = '0 0';
                            }});
                        }});

                        // 窗口大小变化时重新缩放
                        window.addEventListener('resize', () => {{
                            initAutoZoom();
                            mermaid.init();
                        }});
                    </script>
                </body>
                </html>
                """

        return html_template

    def _extract_mermaid_code(self, analysis):
        # 从分析结果中提取Mermaid代码并验证语法
        start = analysis.find("```mermaid") + 10
        end = analysis.find("```", start)
        code = analysis[start:end].strip() if start != -1 and end != -1 else ""

        # 基本语法校验
        if code and not code.startswith("erDiagram"):
            # 尝试自动修复旧版语法
            if "graph TD" in code or "graph LR" in code:
                code = code.replace("graph TD", "erDiagram").replace("graph LR", "erDiagram")
            else:
                code = f"erDiagram\n{code}"

        # 移除可能存在的HTML标签
        code = code.replace("<", "<").replace(">", ">")

        return code


if __name__ == "__main__":
    analyzer = DatabaseAnalyzer()

    try:
        print("正在获取数据库元数据...")
        metadata = analyzer.get_metadata()

        print("正在抽取样本数据...")
        samples = analyzer.sample_data()

        print("正在调用DeepSeek API进行分析...")
        analysis_result = analyzer.analyze_with_deepseek(metadata, samples)

        print("正在生成分析报告...")
        report = analyzer.generate_report(analysis_result)

        with open("report1.html", "w", encoding="utf-8") as f:
            f.write(report)

        print("分析报告已生成：report1.html")

    except Exception as e:
        print(f"程序执行出错：{str(e)}")