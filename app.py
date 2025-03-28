import pymysql
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# 数据库连接配置
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'amway'
}


# 获取订单各月总金额数据
@app.route('/get_monthly_order_amount', methods=['GET'])
def get_monthly_order_amount():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    order_type = request.args.get('order_type')
    customer_source = request.args.get('customer_source')

    conn = pymysql.connect(**config)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    base_sql = """
        SELECT 
            DATE_FORMAT(o.order_date, '%Y-%m') AS month, 
            SUM(o.order_value) AS total_amount 
        FROM 
            cdm_f_oms_order o
        LEFT JOIN 
            dim_d_order_type ot ON o.dim_order_type_id = ot.dim_order_type_id
        WHERE 1 = 1
    """
    conditions = []
    if start_date and end_date:
        conditions.append("o.order_date BETWEEN %s AND %s")
    if order_type:
        conditions.append("ot.order_type_code = %s")
    if customer_source:
        conditions.append("o.customer_source = %s")

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    base_sql += " GROUP BY month ORDER BY month"

    try:
        if start_date and end_date:
            cursor.execute(base_sql, (start_date, end_date, order_type, customer_source))
        elif order_type:
            cursor.execute(base_sql, (order_type, customer_source))
        elif customer_source:
            cursor.execute(base_sql, (customer_source,))
        else:
            cursor.execute(base_sql)

        data = cursor.fetchall()
        return jsonify(data)
    except Exception as e:
        print(f"获取订单各月总金额数据时出错: {e}")
        return jsonify({"error": "获取数据失败"}), 500
    finally:
        cursor.close()
        conn.close()


# 获取不同订单类型的订单数量占比数据
@app.route('/get_order_type_ratio', methods=['GET'])
def get_order_type_ratio():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    customer_source = request.args.get('customer_source')

    conn = pymysql.connect(**config)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    base_sql = """
        SELECT 
            ot.order_type_name, 
            COUNT(o.order_num) AS order_count,
            (COUNT(o.order_num) / total.total_orders) * 100 AS ratio
        FROM 
            cdm_f_oms_order o
        LEFT JOIN 
            dim_d_order_type ot ON o.dim_order_type_id = ot.dim_order_type_id,
            (SELECT COUNT(*) AS total_orders FROM cdm_f_oms_order) AS total
        WHERE 1 = 1
    """
    conditions = []
    if start_date and end_date:
        conditions.append("o.order_date BETWEEN %s AND %s")
    if customer_source:
        conditions.append("o.customer_source = %s")

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    base_sql += " GROUP BY ot.order_type_name, total.total_orders"

    try:
        if start_date and end_date:
            cursor.execute(base_sql, (start_date, end_date, customer_source))
        elif customer_source:
            cursor.execute(base_sql, (customer_source,))
        else:
            cursor.execute(base_sql)

        data = cursor.fetchall()
        return jsonify(data)
    except Exception as e:
        print(f"获取不同订单类型的订单数量占比数据时出错: {e}")
        return jsonify({"error": "获取数据失败"}), 500
    finally:
        cursor.close()
        conn.close()


# 获取订单明细信息
@app.route('/get_order_detail', methods=['GET'])
def get_order_detail():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    order_type = request.args.get('order_type')
    customer_source = request.args.get('customer_source')

    conn = pymysql.connect(**config)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    base_sql = """
        SELECT 
            o.order_num, 
            o.order_date, 
            o.item_sku, 
            o.item_qnty, 
            o.item_price, 
            o.order_value, 
            o.customer_source
        FROM 
            cdm_f_oms_order o
        LEFT JOIN 
            dim_d_order_type ot ON o.dim_order_type_id = ot.dim_order_type_id
        WHERE 1 = 1
    """
    conditions = []
    if start_date and end_date:
        conditions.append("o.order_date BETWEEN %s AND %s")
    if order_type:
        conditions.append("ot.order_type_code = %s")
    if customer_source:
        conditions.append("o.customer_source = %s")

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    try:
        if start_date and end_date:
            cursor.execute(base_sql, (start_date, end_date, order_type, customer_source))
        elif order_type:
            cursor.execute(base_sql, (order_type, customer_source))
        elif customer_source:
            cursor.execute(base_sql, (customer_source,))
        else:
            cursor.execute(base_sql)

        data = cursor.fetchall()
        return jsonify(data)
    except Exception as e:
        print(f"获取订单明细信息时出错: {e}")
        return jsonify({"error": "获取数据失败"}), 500
    finally:
        cursor.close()
        conn.close()

# 新增路由用于渲染 index.html
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)