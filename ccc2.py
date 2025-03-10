import openai
from flask import Flask, request, jsonify,render_template
import snowflake.connector
from pandasai import SmartDataframe
from pandasai.llm import BambooLLM
import pandas as pd
import matplotlib.pyplot as plt
import io
from langchain_community.chat_models import ChatOpenAI
import base64
import os
import shutil
from dotenv import load_dotenv 

load_dotenv()

llm = ChatOpenAI(model_name="gpt-4", openai_api_key=os.getenv("OPEN_API_KEY"))
app = Flask(__name__)
app.secret_key = "summa"
results = []
columns = []
rowt=0
i=0
# Set your OpenAI API Key
openai.api_key = os.getenv("OPEN_API_KEY")
# Define the system message to guide the model
SYSTEM_MESSAGE = """
INSTRUCTIONS: Analyze the database schema and generate the SQL query accordingly. If asked any non-related question, do not generate an SQL query and respond accordingly.
- ONLY RETURN THE RAW SQL QUERY. DO NOT INCLUDE ANY EXPLANATIONS, MARKDOWN BACKTICKS, OR ADDITIONAL TEXT.
- Ensure the query ends with a semicolon (;).
- Optimize performance by minimizing unnecessary operations.
- Use IN, ON, BY operators as per applicable conditions and subqueries.
- If a query involves multiple steps or conditions, consider breaking it into subqueries or using CTEs.
- SCHEMA-RESTRICTED QUERIES: Only use tables and columns that exist in the schema.
- Return an error if a requested table or column is not found.
- MONTH HANDLING: Use numerical values for months instead of LIKE %pattern%.
- FOR STRING HANDLING: Convert strings to lowercase and use LIKE %pattern% for matching.
- JOIN REQUESTS: Interpret "along" as a request to join tables using valid schema relationships.
- VALIDATION: If a table or column does not exist, diagnose the error and then give me possible issue and return issue do not use the word "Error"
- QUERY GENERATION CONDITIONS: Use JOIN, alias columns, apply GROUP BY, ORDER BY, and filters. Use COUNT, AVG, MAX, MIN, and format dates as YYYY-MM-DD.
- SQL INJECTION: IF user for any sql injection queries return "Permission denied"


Database Schema:
Database name:MEC.BIKE_SALE
brands(Brand_id NUMBER PRIMARY KEY, Brand_name VARCHAR NOT NULL)  
categories(Category_id NUMBER PRIMARY KEY, Category_name VARCHAR NOT NULL)  
customers(Customer_id NUMBER PRIMARY KEY, First_name VARCHAR NOT NULL, Last_name VARCHAR NOT NULL, Phone VARCHAR, Email VARCHAR, Street VARCHAR, City VARCHAR, State VARCHAR, Zip_code NUMBER)  
stores(Store_id NUMBER PRIMARY KEY, Store_name VARCHAR NOT NULL, Phone VARCHAR, Email VARCHAR, Street VARCHAR, City VARCHAR, State VARCHAR, Zip_code NUMBER)  
staffs(Staff_id NUMBER PRIMARY KEY, First_name VARCHAR NOT NULL, Last_name VARCHAR NOT NULL, Email VARCHAR, Phone VARCHAR, Active NUMBER, Store_id NUMBER, Manager_id NUMBER, FOREIGN KEY (Store_id) REFERENCES stores(Store_id), FOREIGN KEY (Manager_id) REFERENCES staffs(Staff_id))  
products(Product_id NUMBER PRIMARY KEY, Product_name VARCHAR NOT NULL, Brand_id NUMBER, Category_id NUMBER, Model_year NUMBER, List_price NUMBER, FOREIGN KEY (Brand_id) REFERENCES brands(Brand_id), FOREIGN KEY (Category_id) REFERENCES categories(Category_id))  
stocks(Store_id NUMBER, Product_id NUMBER, Quantity NUMBER NOT NULL, PRIMARY KEY (Store_id, Product_id), FOREIGN KEY (Store_id) REFERENCES stores(Store_id), FOREIGN KEY (Product_id) REFERENCES products(Product_id))  
orders(Order_id NUMBER PRIMARY KEY, Customer_id NUMBER, Order_status NUMBER, Order_date DATE NOT NULL, Required_date DATE, Shipped_date DATE, Store_id NUMBER, Staff_id NUMBER, FOREIGN KEY (Customer_id) REFERENCES customers(Customer_id), FOREIGN KEY (Store_id) REFERENCES stores(Store_id), FOREIGN KEY (Staff_id) REFERENCES staffs(Staff_id))  
order_items(Order_id NUMBER, Item_id NUMBER, Product_id NUMBER, Quantity NUMBER NOT NULL, List_price NUMBER, Discount NUMBER, PRIMARY KEY (Order_id, Item_id), FOREIGN KEY (Order_id) REFERENCES orders(Order_id), FOREIGN KEY (Product_id) REFERENCES products(Product_id))  


Conversation history:
User: Show me the active staff members.
AI: SELECT staff_id, first_name, last_name, email, phone, active, store_id, manager_id FROM staffs WHERE active = 1;

Last line:
User: Which store has the minimum number of stocks left for the Sun Bicycle brand ? 
Output:
SELECT st.store_name,SUM(s.quantity) AS total_stock FROM stocks s JOIN products p ON s.product_id = p.product_id JOIN brands b ON p.brand_id = b.brand_id
JOIN 
    stores st ON s.store_id = st.store_id
WHERE 
    LOWER(b.brand_name) LIKE '%sun%bicycle%'
GROUP BY 
    st.store_name
ORDER BY 
    total_stock ASC
LIMIT 1;


Conversation history:
User: Show me the brands table.
AI: SELECT Brand_id, Brand_name FROM brands;

Last line:
User: Identify employees who have processed the most orders in the last 12 months.
Output:
SELECT 
    s.first_name, 
    s.last_name, 
    COUNT(o.order_id) AS total_orders 
FROM 
    staffs s 
JOIN 
    orders o ON s.staff_id = o.staff_id 
WHERE 
    o.order_date > (CURRENT_DATE - INTERVAL '1 YEAR') 
GROUP BY 
    s.staff_id, s.first_name, s.last_name 
ORDER BY 
    total_orders DESC 
LIMIT 1;

Conversation history:
User: Show me the active staff members.
AI: SELECT staff_id, first_name, last_name, email, phone, active, store_id, manager_id FROM staffs WHERE active = 1;

Last line:
User: Get the total number of orders placed in each store.

Output:
WITH StoreOrders AS (
    SELECT Store_id, COUNT(Order_id) AS Total_Orders
    FROM orders
    GROUP BY Store_id
)
SELECT s.Store_name, so.Total_Orders
FROM StoreOrders so
JOIN stores s ON so.Store_id = s.Store_id;

Conversation history:
User: Show the available schemas.
Output: 
SELECT DISTINCT table_name 
FROM information_schema.tables 
WHERE table_schema = 'BIKE_SALE';
Last line:
User: Show the schema of the orders table.
Output:
SELECT 
    column_name, 
    data_type, 
    is_nullable, 
    column_default 
FROM 
    INFORMATION_SCHEMA.COLUMNS 
WHERE 
    table_schema = 'BIKE_SALE' 
    AND table_name = 'orders';
"""
# Snowflake Connection Function
def connect_snowflake():
    try:
        return snowflake.connector.connect(
            user="Naveen04",
            password="Naveenkumar0410",
            account="SY62755.ap-southeast-1",
            warehouse="COMPUTE_WH",
            database="MEC",
            schema="BIKE_SALE",
            role="ACCOUNTADMIN"
        )
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return None

# In-memory storage for chat history
chat_histories = {}

@app.route('/')
def main1():
    return render_template('index2.html')
@app.route('/execute_query', methods=['POST'])
def execute_query():
    global results, columns, visualization

    try:
        data = request.json
        sql_query = data.get("query")

        if not sql_query:
            results, columns = [], []  # Reset global variables
            return jsonify({"error": "No query provided"}), 400

        # Establish new connection per request
        conn = connect_snowflake()
        if not conn:
            return jsonify({"error": "Failed to connect to Snowflake"}), 500

        cursor = conn.cursor()
        cursor.execute(sql_query)

        # Fetch results
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # Get column names

        # Close cursor and connection
        cursor.close()
        conn.close()

        # Convert results to JSON format
        response_data = [dict(zip(columns, row)) for row in results]

        return jsonify({"status": "success", "data": response_data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_columns", methods=["GET"])
def get_columns():
    global columns
    """ Fetch the updated column names dynamically. """
    columns = columns
    return jsonify({"columns": columns})


@app.route("/generate_chart", methods=["POST"])
def generate_chart():
    global i,results,columns,rowt
    """ Handle chart generation request """
    i+=1
    data = request.json
    chart_type = data.get("chart_type", "bar")  # Default: Bar Chart
    num_rows = data.get("num_rows", "All")
    print(data,type(num_rows))
    df=pd.DataFrame(results, columns=columns)
    rowt=len(df)
    if num_rows.lower() == "all":
        if rowt > 50:
            # Display a warning if the number of rows exceeds 50 for any chart type
            if chart_type in ["Histogram", "Bar Chart", "Line Chart", "Pie Chart"]:
                return jsonify({"warning": f"{chart_type} cannot display more than 50 rows. Your dataset has {rowt} rows."}), 200    
    #ques = f"Visualize the data as a {ques} using {num_rows} rows."
    ques = f"Create a {chart_type} using {'all rows' if num_rows.lower() == 'all' else f'the {num_rows} rows'} of the dataset, analyse the data and put a meaningful chart"

    df = SmartDataframe(pd.DataFrame(results, columns=columns), config={"llm": llm})
    result = df.chat(ques)

    if isinstance(result, str):
        print("string")
        # Check if result is a valid file path
        if os.path.isfile(result):
            # Define destination path
            dest_path = f"static/img/visualization{i}.png"
            
            # Ensure the static/img directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Copy the file to the static directory
            shutil.copy(result, dest_path)
            
            print("Saved visualization at:", dest_path)
            visualization=dest_path
    elif isinstance(result, plt.Figure):
        # Define save path
        # Save figure to 
        image_path=f"static/img/visualization{i}.png"
        result.savefig(image_path, format="png")

        visualization = image_path  # Store the path instead of Base64
    else:
        visualization = "Unsupported response type."


    return jsonify({"status": "success", "visualization": visualization})

@app.route("/vis", methods=["POST"])
def vis():
    global results, columns, visualization,i
    i+=1
    try:
        if not results or not columns:
            return jsonify({"error": "No data available for visualization"}), 400

        # Convert results to SmartDataFrame
        df = SmartDataframe(pd.DataFrame(results, columns=columns), config={"llm": llm})
        result = df.chat("visualise data in efficient way of analysis")

        if isinstance(result, str):
            print("string")
            
            # Check if result is a valid file path
            if os.path.isfile(result):
                # Define destination path
                dest_path = f"static/img/visualization{i}.png"
                
                # Ensure the static/img directory exists
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                
                # Copy the file to the static directory
                shutil.copy(result, dest_path)
                
                print("Saved visualization at:", dest_path)
                visualization=dest_path
        elif isinstance(result, plt.Figure):
            # Define save path
            # Save figure to 
            image_path=f"static/img/visualization{i}.png"
            result.savefig(image_path, format="png")

            visualization = image_path  # Store the path instead of Base64
        else:
            visualization = "Unsupported response type."


        return jsonify({"status": "success", "visualization": visualization})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/generate_sql", methods=["POST"])  
def generate_sql():
    data = request.json
    user_query = data.get("query", "")
    user_id = data.get("user_id", "default_user")  # Use a unique identifier for users

    if not user_query:
        return jsonify({"error": "No query provided"}), 400

    # Initialize chat history for the user if not present
    if user_id not in chat_histories:
        chat_histories[user_id] = [{"role": "system", "content": SYSTEM_MESSAGE}]

    # Append the new user query
    chat_histories[user_id].append({"role": "user", "content": user_query})

    try:
        # Call OpenAI API with chat history
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=chat_histories[user_id],
            temperature=0.25,
            seed=12
                
        )

        # Get the response and update history
        sql_query = response["choices"][0]["message"]["content"]
        chat_histories[user_id].append({"role": "assistant", "content": sql_query})
        

        return jsonify({"sql_query": sql_query})

    except Exception as e:
        
        return jsonify({"error": str(e)}), 500

@app.route("/clear_history", methods=["POST"])
def clear_history():
    data = request.json
    user_id = data.get("user_id", "default_user")

    if user_id in chat_histories:
        del chat_histories[user_id]

    return jsonify({"message": "Chat history cleared"}), 200

if __name__ == "__main__":
    app.run(debug=True)