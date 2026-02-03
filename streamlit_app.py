import streamlit as st
from snowflake.snowpark.functions import col, desc

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

cnx = st.connection("snowflake")
session = cnx.session()

# --- SECCIÓN DE PEDIDOS ---
st.subheader("Place an Order")
name_on_order = st.text_input('Name on Smoothie:').strip()

if name_on_order:
    my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
    ingredients_list = st.multiselect('Choose up to 5 ingredients:', my_dataframe, max_selections=5)

    if ingredients_list:
        # UNIÓN PERFECTA: Un solo espacio entre frutas, sin espacios al final
        ingredients_string = ' '.join(ingredients_list).strip()
        
        if st.button('Submit Order'):
            # Insertamos con ORDER_FILLED = FALSE por defecto y TIMESTAMP
            my_insert_stmt = f"""
                INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled, order_ts)
                VALUES ('{ingredients_string}', '{name_on_order}', FALSE, CURRENT_TIMESTAMP())
            """
            session.sql(my_insert_stmt).collect()
            st.success(f'Order placed for {name_on_order}!', icon="✅")

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN (Lógica de los 3 más recientes) ---
st.subheader("Pending Orders (Last 3)")

# Esta consulta limpia visualmente y permite marcar como filled
recent_orders = session.table("smoothies.public.orders") \
                .order_by(desc("order_ts")) \
                .limit(3)

if recent_orders.count() > 0:
    st.dataframe(recent_orders)
    
    # Solo permitimos marcar como filled los que aún están en FALSE
    pending_list = recent_orders.filter(col("ORDER_FILLED") == False).to_pandas()
    
    if not pending_list.empty:
        order_to_fill = st.selectbox("Select Name to Mark as Filled:", pending_list["NAME_ON_ORDER"])
        
        if st.button("Mark as Filled"):
            session.sql(f"""
                UPDATE smoothies.public.orders 
                SET ORDER_FILLED = TRUE 
                WHERE NAME_ON_ORDER = '{order_to_fill}'
            """).collect()
            st.rerun()
else:
    st.write("No orders found.")
