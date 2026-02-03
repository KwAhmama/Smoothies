import streamlit as st
from snowflake.snowpark.functions import col

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

# Conexión
cnx = st.connection("snowflake")
session = cnx.session()

# --- SECCIÓN DE PEDIDOS ---
st.subheader("Place an Order")
name_on_order = st.text_input('Name on Smoothie:')

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
ingredients_list = st.multiselect('Choose up to 5 ingredients:', my_dataframe, max_selections=5)

if ingredients_list:
    # Formateamos los ingredientes exactamente para el HASH de DORA
    ingredients_string = ' '.join(ingredients_list).strip()
    
    # Usamos f-strings para el INSERT (más limpio)
    # Modifica esta parte en tu código:
    my_insert_stmt = f"""INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled) 
                    VALUES ('{ingredients_string}','{name_on_order}', FALSE)"""
    
    if st.button('Submit Order'):
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN ---
st.subheader("Pending Orders")
pending_df = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False)

if pending_df.count() > 0:
    st.dataframe(pending_df)
    
    # Lista de nombres para el selector de completar
    order_names = pending_df.select("NAME_ON_ORDER").to_pandas()["NAME_ON_ORDER"].tolist()
    order_to_fill = st.selectbox("Select Name to Mark as Filled:", order_names)
    
    if st.button("Mark as Filled"):
        session.sql(f"UPDATE smoothies.public.orders SET ORDER_FILLED = TRUE WHERE NAME_ON_ORDER = '{order_to_fill}'").collect()
        st.success(f"Order for {order_to_fill} marked as filled!")
        st.rerun()
else:
    st.write("No pending orders.")
