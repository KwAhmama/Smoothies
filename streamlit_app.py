import streamlit as st
from snowflake.snowpark.functions import col, val

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
    ingredients_string = ' '.join(ingredients_list).strip()
    my_insert_stmt = f"INSERT INTO smoothies.public.orders(ingredients, name_on_order) VALUES ('{ingredients_string}','{name_on_order}')"
    
    if st.button('Submit Order'):
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN (Para marcar como rellenado) ---
st.subheader("Pending Orders")
# Solo mostramos los que tienen ORDER_FILLED = FALSE
pending_df = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False)

if pending_df.count() > 0:
    # Mostramos la tabla para ver qué hay
    st.dataframe(pending_df)
    
    # Creamos un formulario para editar
    with st.form("fill_order"):
        order_to_fill = st.selectbox("Select Name to Mark as Filled:", pending_df.select("NAME_ON_ORDER"))
        submit_update = st.form_submit_button("Mark as Filled")
        
        if submit_update:
            session.sql(f"UPDATE smoothies.public.orders SET ORDER_FILLED = TRUE WHERE NAME_ON_ORDER = '{order_to_fill}'").collect()
            st.success(f"Order for {order_to_fill} marked as filled!")
            st.rerun()
else:
    st.write("No pending orders.")
