import streamlit as st
from snowflake.snowpark.functions import col

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

# Conexión optimizada para Streamlit Cloud
cnx = st.connection("snowflake")
session = cnx.session()

# --- SECCIÓN DE PEDIDOS ---
st.subheader("Place an Order")

# Usamos .strip() para evitar que espacios accidentales rompan el HASH
name_on_order = st.text_input('Name on Smoothie:').strip()

# Obtener opciones de frutas
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

ingredients_list = st.multiselect(
    'Choose the fruits in the EXACT order described:', 
    my_dataframe, 
    max_selections=5
)

if ingredients_list:
    # Unión de ingredientes con un solo espacio (vital para el HASH de 19 caracteres de Kevin)
    ingredients_string = ' '.join(ingredients_list).strip()
    
    # INSERT explícito con ORDER_FILLED = FALSE
    my_insert_stmt = f"""INSERT INTO smoothies.public.orders(ingredients, name_on_order, order_filled) 
                        VALUES ('{ingredients_string}','{name_on_order}', FALSE)"""
    
    if st.button('Submit Order'):
        if name_on_order:
            session.sql(my_insert_stmt).collect()
            st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
        else:
            st.error("Please enter a name for the order.")

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN ---
st.subheader("Pending Orders")

# El Grader necesita que esta sección funcione para Divya y Xi
pending_df = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False)

if pending_df.count() > 0:
    st.dataframe(pending_df)
    
    # Conversión a Pandas para el selector de Streamlit
    order_names = pending_df.select("NAME_ON_ORDER").to_pandas()["NAME_ON_ORDER"].tolist()
    order_to_fill = st.selectbox("Select Name to Mark as Filled:", order_names)
    
    if st.button("Mark as Filled"):
        # Actualización a TRUE requerida para los pedidos de Divya y Xi
        session.sql(f"UPDATE smoothies.public.orders SET ORDER_FILLED = TRUE WHERE NAME_ON_ORDER = '{order_to_fill}'").collect()
        st.success(f"Order for {order_to_fill} marked as filled!")
        st.rerun()
else:
    st.success("No pending orders! 🥤")
