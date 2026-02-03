import streamlit as st
from snowflake.snowpark.functions import col, desc

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

cnx = st.connection("snowflake")
session = cnx.session()

# --- FUNCIÓN DE LIMPIEZA AUTOMÁTICA ---
# Esto elimina todo excepto los 3 registros más nuevos cada vez que usas la app
def cleanup_old_orders():
    session.sql("""
        DELETE FROM SMOOTHIES.PUBLIC.ORDERS 
        WHERE ORDER_ID NOT IN (
            SELECT ORDER_ID FROM SMOOTHIES.PUBLIC.ORDERS 
            ORDER BY ORDER_ID DESC LIMIT 3
        )
    """).collect()

cleanup_old_orders()

# --- SECCIÓN DE PEDIDOS ---
name_on_order = st.text_input('Name on Smoothie:').strip()

if name_on_order:
    my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
    ingredients_list = st.multiselect('Choose ingredients:', my_dataframe)

    if ingredients_list:
        ingredients_string = ' '.join(ingredients_list).strip()
        
        if st.button('Submit Order'):
            # Insertamos con el formato exacto para el HASH de DORA
            session.sql(f"""
                INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
                VALUES ('{ingredients_string}', '{name_on_order}', FALSE)
            """).collect()
            st.success(f'Order placed for {name_on_order}!')
            st.rerun() # Refrescamos para que la limpieza actúe

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN ---
st.subheader("Pending Orders (Keeping only last 3)")

try:
    # Consultamos los últimos 3 registros
    recent_orders = session.table("SMOOTHIES.PUBLIC.ORDERS").order_by(desc("ORDER_ID")).limit(3).collect()
    
    if recent_orders:
        st.dataframe(recent_orders)
        
        # Filtramos solo los que están en FALSE para el selector
        pending_names = [row['NAME_ON_ORDER'] for row in recent_orders if not row['ORDER_FILLED']]
        
        if pending_names:
            order_to_fill = st.selectbox("Select Name to Mark as Filled:", pending_names)
            if st.button("Mark as Filled"):
                session.sql(f"UPDATE SMOOTHIES.PUBLIC.ORDERS SET ORDER_FILLED = TRUE WHERE NAME_ON_ORDER = '{order_to_fill}'").collect()
                st.rerun()
    else:
        st.write("No orders found.")
except Exception as e:
    st.error(f"Error de base de datos: {e}")
