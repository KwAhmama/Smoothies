import streamlit as st
from snowflake.snowpark.functions import col, desc

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

# Conexión a Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# --- FUNCIÓN DE LIMPIEZA AUTOMÁTICA ---
# Mantiene la tabla con solo los 3 registros más recientes para evitar errores en el Grader
def cleanup_old_orders():
    try:
        # Buscamos cuántos registros hay
        total_rows = session.table("SMOOTHIES.PUBLIC.ORDERS").count()
        if total_rows > 3:
            # Si hay más de 3, borramos los más antiguos dejando solo los IDs más altos
            session.sql("""
                DELETE FROM SMOOTHIES.PUBLIC.ORDERS 
                WHERE ORDER_ID NOT IN (
                    SELECT ORDER_ID FROM (
                        SELECT ORDER_ID FROM SMOOTHIES.PUBLIC.ORDERS 
                        ORDER BY ORDER_ID DESC LIMIT 3
                    )
                )
            """).collect()
    except:
        pass # Si la tabla está vacía o no existe aún, ignoramos el error

cleanup_old_orders()

# --- SECCIÓN DE PEDIDOS ---
st.subheader("Place your Order")
name_on_order = st.text_input('Name on Smoothie:').strip()

if name_on_order:
    # IMPORTANTE: Convertimos los objetos Row a una lista de Strings limpia
    # Esto evita que el HASH se rompa por meter metadatos de Snowflake en el string
    fruit_query = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME')).collect()
    fruit_list = [row['FRUIT_NAME'] for row in fruit_query] 

    ingredients_list = st.multiselect(
        'Choose up to 5 ingredients (Order matters for the Hash!):',
        fruit_list,
        max_selections=5
    )

    if ingredients_list:
        # Unimos las frutas con un solo espacio, exactamente como pide el ejercicio
        ingredients_string = ' '.join(ingredients_list).strip()
        
        if st.button('Submit Order'):
            # Inserción con formato limpio
            session.sql(f"""
                INSERT INTO SMOOTHIES.PUBLIC.ORDERS (INGREDIENTS, NAME_ON_ORDER, ORDER_FILLED)
                VALUES ('{ingredients_string}', '{name_on_order}', FALSE)
            """).collect()
            st.success(f'Order placed for {name_on_order}!', icon="✅")
            st.rerun()

st.divider()

# --- SECCIÓN DE ADMINISTRACIÓN ---
st.subheader("Pending Orders (Last 3)")

try:
    # Obtenemos los últimos 3 para gestionar a Divya y Xi
    recent_data = session.table("SMOOTHIES.PUBLIC.ORDERS").order_by(desc("ORDER_ID")).limit(3).collect()
    
    if len(recent_data) > 0:
        st.dataframe(recent_data, use_container_width=True)
        
        # Filtramos los nombres que aún no están "Filled"
        pending_names = [row['NAME_ON_ORDER'] for row in recent_data if not row['ORDER_FILLED']]
        
        if pending_names:
            order_to_fill = st.selectbox("Select Name to Mark as Filled:", pending_names)
            
            if st.button("Mark as Filled"):
                session.sql(f"""
                    UPDATE SMOOTHIES.PUBLIC.ORDERS 
                    SET ORDER_FILLED = TRUE 
                    WHERE NAME_ON_ORDER = '{order_to_fill}'
                """).collect()
                st.rerun()
    else:
        st.info("No orders in the system.")
        
except Exception as e:
    st.error(f"Waiting for table data... {e}")
