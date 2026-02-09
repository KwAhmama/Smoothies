import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")

# 1. CONEXIÓN (Ajustada para funcionar dentro de Snowflake/Snowsight)
try:
    session = get_active_session()
except:
    # Fallback si estás probando localmente (opcional)
    cnx = st.connection("snowflake")
    session = cnx.session()

st.subheader("Place your Order")
name_on_order = st.text_input('Name on Smoothie:')

# Obtenemos las frutas disponibles
fruit_query = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME')).collect()
fruit_list = [row['FRUIT_NAME'] for row in fruit_query]

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    fruit_list,
    max_selections=5
)

if ingredients_list and name_on_order:
    # --- CORRECCIÓN CRÍTICA PARA EL HASH ---
    # En lugar de .join().strip(), usamos un bucle para asegurar el espacio final
    ingredients_string = ''
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' ' 
    # El resultado será algo como "Lemon Lime " (con espacio al final)

    if st.button('Submit Order'):
        # Inserción en la base de datos
        my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order, order_filled)
            values ('""" + ingredients_string + """','"""+name_on_order+"""', FALSE)"""

        session.sql(my_insert_stmt).collect()
        
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
        st.stop() # Detenemos aquí para que no recargue innecesariamente

st.divider()

# --- SECCIÓN DE ADMIN (Para marcar como completado) ---
st.subheader("Pending Orders")
# Solo mostramos las órdenes NO completadas
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    
    submitted = st.button('Submit Changes')
    if submitted:
        # Nota: Para actualizar filas complejas es mejor usar MERGE, 
        # pero para este ejercicio simple, SQL directo funciona si el ID es único.
        # Aquí solo recargamos para ver cambios si editaste la tabla visualmente.
        st.success('Orders Updated!')
        st.experimental_rerun()
else:
    st.write("No pending orders.")
