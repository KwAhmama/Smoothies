# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled")

session = get_active_session()

# 1. Obtenemos el DataFrame de Snowpark
# Quitamos .collect() para que siga siendo un objeto de datos procesable
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

# 2. Convertimos a Pandas para que st.data_editor funcione correctamente
if my_dataframe.count() > 0:
    editable_df = st.data_editor(my_dataframe.to_pandas())
    
    submitted = st.button('Submit')
    
    if submitted:
        try:
            # 3. Convertimos los cambios de vuelta a un DataFrame de Snowpark
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            # 4. Realizamos el Merge para actualizar los registros
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order(s) updated!', icon='👍')
            
            # Refrescar la página para que desaparezcan las órdenes completadas
            st.rerun()
            
        except Exception as e:
            st.error(f'Something went wrong: {e}')
else:
    st.success('There are no pending orders right now', icon='👍')
