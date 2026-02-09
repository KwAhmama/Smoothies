import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")

# Esto NO fallará si estás dentro de Snowsight
session = get_active_session()

# Solo mostramos lo que no está entregado
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

if my_dataframe.count() > 0:
    # El editor de datos
    editable_df = st.data_editor(my_dataframe.to_pandas())
    
    if st.button('Submit'):
        try:
            # Convertimos los cambios y actualizamos
            edited_dataset = session.create_dataframe(editable_df)
            
            # Usamos SQL directo para asegurar que no haya errores de esquema
            for row in editable_df.itertuples():
                session.sql(f"UPDATE smoothies.public.orders SET ORDER_FILLED = {row.ORDER_FILLED} WHERE NAME_ON_ORDER = '{row.NAME_ON_ORDER}'").collect()
            
            st.success('¡Órdenes actualizadas!', icon='👍')
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")
else:
    st.success('¡No hay órdenes pendientes!', icon='👍')
