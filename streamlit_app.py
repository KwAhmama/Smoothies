import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled")

# DENTRO de Snowflake, esto funciona automáticamente
session = get_active_session()

# Obtenemos la tabla como un DataFrame de Snowpark
# No usamos .collect() para que el merge funcione después
query_df = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

if query_df.count() > 0:
    # Convertimos a Pandas para que st.data_editor sea interactivo
    editable_df = st.data_editor(query_df.to_pandas())
    
    submitted = st.button('Submit')
    
    if submitted:
        try:
            # Convertimos los cambios del editor de vuelta a Snowpark
            edited_dataset = session.create_dataframe(editable_df)
            og_dataset = session.table("smoothies.public.orders")

            # Aplicamos los cambios a la tabla original
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order(s) updated!', icon='👍')
            st.rerun()
            
        except Exception as e:
            st.error(f'Error updating: {e}')
else:
    st.success('There are no pending orders right now', icon='👍')
