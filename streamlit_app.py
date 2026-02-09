import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")

# Esta línea SOLO funciona si la App se ejecuta DENTRO de Snowflake (Snowsight)
session = get_active_session()

# Cargamos las órdenes
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

if my_dataframe.count() > 0:
    # Mostramos el editor
    editable_df = st.data_editor(my_dataframe.to_pandas())
    
    if st.button('Submit'):
        try:
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order(s) updated!', icon='👍')
            st.rerun()
        except Exception as e:
            st.error(f'Error: {e}')
else:
    st.success('No pending orders!', icon='👍')
