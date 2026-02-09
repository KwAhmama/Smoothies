import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to be filled")

# Usamos la sesión activa (Asegúrate de ejecutar esto DENTRO de Snowflake)
session = get_active_session()

# 1. Cargamos las órdenes pendientes
# Importante: No usamos .collect() aquí para mantener el objeto de Snowpark
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0)

if my_dataframe.count() > 0:
    # 2. Mostramos el editor (convertimos a Pandas para la interfaz)
    editable_df = st.data_editor(my_dataframe.to_pandas())
    
    submitted = st.button('Submit')
    
    if submitted:
        try:
            # 3. Preparamos la actualización
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            # 4. Hacemos el MERGE basado en ORDER_UID
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order(s) updated!', icon='👍')
            st.rerun()
            
        except Exception as e:
            st.error(f'Something went wrong: {e}')
else:
    st.success('There are no pending orders right now', icon='👍')
