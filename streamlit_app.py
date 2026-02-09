# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Título
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")

# CONEXIÓN: Esta línea solo funciona si estás DENTRO de Snowflake
try:
    session = get_active_session()
except:
    st.error("Error crítico: Esta app debe ejecutarse desde dentro de Snowflake (Snowsight), no desde Streamlit Cloud externo.")
    st.stop()

# Carga de datos
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0)

# Mostrar datos si existen
if my_dataframe.count() > 0:
    # Convertimos a Pandas para el editor
    editable_df = st.data_editor(my_dataframe.to_pandas())
    
    submitted = st.button('Submit')
    
    if submitted:
        try:
            og_dataset = session.table("smoothies.public.orders")
            edited_dataset = session.create_dataframe(editable_df)

            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('¡Orden actualizada!', icon='👍')
            st.experimental_rerun()
        except Exception as e:
            st.error(f'Algo salió mal: {e}')
else:
    st.success('No hay órdenes pendientes ahora mismo', icon='👍')
