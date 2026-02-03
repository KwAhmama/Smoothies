import streamlit as st
from snowflake.snowpark.functions import col

st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")
name_on_order = st.text_input('Name on Smoothie:')

# Si estás en Streamlit Cloud usa: cnx = st.connection("snowflake")
# Si estás DENTRO de Snowflake usa:
from snowflake.snowpark.context import get_active_session
session = get_active_session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

ingredients_list = st.multiselect('Choose up to 5 ingredients:', my_dataframe, max_selections=5)

if ingredients_list:
    # Unir ingredientes sin dejar espacios sueltos al final
    ingredients_string = ', '.join(ingredients_list) 

    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
                values ('""" + ingredients_string + """','""" + name_on_order + """')"""

    time_to_insert = st.button('Submit Order')
    
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
