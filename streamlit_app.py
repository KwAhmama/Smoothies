import streamlit as st
from snowflake.snowpark.functions import col

# Título de la App
st.title(":cup_with_straw: Customize Your Smoothie :cup_with_straw:")
st.write("Choose the fruits you want in your custom Smoothie!")

# Entrada de texto para el nombre
name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your Smoothie will be:', name_on_order)

# CONEXIÓN CORRECTA PARA GITHUB
# En lugar de get_active_session, usamos st.connection
cnx = st.connection("snowflake")
session = cnx.session()

# Obtener los datos de las frutas
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))

# Selector múltiple
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    my_dataframe,
    max_selections=5
)

if ingredients_list:
    # Construir el string de ingredientes exactamente como pide el Grader
    ingredients_string = ''
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '
    
    # Limpiar espacios en blanco al inicio/final
    ingredients_string = ingredients_string.strip()

    # Construir el INSERT
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
            values ('""" + ingredients_string + """','""" + name_on_order + """')"""

    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success(f'Your Smoothie is ordered, {name_on_order}!', icon="✅")
